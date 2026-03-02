import subprocess
import logging
import os
import re
import shutil
import signal
import sys
import time

logger = logging.getLogger("converter")


def _extract_exception_message(stderr: str) -> str:
    """Extract the final exception message from a Python traceback.

    Returns the human-readable error (e.g. 'File is not a zip file')
    instead of the full stack trace.  Captures multi-line messages like
    FileConversionException that list individual converter failures.
    """
    lines = stderr.strip().splitlines()
    # Find the last exception line and return everything from it onwards
    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.match(r'^[\w.]+(?:Error|Exception|Failure):\s', stripped):
            # Take this line + any continuation lines that follow
            _, _, message = stripped.partition(': ')
            tail = '\n'.join(l.strip() for l in lines[i + 1:] if l.strip())
            full = f"{message}\n{tail}".strip() if tail else (message or stripped)
            return full
    # Fallback: return last non-empty line
    non_empty = [l.strip() for l in lines if l.strip()]
    return non_empty[-1] if non_empty else stderr.strip()

# File magic bytes
_OLE2_MAGIC = b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1'
_ZIP_MAGIC = b'PK\x03\x04'
_RTF_MAGIC = b'{\\rtf'
# Modern Office formats (.xlsx, .pptx, .docx) are ZIP-based.
# When password-protected, Office wraps them in an OLE2 encrypted container.
_ZIP_BASED_EXTENSIONS = {".xlsx", ".pptx", ".docx"}

# Extension-to-converter routing table
PANDOC_EXTENSIONS = {".rtf", ".odt", ".txt", ".docx"}
MARKITDOWN_EXTENSIONS = {".pptx", ".xls", ".xlsx", ".pdf"}
# .doc is handled separately with a fallback chain (see convert())
SUPPORTED_EXTENSIONS = PANDOC_EXTENSIONS | MARKITDOWN_EXTENSIONS | {".doc"}

DEFAULT_TIMEOUT = 120
PANDOC_MAX_HEAP = os.environ.get("PANDOC_MAX_HEAP", "128m")

# RSS limit for converter subprocesses in MB.  The parent polls the child's
# RSS and kills it (SIGKILL) if it exceeds this threshold.
# Set to 0 to disable.  Only enforced on Linux (inside the container).
# For a 512 MB container, 350 MB leaves ~130 MB for the parent process + OS.
SUBPROCESS_MEMORY_LIMIT_MB = int(os.environ.get("SUBPROCESS_MEMORY_LIMIT_MB", 350))

# How often (seconds) to check the child's RSS while it runs.
# procfs reads are negligible (~microseconds per PID in the container),
# so polling at 100ms adds no meaningful overhead.
_RSS_POLL_INTERVAL = 0.1


class MemoryLimitExceeded(Exception):
    """Raised when a conversion subprocess exceeds its memory limit."""


def _get_rss_bytes(pid: int) -> int | None:
    """Read resident set size of *pid* from /proc on Linux.

    Returns RSS in bytes, or None if unavailable (non-Linux / process gone).
    """
    try:
        with open(f"/proc/{pid}/statm", "rb") as f:
            # statm fields: size resident shared text lib data dt  (pages)
            pages = int(f.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


def _get_process_group_rss_bytes(pgid: int) -> int | None:
    """Sum RSS of all processes in process group *pgid* via /proc.

    This captures the direct child plus any grandchildren (e.g. if
    markitdown or pandoc spawns helper processes).
    Returns total RSS in bytes, or None if /proc is unavailable.
    """
    page_size = os.sysconf("SC_PAGE_SIZE")
    total = 0
    found = False
    try:
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat", "rb") as f:
                    stat_data = f.read()
                # Parse: pid (comm) state ppid pgrp ...
                # comm can contain spaces/parens, so find the last ')' first
                close_paren = stat_data.rindex(b")")
                fields_after = stat_data[close_paren + 2:].split()
                # fields_after[0]=state, [1]=ppid, [2]=pgrp
                pgrp = int(fields_after[2])
                if pgrp != pgid:
                    continue
                with open(f"/proc/{entry}/statm", "rb") as f:
                    pages = int(f.read().split()[1])
                total += pages * page_size
                found = True
            except (OSError, ValueError, IndexError):
                continue
    except OSError:
        return None
    return total if found else None


def _run_with_memory_limit(
    cmd: list,
    timeout: int,
    converter_name: str,
) -> subprocess.CompletedProcess:
    """Run *cmd* as a subprocess, killing it if RSS exceeds the limit.

    The child is started in its own process group (``start_new_session``),
    so the watchdog sums RSS across the whole group — covering any
    grandchild processes the converter may spawn.

    On non-Linux or when the limit is disabled this falls back to plain
    subprocess.run() with the same timeout.
    """
    limit_bytes = SUBPROCESS_MEMORY_LIMIT_MB * 1024 * 1024
    use_watchdog = (
        sys.platform != "win32"
        and limit_bytes > 0
        and os.path.isdir("/proc")
    )

    if not use_watchdog:
        result = subprocess.run(cmd, capture_output=True, timeout=timeout)
        _check_memory_failure(result, converter_name)
        return result

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    pgid = os.getpgid(proc.pid)
    killed_for_memory = False
    deadline = time.monotonic() + timeout

    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                os.killpg(pgid, signal.SIGKILL)
                proc.wait()
                raise subprocess.TimeoutExpired(cmd, timeout)

            try:
                proc.wait(timeout=min(_RSS_POLL_INTERVAL, remaining))
                break  # process finished
            except subprocess.TimeoutExpired:
                pass  # still running — check RSS

            rss = _get_process_group_rss_bytes(pgid)
            if rss is not None and rss > limit_bytes:
                logger.warning(
                    "[Converter] %s exceeded RSS limit (%d MB > %d MB), killing",
                    converter_name, rss // (1024 * 1024),
                    SUBPROCESS_MEMORY_LIMIT_MB,
                )
                os.killpg(pgid, signal.SIGKILL)
                proc.wait()
                killed_for_memory = True
                break
    except Exception:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except OSError:
            proc.kill()
        proc.wait()
        raise

    stdout = proc.stdout.read()
    stderr = proc.stderr.read()
    result = subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)

    if killed_for_memory:
        raise MemoryLimitExceeded(
            f"{converter_name}: process killed (RSS exceeded "
            f"{SUBPROCESS_MEMORY_LIMIT_MB} MB limit)"
        )

    _check_memory_failure(result, converter_name)
    return result


def _check_memory_failure(result: subprocess.CompletedProcess,
                          converter_name: str) -> None:
    """Raise MemoryLimitExceeded if the subprocess died from memory exhaustion."""
    if result.returncode == 0:
        return
    # Killed by signal 9 (OOM killer)
    if result.returncode in (-9, 137):
        raise MemoryLimitExceeded(
            f"{converter_name}: process killed (out of memory)"
        )
    stderr = result.stderr.decode("utf-8", errors="replace") if result.stderr else ""
    if "MemoryError" in stderr or "Cannot allocate memory" in stderr:
        raise MemoryLimitExceeded(f"{converter_name}: out of memory")
    if "Heap exhausted" in stderr:
        raise MemoryLimitExceeded(
            f"{converter_name}: out of memory (heap exhausted)"
        )


def antiword_to_markdown(input_path: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Convert a legacy .doc file to plain text using antiword CLI."""
    result = _run_with_memory_limit(
        ["antiword", input_path],
        timeout=timeout,
        converter_name="antiword",
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"antiword conversion failed: {stderr}")
    return result.stdout.decode("utf-8", errors="replace")


def pandoc_to_markdown(input_path: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Convert a document to Markdown using Pandoc CLI."""
    result = _run_with_memory_limit(
        ["pandoc", "+RTS", f"-M{PANDOC_MAX_HEAP}", "-RTS",
         input_path, "-t", "markdown", "--wrap=none"],
        timeout=timeout,
        converter_name="pandoc",
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"Pandoc conversion failed: {stderr}")
    return result.stdout.decode("utf-8", errors="replace")


def markitdown_to_markdown(input_path: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Convert a document to Markdown using MarkItDown in a subprocess.

    Running in a subprocess ensures all memory is returned to the OS when
    the conversion finishes, instead of fragmenting the main process heap.
    """
    result = _run_with_memory_limit(
        [
            sys.executable, "-c",
            "import sys; "
            "from markitdown import MarkItDown; "
            "md = MarkItDown(); "
            "r = md.convert(sys.argv[1]); "
            "sys.stdout.buffer.write(r.text_content.encode('utf-8'))",
            input_path,
        ],
        timeout=timeout,
        converter_name="markitdown",
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        clean_msg = _extract_exception_message(stderr)
        logger.error("[Converter] MarkItDown stderr: %s", stderr)
        raise RuntimeError(f"MarkItDown conversion failed: {clean_msg}")
    return result.stdout.decode("utf-8", errors="replace")


def xls_to_markdown(input_path: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Convert a legacy .xls file to Markdown using python-calamine in a subprocess.

    xlrd 2.x rejects some .xls files with OLE2 FAT chain issues;
    python-calamine (Rust-based) is more tolerant and faster.
    """
    script = r'''
import sys
from python_calamine import CalamineWorkbook

path = sys.argv[1]
wb = CalamineWorkbook.from_path(path)
parts = []
for name in wb.sheet_names:
    data = wb.get_sheet_by_name(name).to_python()
    if not data:
        continue
    parts.append(f"## {name}")
    for ri, row in enumerate(data):
        cells = [str(c) if c is not None else "" for c in row]
        parts.append("| " + " | ".join(cells) + " |")
        if ri == 0:
            parts.append("| " + " | ".join("---" for _ in cells) + " |")
    parts.append("")
sys.stdout.buffer.write("\n".join(parts).encode("utf-8"))
'''
    result = _run_with_memory_limit(
        [sys.executable, "-c", script, input_path],
        timeout=timeout,
        converter_name="xls_to_markdown",
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        clean_msg = _extract_exception_message(stderr)
        logger.error("[Converter] xls_to_markdown stderr: %s", stderr)
        raise RuntimeError(f"XLS conversion failed: {clean_msg}")
    return result.stdout.decode("utf-8", errors="replace")


def xlsx_to_markdown(input_path: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Convert an .xlsx file to Markdown using python-calamine in a subprocess.

    Calamine (Rust-based) is fast, tolerant of unusual styles/fills that
    trip up openpyxl, and already used for .xls files.
    """
    script = r'''
import sys
from python_calamine import CalamineWorkbook

path = sys.argv[1]
wb = CalamineWorkbook.from_path(path)
parts = []
for name in wb.sheet_names:
    data = wb.get_sheet_by_name(name).to_python()
    if not data:
        continue
    parts.append(f"## {name}")
    for ri, row in enumerate(data):
        cells = [str(c) if c is not None else "" for c in row]
        parts.append("| " + " | ".join(cells) + " |")
        if ri == 0:
            parts.append("| " + " | ".join("---" for _ in cells) + " |")
    parts.append("")
sys.stdout.buffer.write("\n".join(parts).encode("utf-8"))
'''
    result = _run_with_memory_limit(
        [sys.executable, "-c", script, input_path],
        timeout=timeout,
        converter_name="xlsx_to_markdown",
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        clean_msg = _extract_exception_message(stderr)
        logger.error("[Converter] xlsx_to_markdown stderr: %s", stderr)
        raise RuntimeError(f"XLSX conversion failed: {clean_msg}")
    return result.stdout.decode("utf-8", errors="replace")


def get_converter(extension: str) -> str | None:
    """Return the converter name for a given extension, or None if unsupported."""
    ext = extension.lower()
    if ext in PANDOC_EXTENSIONS:
        return "pandoc"
    if ext == ".xlsx":
        return "xlsx"
    if ext == ".xls":
        return "xls"
    if ext in MARKITDOWN_EXTENSIONS or ext == ".doc":
        return "markitdown"
    return None


def _check_password_protected(input_path: str, extension: str) -> None:
    """Raise early if the file appears to be password-protected."""
    ext = extension.lower()
    try:
        with open(input_path, 'rb') as f:
            header = f.read(8)
    except OSError:
        return  # let the converter deal with unreadable files

    if ext in _ZIP_BASED_EXTENSIONS and header.startswith(_OLE2_MAGIC):
        # Password-protected Office files get encrypted into an OLE2 container,
        # so a .xlsx/.pptx/.docx that starts with OLE2 magic instead of ZIP
        # magic (PK) is almost certainly encrypted.  Detecting this upfront
        # avoids the confusing "File is not a zip file" / "Can't find workbook
        # in OLE2 compound document" errors from downstream parsers.
        raise ValueError(
            f"File appears to be password-protected (encrypted Office document)"
        )


def _detect_doc_format(input_path: str) -> str:
    """Sniff the actual format of a .doc file.

    Returns 'rtf', 'ole2', or 'unknown'.
    Many .doc files are actually RTF saved with a .doc extension.
    True legacy Word documents use the OLE2 binary format.
    """
    try:
        with open(input_path, 'rb') as f:
            header = f.read(8)
    except OSError:
        return 'unknown'
    if header.startswith(_RTF_MAGIC):
        return 'rtf'
    if header.startswith(_OLE2_MAGIC):
        return 'ole2'
    return 'unknown'


def _convert_doc(input_path: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Convert a .doc file with format detection and fallback.

    .doc files can be RTF (Pandoc handles well) or OLE2 binary Word
    (MarkItDown may handle).  We sniff the content and try the best
    converter first, falling back to the other if it fails.
    """
    fmt = _detect_doc_format(input_path)

    if fmt == 'rtf':
        # RTF masquerading as .doc — Pandoc handles this natively
        logger.info("[Converter] .doc is RTF, using Pandoc")
        return pandoc_to_markdown(input_path, timeout=timeout)

    # OLE2 binary or unknown — try antiword first (purpose-built for .doc),
    # then MarkItDown, then Pandoc as final fallback.
    if shutil.which("antiword"):
        logger.info("[Converter] .doc is %s format, trying antiword", fmt)
        try:
            return antiword_to_markdown(input_path, timeout=timeout)
        except RuntimeError as e:
            logger.warning("[Converter] antiword failed for .doc: %s", e)

    logger.info("[Converter] .doc is %s format, trying MarkItDown", fmt)
    try:
        return markitdown_to_markdown(input_path, timeout=timeout)
    except RuntimeError as e:
        logger.warning("[Converter] MarkItDown failed for .doc, trying Pandoc fallback: %s", e)
    try:
        return pandoc_to_markdown(input_path, timeout=timeout)
    except RuntimeError:
        pass

    raise RuntimeError(
        "Unable to convert .doc file. The legacy binary Word format (.doc) "
        "has limited conversion support. Try re-saving as .docx."
    )


def convert(input_path: str, extension: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Route to the appropriate converter based on file extension."""
    _check_password_protected(input_path, extension)
    ext = extension.lower()

    if ext == ".doc":
        return _convert_doc(input_path, timeout=timeout)

    converter = get_converter(ext)
    if converter == "pandoc":
        logger.info("[Converter] Using Pandoc for %s", extension)
        try:
            return pandoc_to_markdown(input_path, timeout=timeout)
        except MemoryLimitExceeded:
            if ext == ".docx":
                logger.warning("[Converter] Pandoc OOM for %s, falling back to MarkItDown", extension)
                return markitdown_to_markdown(input_path, timeout=timeout)
            raise
        except RuntimeError as e:
            if ext == ".docx" and "Heap exhausted" in str(e):
                logger.warning("[Converter] Pandoc heap exhausted for %s, falling back to MarkItDown", extension)
                return markitdown_to_markdown(input_path, timeout=timeout)
            raise
    elif converter == "xlsx":
        logger.info("[Converter] Using calamine for %s", extension)
        return xlsx_to_markdown(input_path, timeout=timeout)
    elif converter == "xls":
        logger.info("[Converter] Using calamine for %s", extension)
        return xls_to_markdown(input_path, timeout=timeout)
    elif converter == "markitdown":
        logger.info("[Converter] Using MarkItDown for %s", extension)
        return markitdown_to_markdown(input_path, timeout=timeout)
    else:
        raise ValueError(f"Unsupported extension: {extension}")
