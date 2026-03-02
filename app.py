import asyncio
import importlib.util
import logging
import os
import re
import shutil
import subprocess
import tempfile

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from python_multipart.multipart import parse_options_header

from converter import SUPPORTED_EXTENSIONS, MemoryLimitExceeded, convert, get_converter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("converter")

MAX_UPLOAD_SIZE = int(os.environ.get("MAX_UPLOAD_SIZE", 10 * 1024 * 1024))  # 10MB
CONVERSION_TIMEOUT = int(os.environ.get("CONVERSION_TIMEOUT", 120))
MAX_CONCURRENT_CONVERSIONS = int(os.environ.get("MAX_CONCURRENT_CONVERSIONS", 1))
MAX_QUEUED_CONVERSIONS = int(os.environ.get("MAX_QUEUED_CONVERSIONS", 5))

conversion_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONVERSIONS)
# Bounds total in-flight requests (active + queued)
_queue_slots = asyncio.Semaphore(MAX_CONCURRENT_CONVERSIONS + MAX_QUEUED_CONVERSIONS)

app = FastAPI(title="Markdown Converter Image")

SAFE_FILENAME_RE = re.compile(r"^[a-zA-Z0-9._-]+$")


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal and injection."""
    name = os.path.basename(filename)
    if not name or not SAFE_FILENAME_RE.match(name):
        # Strip unsafe characters, keep only safe ones
        name = re.sub(r"[^a-zA-Z0-9._-]", "_", name)
    if not name:
        raise ValueError("Invalid filename")
    return name


@app.get("/health")
async def health():
    pandoc_ok = shutil.which("pandoc") is not None
    markitdown_ok = importlib.util.find_spec("markitdown") is not None

    return {"status": "ok", "pandoc": pandoc_ok, "markitdown": markitdown_ok}


@app.post("/convert")
async def convert_file(request: Request):
    # ── 1. Validate filename from Content-Disposition or query before reading body ──
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise HTTPException(status_code=400, detail="Expected multipart/form-data")

    # ── 2. Reject immediately if the queue is full — before reading body ──
    if _queue_slots.locked():
        raise HTTPException(status_code=429, detail="Too many conversion requests queued")

    await _queue_slots.acquire()
    tmp_dir = None
    try:
        # ── 3. Stream multipart body, accumulate file bytes in memory ──
        _, params = parse_options_header(content_type)
        boundary = params.get(b"boundary")
        if not boundary:
            raise HTTPException(status_code=400, detail="Missing multipart boundary")

        import python_multipart.multipart as multipart_mod

        current_field = None
        cd_filename = None  # filename from Content-Disposition
        file_data = bytearray()
        file_size = 0
        form_fields = {}
        # Buffers for accumulating header field/value across split callbacks
        _hdr_field = bytearray()
        _hdr_value = bytearray()
        _part_headers = {}

        def _flush_header():
            """Save accumulated header field/value pair."""
            if _hdr_field:
                field = bytes(_hdr_field).decode("utf-8", errors="replace").lower()
                _part_headers[field] = bytes(_hdr_value)
            _hdr_field.clear()
            _hdr_value.clear()

        def on_part_begin():
            nonlocal current_field
            current_field = None
            _hdr_field.clear()
            _hdr_value.clear()
            _part_headers.clear()

        def on_header_field(data, start, end):
            # New field starting means previous field/value pair is complete
            if _hdr_value:
                _flush_header()
            _hdr_field.extend(data[start:end])

        def on_header_value(data, start, end):
            _hdr_value.extend(data[start:end])

        def on_headers_finished():
            nonlocal current_field, cd_filename
            _flush_header()
            cd = _part_headers.get("content-disposition", b"")
            if cd:
                _, vparams = parse_options_header(cd)
                name = vparams.get(b"name", b"").decode("utf-8", errors="replace")
                fname = vparams.get(b"filename", b"").decode("utf-8", errors="replace")
                if name == "file":
                    current_field = "file"
                    if fname:
                        cd_filename = fname
                elif name:
                    current_field = name

        def on_part_data(data, start, end):
            nonlocal file_size
            chunk = data[start:end]
            if current_field == "file":
                file_size += len(chunk)
                if file_size > MAX_UPLOAD_SIZE:
                    raise HTTPException(status_code=413, detail="File too large")
                file_data.extend(chunk)
            elif current_field:
                form_fields[current_field] = form_fields.get(current_field, b"") + chunk

        def on_part_end():
            pass

        def on_end():
            pass

        callbacks = {
            "on_part_begin": on_part_begin,
            "on_part_data": on_part_data,
            "on_part_end": on_part_end,
            "on_header_field": on_header_field,
            "on_header_value": on_header_value,
            "on_headers_finished": on_headers_finished,
            "on_end": on_end,
        }
        parser = multipart_mod.MultipartParser(boundary, callbacks)

        async for chunk in request.stream():
            parser.write(chunk)
        parser.finalize()

        # ── 4. Resolve filename and validate ──
        form_filename = form_fields.get("filename", b"").decode("utf-8", errors="replace")

        if not file_data:
            raise HTTPException(status_code=400, detail="Missing file upload")

        raw_name = form_filename or cd_filename
        if not raw_name:
            raise HTTPException(status_code=400, detail="Missing filename")

        try:
            safe_name = sanitize_filename(raw_name)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid filename")

        _, ext = os.path.splitext(safe_name)
        ext = ext.lower()

        if not ext or ext not in SUPPORTED_EXTENSIONS:
            raise HTTPException(
                status_code=415,
                detail=f"Unsupported file extension: {ext or '(none)'}",
            )

        logger.info("[Converter] Converting %s (%s, %d bytes)", safe_name, ext, file_size)

        # ── 5. Write to disk with correct filename for converter ──
        tmp_dir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmp_dir, safe_name)
        with open(tmp_path, "wb") as f:
            f.write(file_data)
        del file_data  # free memory before conversion

        # ── 6. Wait for a conversion slot ──
        while True:
            try:
                await asyncio.wait_for(conversion_semaphore.acquire(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                if await request.is_disconnected():
                    logger.info("[Converter] Client disconnected while queued: %s", safe_name)
                    return PlainTextResponse(content="", status_code=499)

        try:
            # Run conversion in a thread; periodically check for disconnect
            loop = asyncio.get_event_loop()
            task = loop.run_in_executor(None, convert, tmp_path, ext, CONVERSION_TIMEOUT)
            while True:
                done, _ = await asyncio.wait({task}, timeout=2.0)
                if done:
                    markdown = task.result()
                    break
                if await request.is_disconnected():
                    logger.info("[Converter] Client disconnected during conversion: %s", safe_name)
                    return PlainTextResponse(content="", status_code=499)
        finally:
            conversion_semaphore.release()

        # Free disk space immediately
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir = None

        return PlainTextResponse(
            content=markdown,
            media_type="text/markdown; charset=utf-8",
        )

    except ValueError as e:
        detail = str(e) if str(e) else "Invalid file"
        raise HTTPException(status_code=415, detail=detail)
    except HTTPException:
        raise
    except MemoryLimitExceeded as e:
        logger.warning("[Converter] Memory limit exceeded: %s", str(e))
        raise HTTPException(status_code=507, detail=str(e))
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Conversion timed out")
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Conversion timed out")
    except Exception as e:
        logger.error("[Converter] Conversion failed: %s", str(e))
        raise HTTPException(status_code=422, detail=f"Conversion failed: {str(e)}")
    finally:
        _queue_slots.release()
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
