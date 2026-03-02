# Markdown Converter Image

A lightweight memory-efficient HTTP API in a Docker Image that converts documents to Markdown using multiple converters.

## Supported Formats

| Extension | Converter | Notes |
|-----------|-----------|-------|
| `.rtf`, `.odt`, `.txt` | Pandoc | |
| `.docx` | Pandoc → MarkItDown | Falls back to MarkItDown on Pandoc heap exhaustion (table-heavy documents) |
| `.doc` | Auto-detected: RTF → Pandoc, OLE2 binary → antiword → MarkItDown → Pandoc fallback chain | |
| `.pptx`, `.pdf` | MarkItDown | |
| `.xls`, `.xlsx` | python-calamine (direct) | |

Password-protected Office files (`.docx`, `.xlsx`, `.pptx`) are detected and rejected early.

## API

**`POST /convert`** — Convert a document to Markdown

```bash
curl -F "file=@document.docx" -F "filename=document.docx" http://localhost:8100/convert
```

Returns `text/markdown` on success.

| Status | Meaning |
|--------|---------|
| `200` | Success |
| `400` | Missing or invalid filename |
| `413` | File too large |
| `415` | Unsupported format or password-protected file |
| `422` | Conversion failed |
| `429` | Too many conversion requests queued |
| `499` | Client disconnected before conversion completed |
| `504` | Conversion timed out |
| `507` | Conversion exceeded memory limit |

**`GET /health`** — Health check

```bash
curl http://localhost:8100/health
# {"status": "ok", "pandoc": true, "markitdown": true}
```

## Running

### Docker Compose

```bash
docker compose -f docker-compose.test.yml up
```

### Standalone

```bash
pip install -r requirements.txt
uvicorn app:app --port 8100
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MAX_UPLOAD_SIZE` | `10485760` (10MB) | Maximum upload size in bytes |
| `CONVERSION_TIMEOUT` | `120` | Subprocess timeout in seconds |
| `MAX_CONCURRENT_CONVERSIONS` | `1` | Maximum parallel conversions |
| `MAX_QUEUED_CONVERSIONS` | `5` | Maximum requests waiting in queue |
| `PANDOC_MAX_HEAP` | `128m` | Pandoc RTS max heap size (`-M`); on heap exhaustion `.docx` files fall back to MarkItDown automatically |
| `SUBPROCESS_MEMORY_LIMIT_MB` | `350` | RSS limit per conversion subprocess (MB). The parent polls `/proc` and kills the process group if RSS exceeds this. Set to `0` to disable. Only enforced on Linux. |

**Container memory sizing:**

> **Container MB** ≥ `SUBPROCESS_MEMORY_LIMIT_MB` × `MAX_CONCURRENT_CONVERSIONS` + `MAX_UPLOAD_SIZE` MB × (`MAX_CONCURRENT_CONVERSIONS` + `MAX_QUEUED_CONVERSIONS`) + 80 MB base
>
> *Defaults:* 350×1 + 10×6 + 80 = **490 MB** → use a 512 MB container

## Architecture Notes

- **Streaming multipart parsing** — File bytes are accumulated in memory during upload with incremental size checking (`MAX_UPLOAD_SIZE` enforced per-chunk), then written to a temp file with the correct filename just before conversion. This avoids the double-memory overhead of Starlette's built-in `request.form()`.
- **Subprocess isolation** — MarkItDown and calamine conversions run in child processes so memory is fully returned to the OS after each conversion.
- **RSS memory watchdog** — Each converter subprocess runs in its own process group (`start_new_session`). The parent polls `/proc/*/statm` every 100ms, summing RSS across the entire group (child + any grandchildren). If total RSS exceeds `SUBPROCESS_MEMORY_LIMIT_MB`, the whole group is killed and the request returns `507`. This prevents OOM kills at the container level.
- **Queue bounding** — Total in-flight requests (active + queued) are capped at `MAX_CONCURRENT_CONVERSIONS + MAX_QUEUED_CONVERSIONS` to prevent memory exhaustion under load. Excess requests receive `429` immediately, before the request body is read.
- **Client disconnect detection** — While queued or during conversion, the server periodically checks for client disconnects and aborts early (status `499`).

## Testing

```bash
pip install pytest httpx
python -m pytest test_converter.py -v
```

## Tech Stack

Python 3.12 · FastAPI · Pandoc · MarkItDown · python-calamine · antiword
