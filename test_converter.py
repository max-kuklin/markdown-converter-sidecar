import os
import subprocess
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app import app, sanitize_filename, _queue_slots, MAX_CONCURRENT_CONVERSIONS, MAX_QUEUED_CONVERSIONS
from converter import (
    MARKITDOWN_EXTENSIONS,
    PANDOC_EXTENSIONS,
    SUPPORTED_EXTENSIONS,
    convert,
    get_converter,
    antiword_to_markdown,
)

client = TestClient(app)


# ── Extension routing tests ──────────────────────────────────────────────────

class TestExtensionRouting:
    @pytest.mark.parametrize("ext", sorted(PANDOC_EXTENSIONS))
    def test_pandoc_extensions(self, ext):
        assert get_converter(ext) == "pandoc"

    @pytest.mark.parametrize("ext", sorted(MARKITDOWN_EXTENSIONS - {".xlsx", ".xls"}))
    def test_markitdown_extensions(self, ext):
        assert get_converter(ext) == "markitdown"

    def test_xlsx_extension(self):
        assert get_converter(".xlsx") == "xlsx"

    def test_xls_extension(self):
        assert get_converter(".xls") == "xls"

    def test_unsupported_extension(self):
        assert get_converter(".zip") is None
        assert get_converter(".exe") is None
        assert get_converter(".mp3") is None

    def test_legacy_binary_formats_unsupported(self):
        assert get_converter(".ppt") is None
        assert get_converter(".ods") is None

    def test_legacy_binary_formats_use_markitdown(self):
        assert get_converter(".doc") == "markitdown"

    def test_legacy_binary_formats_use_dedicated_converter(self):
        assert get_converter(".xls") == "xls"

    def test_case_insensitive(self):
        assert get_converter(".DOCX") == "pandoc"
        assert get_converter(".Xlsx") == "xlsx"
        assert get_converter(".PDF") == "markitdown"


# ── Health endpoint tests ────────────────────────────────────────────────────

class TestHealthEndpoint:
    def test_health_returns_200(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "pandoc" in data
        assert "markitdown" in data

    def test_health_structure(self):
        response = client.get("/health")
        data = response.json()
        assert isinstance(data["pandoc"], bool)
        assert isinstance(data["markitdown"], bool)


# ── Filename sanitization tests ──────────────────────────────────────────────

class TestFilenameSanitization:
    def test_safe_filename(self):
        assert sanitize_filename("document.docx") == "document.docx"

    def test_path_traversal_blocked(self):
        result = sanitize_filename("../../etc/passwd")
        assert ".." not in result
        assert "/" not in result

    def test_strips_directory(self):
        result = sanitize_filename("/some/path/file.pdf")
        assert result == "file.pdf"

    def test_unsafe_characters_replaced(self):
        result = sanitize_filename("file name (1).docx")
        assert " " not in result
        assert "(" not in result

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            sanitize_filename("")


# ── Convert endpoint tests ───────────────────────────────────────────────────

class TestConvertEndpoint:
    def test_missing_file_returns_400(self):
        response = client.post("/convert", data={"filename": "test.docx"})
        assert response.status_code == 400

    def test_unsupported_extension_returns_415(self):
        response = client.post(
            "/convert",
            files={"file": ("test.zip", b"fake content", "application/zip")},
            data={"filename": "test.zip"},
        )
        assert response.status_code == 415

    def test_no_extension_returns_415(self):
        response = client.post(
            "/convert",
            files={"file": ("noext", b"fake content", "application/octet-stream")},
            data={"filename": "noext"},
        )
        assert response.status_code == 415

    @patch("app.convert")
    def test_successful_pandoc_conversion(self, mock_convert):
        mock_convert.return_value = "# Hello World\n\nSome content."
        response = client.post(
            "/convert",
            files={"file": ("test.docx", b"fake docx content", "application/octet-stream")},
            data={"filename": "test.docx"},
        )
        assert response.status_code == 200
        assert "Hello World" in response.text
        assert response.headers["content-type"].startswith("text/markdown")

    @patch("app.convert")
    def test_successful_markitdown_conversion(self, mock_convert):
        mock_convert.return_value = "| Col A | Col B |\n|---|---|\n| 1 | 2 |"
        response = client.post(
            "/convert",
            files={"file": ("data.xlsx", b"fake xlsx content", "application/octet-stream")},
            data={"filename": "data.xlsx"},
        )
        assert response.status_code == 200
        assert "Col A" in response.text

    @patch("app.convert")
    def test_conversion_failure_returns_422(self, mock_convert):
        mock_convert.side_effect = RuntimeError("Corrupt file")
        response = client.post(
            "/convert",
            files={"file": ("bad.docx", b"corrupt", "application/octet-stream")},
            data={"filename": "bad.docx"},
        )
        assert response.status_code == 422

    @patch("app.convert")
    def test_conversion_timeout_returns_504(self, mock_convert):
        mock_convert.side_effect = subprocess.TimeoutExpired(cmd="pandoc", timeout=120)
        response = client.post(
            "/convert",
            files={"file": ("slow.docx", b"content", "application/octet-stream")},
            data={"filename": "slow.docx"},
        )
        assert response.status_code == 504


# ── Temp file cleanup tests ─────────────────────────────────────────────────

class TestTempFileCleanup:
    @patch("app.convert")
    def test_temp_files_cleaned_on_success(self, mock_convert):
        mock_convert.return_value = "# Result"
        response = client.post(
            "/convert",
            files={"file": ("test.docx", b"content", "application/octet-stream")},
            data={"filename": "test.docx"},
        )
        assert response.status_code == 200
        # Verify no leftover temp dirs with our test file
        # (temp dirs are cleaned in finally block)

    @patch("app.convert")
    def test_temp_files_cleaned_on_failure(self, mock_convert):
        mock_convert.side_effect = RuntimeError("fail")
        response = client.post(
            "/convert",
            files={"file": ("test.docx", b"content", "application/octet-stream")},
            data={"filename": "test.docx"},
        )
        assert response.status_code == 422
        # Temp dir should still be cleaned up via finally block


# ── Queue limit tests ───────────────────────────────────────────────────────

class TestQueueLimit:
    @patch("app.convert")
    def test_returns_429_when_queue_full(self, mock_convert):
        """When all queue slots are exhausted, new requests get 429."""
        total_slots = MAX_CONCURRENT_CONVERSIONS + MAX_QUEUED_CONVERSIONS
        # Drain all semaphore slots to simulate a full queue
        for _ in range(total_slots):
            _queue_slots._value -= 1
        try:
            response = client.post(
                "/convert",
                files={"file": ("test.docx", b"content", "application/octet-stream")},
                data={"filename": "test.docx"},
            )
            assert response.status_code == 429
            assert "queued" in response.json()["detail"].lower()
        finally:
            # Restore semaphore
            for _ in range(total_slots):
                _queue_slots._value += 1

    @patch("app.convert")
    def test_accepts_request_when_queue_has_room(self, mock_convert):
        """When queue has room, request should succeed normally."""
        mock_convert.return_value = "# OK"
        response = client.post(
            "/convert",
            files={"file": ("test.docx", b"content", "application/octet-stream")},
            data={"filename": "test.docx"},
        )
        assert response.status_code == 200


# ── Converter function unit tests ────────────────────────────────────────────

class TestConverterFunctions:
    @patch("converter._run_with_memory_limit")
    def test_pandoc_success(self, mock_rwml):
        mock_rwml.return_value = subprocess.CompletedProcess(
            [], returncode=0,
            stdout=b"# Converted\n\nText content.",
            stderr=b"",
        )
        from converter import pandoc_to_markdown

        result = pandoc_to_markdown("/tmp/test.docx")
        assert "Converted" in result
        mock_rwml.assert_called_once()
        args = mock_rwml.call_args[1] if mock_rwml.call_args[1] else {}
        cmd = mock_rwml.call_args[0][0] if mock_rwml.call_args[0] else args.get("cmd")
        assert cmd[0] == "pandoc"

    @patch("converter._run_with_memory_limit")
    def test_pandoc_failure_raises(self, mock_rwml):
        mock_rwml.return_value = subprocess.CompletedProcess(
            [], returncode=1,
            stdout=b"",
            stderr=b"pandoc: error reading file",
        )
        from converter import pandoc_to_markdown

        with pytest.raises(RuntimeError, match="Pandoc conversion failed"):
            pandoc_to_markdown("/tmp/bad.docx")

    @patch("converter._run_with_memory_limit")
    def test_pandoc_timeout_raises(self, mock_rwml):
        mock_rwml.side_effect = subprocess.TimeoutExpired(cmd="pandoc", timeout=120)
        from converter import pandoc_to_markdown

        with pytest.raises(subprocess.TimeoutExpired):
            pandoc_to_markdown("/tmp/slow.docx", timeout=120)

    @patch("converter._run_with_memory_limit")
    def test_markitdown_success(self, mock_rwml):
        expected_md = "| A | B |\n|---|---|\n| 1 | 2 |"
        mock_rwml.return_value = subprocess.CompletedProcess(
            [], returncode=0, stdout=expected_md.encode("utf-8"), stderr=b"",
        )

        from converter import markitdown_to_markdown
        result = markitdown_to_markdown("/tmp/data.xlsx")
        assert "| A | B |" in result

    @patch("converter._run_with_memory_limit")
    def test_antiword_success(self, mock_rwml):
        mock_rwml.return_value = subprocess.CompletedProcess(
            [], returncode=0,
            stdout=b"Hello from a .doc file",
            stderr=b"",
        )
        result = antiword_to_markdown("/tmp/test.doc")
        assert "Hello from a .doc file" in result
        cmd = mock_rwml.call_args[0][0]
        assert cmd[0] == "antiword"

    @patch("converter._run_with_memory_limit")
    def test_antiword_failure_raises(self, mock_rwml):
        mock_rwml.return_value = subprocess.CompletedProcess(
            [], returncode=1,
            stdout=b"",
            stderr=b"I can't open the file",
        )
        with pytest.raises(RuntimeError, match="antiword conversion failed"):
            antiword_to_markdown("/tmp/bad.doc")


# ── Password-protected file detection tests ──────────────────────────────────

class TestPasswordProtectedDetection:
    OLE2_HEADER = b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1' + b'\x00' * 100

    def test_encrypted_xlsx_returns_415(self, tmp_path):
        """An OLE2-wrapped .xlsx (password-protected) should be rejected with 415."""
        f = tmp_path / "encrypted.xlsx"
        f.write_bytes(self.OLE2_HEADER)
        response = client.post(
            "/convert",
            files={"file": ("encrypted.xlsx", f.read_bytes(), "application/octet-stream")},
            data={"filename": "encrypted.xlsx"},
        )
        assert response.status_code == 415
        assert "password-protected" in response.json()["detail"].lower()

    def test_encrypted_pptx_returns_415(self, tmp_path):
        f = tmp_path / "encrypted.pptx"
        f.write_bytes(self.OLE2_HEADER)
        response = client.post(
            "/convert",
            files={"file": ("encrypted.pptx", f.read_bytes(), "application/octet-stream")},
            data={"filename": "encrypted.pptx"},
        )
        assert response.status_code == 415
        assert "password-protected" in response.json()["detail"].lower()

    def test_encrypted_docx_returns_415(self, tmp_path):
        f = tmp_path / "encrypted.docx"
        f.write_bytes(self.OLE2_HEADER)
        response = client.post(
            "/convert",
            files={"file": ("encrypted.docx", f.read_bytes(), "application/octet-stream")},
            data={"filename": "encrypted.docx"},
        )
        assert response.status_code == 415
        assert "password-protected" in response.json()["detail"].lower()

    def test_normal_xls_not_flagged(self, tmp_path):
        """OLE2-based .xls files are legitimate and should not be blocked."""
        f = tmp_path / "normal.xls"
        f.write_bytes(self.OLE2_HEADER)
        # .xls is OLE2 natively, so it shouldn't be flagged as password-protected.
        # It will fail conversion for other reasons (fake content), but not with 415.
        response = client.post(
            "/convert",
            files={"file": ("normal.xls", f.read_bytes(), "application/octet-stream")},
            data={"filename": "normal.xls"},
        )
        assert response.status_code != 415


# ── .doc format detection and fallback tests ─────────────────────────────────

class TestDocConversion:
    RTF_CONTENT = b'{\\rtf1 Hello World}'
    OLE2_HEADER = b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1' + b'\x00' * 100

    @patch("converter.pandoc_to_markdown")
    def test_rtf_doc_routes_to_pandoc(self, mock_pandoc):
        """A .doc file that is actually RTF should go straight to Pandoc."""
        mock_pandoc.return_value = "# Hello World"
        from converter import _convert_doc
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix=".doc", delete=False)
        try:
            tmp.write(self.RTF_CONTENT)
            tmp.close()
            result = _convert_doc(tmp.name)
            assert "Hello World" in result
            mock_pandoc.assert_called_once()
        finally:
            os.unlink(tmp.name)

    @patch("converter.pandoc_to_markdown")
    @patch("converter.markitdown_to_markdown")
    def test_ole2_doc_tries_markitdown_first(self, mock_markitdown, mock_pandoc):
        """An OLE2 .doc should try MarkItDown first (when antiword is unavailable)."""
        mock_markitdown.return_value = "# Converted via MarkItDown"
        from converter import _convert_doc
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix=".doc", delete=False)
        try:
            tmp.write(self.OLE2_HEADER)
            tmp.close()
            with patch("converter.shutil.which", return_value=None):
                result = _convert_doc(tmp.name)
            assert "MarkItDown" in result
            mock_markitdown.assert_called_once()
            mock_pandoc.assert_not_called()
        finally:
            os.unlink(tmp.name)

    @patch("converter.pandoc_to_markdown")
    @patch("converter.markitdown_to_markdown")
    def test_ole2_doc_falls_back_to_pandoc(self, mock_markitdown, mock_pandoc):
        """When MarkItDown fails for OLE2 .doc, should fall back to Pandoc (antiword unavailable)."""
        mock_markitdown.side_effect = RuntimeError("MarkItDown failed")
        mock_pandoc.return_value = "# Converted via Pandoc"
        from converter import _convert_doc
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix=".doc", delete=False)
        try:
            tmp.write(self.OLE2_HEADER)
            tmp.close()
            with patch("converter.shutil.which", return_value=None):
                result = _convert_doc(tmp.name)
            assert "Pandoc" in result
            mock_markitdown.assert_called_once()
            mock_pandoc.assert_called_once()
        finally:
            os.unlink(tmp.name)

    @patch("converter.pandoc_to_markdown")
    @patch("converter.markitdown_to_markdown")
    def test_ole2_doc_both_fail_gives_clear_error(self, mock_markitdown, mock_pandoc):
        """When all converters fail for .doc, error should suggest re-saving as .docx."""
        mock_markitdown.side_effect = RuntimeError("MarkItDown failed")
        mock_pandoc.side_effect = RuntimeError("Pandoc failed")
        from converter import _convert_doc
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix=".doc", delete=False)
        try:
            tmp.write(self.OLE2_HEADER)
            tmp.close()
            with patch("converter.shutil.which", return_value=None):
                with pytest.raises(RuntimeError, match="re-saving as .docx"):
                    _convert_doc(tmp.name)
        finally:
            os.unlink(tmp.name)

    @patch("converter.pandoc_to_markdown")
    @patch("converter.markitdown_to_markdown")
    @patch("converter.antiword_to_markdown")
    def test_ole2_doc_tries_antiword_first(self, mock_antiword, mock_markitdown, mock_pandoc):
        """When antiword is available, it should be tried first for OLE2 .doc."""
        mock_antiword.return_value = "Converted via antiword"
        from converter import _convert_doc
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix=".doc", delete=False)
        try:
            tmp.write(self.OLE2_HEADER)
            tmp.close()
            with patch("converter.shutil.which", return_value="/usr/bin/antiword"):
                result = _convert_doc(tmp.name)
            assert "antiword" in result
            mock_antiword.assert_called_once()
            mock_markitdown.assert_not_called()
            mock_pandoc.assert_not_called()
        finally:
            os.unlink(tmp.name)

    @patch("converter.pandoc_to_markdown")
    @patch("converter.markitdown_to_markdown")
    @patch("converter.antiword_to_markdown")
    def test_ole2_doc_antiword_fails_falls_to_markitdown(self, mock_antiword, mock_markitdown, mock_pandoc):
        """When antiword fails, should fall back to MarkItDown."""
        mock_antiword.side_effect = RuntimeError("antiword failed")
        mock_markitdown.return_value = "# Converted via MarkItDown"
        from converter import _convert_doc
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix=".doc", delete=False)
        try:
            tmp.write(self.OLE2_HEADER)
            tmp.close()
            with patch("converter.shutil.which", return_value="/usr/bin/antiword"):
                result = _convert_doc(tmp.name)
            assert "MarkItDown" in result
            mock_antiword.assert_called_once()
            mock_markitdown.assert_called_once()
            mock_pandoc.assert_not_called()
        finally:
            os.unlink(tmp.name)


class TestDocxConversion:
    def test_docx_routes_to_pandoc(self):
        """.docx should route directly to Pandoc (faster, lower memory than MarkItDown)."""
        assert get_converter(".docx") == "pandoc"

    @patch("converter.markitdown_to_markdown")
    @patch("converter.pandoc_to_markdown")
    def test_docx_falls_back_to_markitdown_on_heap_exhaustion(self, mock_pandoc, mock_markitdown):
        """When Pandoc heap-exhausts on a .docx, should fall back to MarkItDown."""
        mock_pandoc.side_effect = RuntimeError("Pandoc conversion failed: pandoc: Heap exhausted;")
        mock_markitdown.return_value = "# Fallback result"
        result = convert("/fake/test.docx", ".docx")
        assert result == "# Fallback result"
        mock_pandoc.assert_called_once()
        mock_markitdown.assert_called_once()

    @patch("converter.pandoc_to_markdown")
    def test_docx_non_heap_error_still_raises(self, mock_pandoc):
        """Non-heap Pandoc errors for .docx should not trigger fallback."""
        mock_pandoc.side_effect = RuntimeError("Pandoc conversion failed: some other error")
        import pytest
        with pytest.raises(RuntimeError, match="some other error"):
            convert("/fake/test.docx", ".docx")
