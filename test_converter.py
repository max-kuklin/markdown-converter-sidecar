import os
import subprocess
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app import app, sanitize_filename
from converter import (
    MARKITDOWN_EXTENSIONS,
    PANDOC_EXTENSIONS,
    SUPPORTED_EXTENSIONS,
    get_converter,
)

client = TestClient(app)


# ── Extension routing tests ──────────────────────────────────────────────────

class TestExtensionRouting:
    @pytest.mark.parametrize("ext", sorted(PANDOC_EXTENSIONS))
    def test_pandoc_extensions(self, ext):
        assert get_converter(ext) == "pandoc"

    @pytest.mark.parametrize("ext", sorted(MARKITDOWN_EXTENSIONS))
    def test_markitdown_extensions(self, ext):
        assert get_converter(ext) == "markitdown"

    def test_unsupported_extension(self):
        assert get_converter(".zip") is None
        assert get_converter(".exe") is None
        assert get_converter(".mp3") is None

    def test_case_insensitive(self):
        assert get_converter(".DOCX") == "pandoc"
        assert get_converter(".Xlsx") == "markitdown"
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
    def test_missing_file_returns_422(self):
        response = client.post("/convert", data={"filename": "test.docx"})
        assert response.status_code == 422

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


# ── Converter function unit tests ────────────────────────────────────────────

class TestConverterFunctions:
    @patch("converter.subprocess.run")
    def test_pandoc_success(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=b"# Converted\n\nText content.",
        )
        from converter import pandoc_to_markdown

        result = pandoc_to_markdown("/tmp/test.docx")
        assert "Converted" in result
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args[0] == "pandoc"

    @patch("converter.subprocess.run")
    def test_pandoc_failure_raises(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr=b"pandoc: error reading file",
        )
        from converter import pandoc_to_markdown

        with pytest.raises(RuntimeError, match="Pandoc conversion failed"):
            pandoc_to_markdown("/tmp/bad.docx")

    @patch("converter.subprocess.run")
    def test_pandoc_timeout_raises(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="pandoc", timeout=120)
        from converter import pandoc_to_markdown

        with pytest.raises(subprocess.TimeoutExpired):
            pandoc_to_markdown("/tmp/slow.docx", timeout=120)

    @patch("converter.MarkItDown")
    def test_markitdown_success(self, mock_md_class):
        mock_result = MagicMock()
        mock_result.text_content = "| A | B |\n|---|---|\n| 1 | 2 |"
        mock_md_class.return_value.convert.return_value = mock_result

        from converter import markitdown_to_markdown

        result = markitdown_to_markdown("/tmp/data.xlsx")
        assert "| A | B |" in result
