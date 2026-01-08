"""
Unit tests for check_cricsheet_updates.py script.

Tests cover:
- Getting local files from directory
- Downloading and parsing Cricsheet zip files
- Comparing local vs remote file lists
- Extracting files from zip archives
- Command-line argument handling
- Error handling and edge cases
"""

import pytest
import sys
import io
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
import requests

# Add scripts/python to path so we can import the module
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts" / "python"))

from check_cricsheet_updates import (
    get_local_files,
    get_cricsheet_files,
    extract_files,
    main,
    CRICSHEET_ZIP_URL,
    LOCAL_DATA_DIR
)


class TestGetLocalFiles:
    """Tests for get_local_files() function."""
    
    def test_returns_empty_set_when_directory_does_not_exist(self, tmp_path, monkeypatch):
        """Should return empty set and create directory if it doesn't exist."""
        non_existent_dir = tmp_path / "data" / "raw" / "all_json"
        monkeypatch.setattr("check_cricsheet_updates.LOCAL_DATA_DIR", non_existent_dir)
        
        result = get_local_files(verbose=False)
        
        assert result == set()
        assert non_existent_dir.exists()
    
    def test_returns_json_files_from_directory(self, tmp_path, monkeypatch):
        """Should return set of JSON filenames in the directory."""
        data_dir = tmp_path / "all_json"
        data_dir.mkdir()
        monkeypatch.setattr("check_cricsheet_updates.LOCAL_DATA_DIR", data_dir)
        
        # Create test files
        (data_dir / "1000851.json").touch()
        (data_dir / "1000853.json").touch()
        (data_dir / "notes.txt").touch()  # Should be ignored
        
        result = get_local_files(verbose=False)
        
        assert result == {"1000851.json", "1000853.json"}
    
    def test_returns_empty_set_for_empty_directory(self, tmp_path, monkeypatch):
        """Should return empty set if directory exists but is empty."""
        data_dir = tmp_path / "all_json"
        data_dir.mkdir()
        monkeypatch.setattr("check_cricsheet_updates.LOCAL_DATA_DIR", data_dir)
        
        result = get_local_files(verbose=False)
        
        assert result == set()
    
    def test_verbose_output_shows_debug_info(self, tmp_path, monkeypatch, capsys):
        """Should print debug info when verbose=True."""
        data_dir = tmp_path / "all_json"
        data_dir.mkdir()
        (data_dir / "test.json").touch()
        monkeypatch.setattr("check_cricsheet_updates.LOCAL_DATA_DIR", data_dir)
        
        result = get_local_files(verbose=True)
        
        captured = capsys.readouterr()
        assert "[DEBUG]" in captured.out
        assert "Checking local directory" in captured.out
        assert "Found 1 local JSON files" in captured.out


class TestGetCricsheetFiles:
    """Tests for get_cricsheet_files() function."""
    
    def create_test_zip(self, filenames):
        """Helper to create a test zip file in memory."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for filename in filenames:
                zip_file.writestr(filename, f"content of {filename}")
        zip_buffer.seek(0)
        return zip_buffer
    
    def test_downloads_and_parses_zip_successfully(self, capsys):
        """Should download zip and return set of JSON files."""
        test_files = ["1000851.json", "1000853.json", "1000855.json"]
        zip_data = self.create_test_zip(test_files)
        
        mock_response = Mock()
        mock_response.content = zip_data.read()
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response) as mock_get:
            files, returned_zip = get_cricsheet_files()
        
        assert files == {"1000851.json", "1000853.json", "1000855.json"}
        mock_get.assert_called_once_with(CRICSHEET_ZIP_URL, timeout=60)
        captured = capsys.readouterr()
        assert "✓" in captured.out
    
    def test_excludes_macosx_metadata_files(self):
        """Should filter out __MACOSX metadata files from the zip."""
        test_files = [
            "1000851.json",
            "__MACOSX/._1000851.json",
            "1000853.json",
            "__MACOSX/.DS_Store"
        ]
        zip_data = self.create_test_zip(test_files)
        
        mock_response = Mock()
        mock_response.content = zip_data.read()
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            files, _ = get_cricsheet_files()
        
        assert files == {"1000851.json", "1000853.json"}
    
    def test_excludes_non_json_files(self):
        """Should only return JSON files from the zip."""
        test_files = [
            "1000851.json",
            "README.txt",
            "data.csv",
            "1000853.json"
        ]
        zip_data = self.create_test_zip(test_files)
        
        mock_response = Mock()
        mock_response.content = zip_data.read()
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            files, _ = get_cricsheet_files()
        
        assert files == {"1000851.json", "1000853.json"}
    
    def test_exits_on_network_error(self, capsys):
        """Should exit with error message on network failure."""
        with patch('requests.get', side_effect=requests.RequestException("Connection error")):
            with pytest.raises(SystemExit) as exc_info:
                get_cricsheet_files()
        
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Failed to fetch Cricsheet zip file" in captured.err
        assert "✗" in captured.out
    
    def test_exits_on_bad_zip_file(self, capsys):
        """Should exit with error message if downloaded file is not a valid zip."""
        mock_response = Mock()
        mock_response.content = b"not a zip file"
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            with pytest.raises(SystemExit) as exc_info:
                get_cricsheet_files()
        
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Failed to read zip file" in captured.err
    
    def test_verbose_mode_shows_download_source(self, capsys):
        """Should show download source in verbose mode."""
        test_files = ["1000851.json"]
        zip_data = self.create_test_zip(test_files)
        
        mock_response = Mock()
        mock_response.content = zip_data.read()
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            files, _ = get_cricsheet_files(verbose=True)
        
        captured = capsys.readouterr()
        assert "[DEBUG] Downloading from:" in captured.out
        assert "[DEBUG]" in captured.out  # Other debug messages


class TestExtractFiles:
    """Tests for extract_files() function."""
    
    def create_test_zip_with_content(self, files_dict):
        """Helper to create a zip with specific files and content."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for filename, content in files_dict.items():
                zip_file.writestr(filename, content)
        zip_buffer.seek(0)
        return zip_buffer
    
    def test_extracts_files_to_output_directory(self, tmp_path, capsys):
        """Should extract specified files from zip to output directory."""
        files_dict = {
            "1000851.json": '{"match": "data1"}',
            "1000853.json": '{"match": "data2"}'
        }
        zip_data = self.create_test_zip_with_content(files_dict)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        result = extract_files(zip_data, ["1000851.json", "1000853.json"], output_dir)
        
        assert result == 2
        assert (output_dir / "1000851.json").exists()
        assert (output_dir / "1000853.json").exists()
        assert (output_dir / "1000851.json").read_text() == '{"match": "data1"}'
    
    def test_handles_nested_paths_in_zip(self, tmp_path, capsys):
        """Should flatten nested paths when extracting files."""
        files_dict = {
            "subfolder/1000851.json": '{"match": "data1"}',
            "another/path/1000853.json": '{"match": "data2"}'
        }
        zip_data = self.create_test_zip_with_content(files_dict)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        result = extract_files(
            zip_data, 
            ["subfolder/1000851.json", "another/path/1000853.json"], 
            output_dir
        )
        
        assert result == 2
        # Files should be flattened to output dir
        assert (output_dir / "1000851.json").exists()
        assert (output_dir / "1000853.json").exists()
        # Single-level subdirs are cleaned up, but nested paths may leave parent dirs
        assert not (output_dir / "subfolder" / "1000851.json").exists()
        assert not (output_dir / "another" / "path" / "1000853.json").exists()
    
    def test_continues_on_extraction_error(self, tmp_path, capsys):
        """Should continue extracting other files if one fails."""
        files_dict = {
            "1000851.json": '{"match": "data1"}',
            "1000853.json": '{"match": "data2"}'
        }
        zip_data = self.create_test_zip_with_content(files_dict)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        # Try to extract a file that doesn't exist and one that does
        result = extract_files(
            zip_data, 
            ["nonexistent.json", "1000851.json"], 
            output_dir
        )
        
        assert result == 1  # Only one successful
        assert (output_dir / "1000851.json").exists()
        captured = capsys.readouterr()
        assert "✗" in captured.out  # Error marker for failed file
        assert "✓" in captured.out  # Success marker for successful file
    
    def test_verbose_output_on_extract(self, tmp_path, capsys):
        """Should show verbose debug info during extraction."""
        files_dict = {"1000851.json": '{"match": "data1"}'}
        zip_data = self.create_test_zip_with_content(files_dict)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        result = extract_files(zip_data, ["1000851.json"], output_dir, verbose=True)
        
        captured = capsys.readouterr()
        assert "[DEBUG] Extracting" in captured.out
        assert "1 files" in captured.out


class TestMain:
    """Tests for main() function and command-line interface."""
    
    def create_mock_zip_data(self, filenames):
        """Helper to create mock zip data."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for filename in filenames:
                zip_file.writestr(filename, f"content of {filename}")
        zip_buffer.seek(0)
        return zip_buffer
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    def test_check_only_mode_shows_summary(self, mock_get_cricsheet, mock_get_local, capsys):
        """Should display summary without downloading when no args provided."""
        mock_get_local.return_value = {"1000851.json", "1000853.json"}
        cricsheet_files = {"1000851.json", "1000853.json", "1000855.json"}
        mock_zip = self.create_mock_zip_data(cricsheet_files)
        mock_get_cricsheet.return_value = (cricsheet_files, mock_zip)
        
        with patch('sys.argv', ['check_cricsheet_updates.py']):
            main()
        
        captured = capsys.readouterr()
        assert "Local files:" in captured.out
        assert "Cricsheet files:" in captured.out
        assert "New files:" in captured.out
        assert "1000855.json" in captured.out
        assert "To download new files" in captured.out
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    def test_reports_when_up_to_date(self, mock_get_cricsheet, mock_get_local, capsys):
        """Should report no changes when local and remote match."""
        files = {"1000851.json", "1000853.json"}
        mock_get_local.return_value = files
        mock_zip = self.create_mock_zip_data(files)
        mock_get_cricsheet.return_value = (files, mock_zip)
        
        with patch('sys.argv', ['check_cricsheet_updates.py']):
            main()
        
        captured = capsys.readouterr()
        assert "No changes detected" in captured.out
        assert "up to date" in captured.out
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    def test_shows_removed_files_warning(self, mock_get_cricsheet, mock_get_local, capsys):
        """Should warn when local files are no longer on Cricsheet."""
        mock_get_local.return_value = {"1000851.json", "1000853.json", "1000855.json"}
        cricsheet_files = {"1000851.json"}
        mock_zip = self.create_mock_zip_data(cricsheet_files)
        mock_get_cricsheet.return_value = (cricsheet_files, mock_zip)
        
        with patch('sys.argv', ['check_cricsheet_updates.py']):
            main()
        
        captured = capsys.readouterr()
        assert "Files removed from Cricsheet" in captured.out
        assert "1000853.json" in captured.out or "1000855.json" in captured.out
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    @patch('check_cricsheet_updates.extract_files')
    def test_download_mode_extracts_files(
        self, mock_extract, mock_get_cricsheet, mock_get_local, tmp_path, capsys
    ):
        """Should extract new files when --download flag is provided."""
        mock_get_local.return_value = {"1000851.json"}
        cricsheet_files = {"1000851.json", "1000853.json", "1000855.json"}
        mock_zip = self.create_mock_zip_data(cricsheet_files)
        mock_get_cricsheet.return_value = (cricsheet_files, mock_zip)
        mock_extract.return_value = 2
        
        with patch('sys.argv', ['check_cricsheet_updates.py', '--download']):
            main()
        
        mock_extract.assert_called_once()
        # Check that it tried to extract the new files
        call_args = mock_extract.call_args[0]
        extracted_files = call_args[1]
        assert "1000853.json" in extracted_files
        assert "1000855.json" in extracted_files
        assert "1000851.json" not in extracted_files  # Already exists locally
        
        captured = capsys.readouterr()
        assert "Extracted" in captured.out
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    @patch('check_cricsheet_updates.extract_files')
    def test_limit_flag_restricts_downloads(
        self, mock_extract, mock_get_cricsheet, mock_get_local, capsys
    ):
        """Should limit number of files downloaded when --limit is provided."""
        mock_get_local.return_value = set()
        cricsheet_files = {f"{i}.json" for i in range(100)}
        mock_zip = self.create_mock_zip_data(cricsheet_files)
        mock_get_cricsheet.return_value = (cricsheet_files, mock_zip)
        mock_extract.return_value = 10
        
        with patch('sys.argv', ['check_cricsheet_updates.py', '--download', '--limit', '10']):
            main()
        
        mock_extract.assert_called_once()
        call_args = mock_extract.call_args[0]
        extracted_files = call_args[1]
        assert len(extracted_files) == 10
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    def test_exits_when_no_cricsheet_files_found(self, mock_get_cricsheet, mock_get_local, capsys):
        """Should exit with error when Cricsheet returns zero files."""
        mock_get_local.return_value = {"1000851.json"}
        mock_zip = self.create_mock_zip_data([])
        mock_get_cricsheet.return_value = (set(), mock_zip)
        
        with patch('sys.argv', ['check_cricsheet_updates.py']):
            with pytest.raises(SystemExit) as exc_info:
                main()
        
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Found 0 files on Cricsheet" in captured.err
        assert "download or zip processing failed" in captured.err
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    def test_shows_sample_of_new_files(self, mock_get_cricsheet, mock_get_local, capsys):
        """Should show first 10 new files and indicate if there are more."""
        mock_get_local.return_value = set()
        cricsheet_files = {f"{i:07d}.json" for i in range(1, 51)}  # 50 files
        mock_zip = self.create_mock_zip_data(cricsheet_files)
        mock_get_cricsheet.return_value = (cricsheet_files, mock_zip)
        
        with patch('sys.argv', ['check_cricsheet_updates.py']):
            main()
        
        captured = capsys.readouterr()
        # Should show sample and "and X more" message
        assert "New files available" in captured.out
        assert "and 40 more" in captured.out
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    def test_shows_more_message_for_many_removed_files(self, mock_get_cricsheet, mock_get_local, capsys):
        """Should show sample of removed files and 'and X more' message."""
        # Create scenario with many removed files
        local_files = {f"{i:07d}.json" for i in range(1, 21)}  # 20 local files
        cricsheet_files = {f"{i:07d}.json" for i in range(1, 6)}  # 5 on Cricsheet
        mock_get_local.return_value = local_files
        mock_zip = self.create_mock_zip_data(cricsheet_files)
        mock_get_cricsheet.return_value = (cricsheet_files, mock_zip)
        
        with patch('sys.argv', ['check_cricsheet_updates.py']):
            main()
        
        captured = capsys.readouterr()
        # Should show first 5 and indicate more
        assert "Files removed from Cricsheet" in captured.out
        assert "and 10 more" in captured.out
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    def test_suggests_limit_for_many_new_files(self, mock_get_cricsheet, mock_get_local, capsys):
        """Should suggest using --limit flag when there are many new files."""
        mock_get_local.return_value = set()
        # Create 150 new files
        cricsheet_files = {f"{i:07d}.json" for i in range(1, 151)}
        mock_zip = self.create_mock_zip_data(cricsheet_files)
        mock_get_cricsheet.return_value = (cricsheet_files, mock_zip)
        
        with patch('sys.argv', ['check_cricsheet_updates.py']):
            main()
        
        captured = capsys.readouterr()
        # Should suggest using limit flag
        assert "Or limit downloads:" in captured.out
        assert "--limit 100" in captured.out
    
    @patch('check_cricsheet_updates.get_local_files')
    @patch('check_cricsheet_updates.get_cricsheet_files')
    def test_verbose_mode_shows_debug_info(self, mock_get_cricsheet, mock_get_local, capsys):
        """Should show debug info in verbose mode."""
        mock_get_local.return_value = {"1000851.json"}
        cricsheet_files = {"1000851.json", "1000853.json"}
        mock_zip = self.create_mock_zip_data(cricsheet_files)
        mock_get_cricsheet.return_value = (cricsheet_files, mock_zip)
        
        with patch('sys.argv', ['check_cricsheet_updates.py', '--verbose']):
            main()
        
        captured = capsys.readouterr()
        assert "[VERBOSE MODE ENABLED]" in captured.out
        assert "[DEBUG] Comparison complete" in captured.out


class TestIntegration:
    """Integration tests that test multiple components together."""
    
    @patch('requests.get')
    def test_full_check_workflow(self, mock_get, tmp_path, monkeypatch, capsys):
        """Test complete workflow from checking to displaying results."""
        # Setup
        local_dir = tmp_path / "local"
        local_dir.mkdir()
        (local_dir / "1000851.json").touch()
        monkeypatch.setattr("check_cricsheet_updates.LOCAL_DATA_DIR", local_dir)
        
        # Create mock zip with new files
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            zf.writestr("1000851.json", "existing")
            zf.writestr("1000853.json", "new file")
        zip_buffer.seek(0)
        
        mock_response = Mock()
        mock_response.content = zip_buffer.read()
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Run
        with patch('sys.argv', ['check_cricsheet_updates.py']):
            main()
        
        # Verify
        captured = capsys.readouterr()
        assert "Local files:      1" in captured.out
        assert "Cricsheet files:  2" in captured.out
        assert "New files:        1" in captured.out
        assert "1000853.json" in captured.out
    
    @patch('requests.get')
    def test_full_download_workflow(self, mock_get, tmp_path, monkeypatch, capsys):
        """Test complete workflow from checking to downloading files."""
        # Setup
        local_dir = tmp_path / "local"
        local_dir.mkdir()
        monkeypatch.setattr("check_cricsheet_updates.LOCAL_DATA_DIR", local_dir)
        
        # Create mock zip
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            zf.writestr("1000851.json", '{"match": "new"}')
        zip_buffer.seek(0)
        
        mock_response = Mock()
        mock_response.content = zip_buffer.read()
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Run
        with patch('sys.argv', ['check_cricsheet_updates.py', '--download']):
            main()
        
        # Verify file was downloaded
        assert (local_dir / "1000851.json").exists()
        assert (local_dir / "1000851.json").read_text() == '{"match": "new"}'
        
        captured = capsys.readouterr()
        assert "Extracted" in captured.out
