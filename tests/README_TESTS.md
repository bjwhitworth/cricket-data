# Test Suite Documentation

## Overview

This directory contains unit and integration tests for the cricket data pipeline project.

## Test Files

### `test_check_cricsheet_updates.py`

Comprehensive test suite for the Cricsheet data update checker script. Covers 97% of the code with 22 tests.

**Test Categories:**

1. **TestGetLocalFiles** (3 tests)
   - Tests directory creation and file discovery
   - Handles missing directories, empty directories, and populated directories
   - Validates JSON file filtering

2. **TestGetCricsheetFiles** (5 tests)
   - Tests zip file downloading and parsing
   - Validates file filtering (__MACOSX exclusion, JSON-only)
   - Tests error handling (network errors, bad zip files)

3. **TestExtractFiles** (3 tests)
   - Tests file extraction from zip archives
   - Validates path flattening for nested files
   - Tests graceful error handling during extraction

4. **TestMain** (9 tests)
   - Tests command-line interface and argument parsing
   - Validates check-only mode, download mode, limit flag
   - Tests summary displays, status messages, and edge cases
   - Validates error handling for empty Cricsheet responses

5. **TestIntegration** (2 tests)
   - End-to-end tests combining multiple components
   - Tests full check and download workflows with mocked HTTP

## Running Tests

### Run all tests:
```bash
python -m pytest tests/test_check_cricsheet_updates.py -v
```

### Run with coverage:
```bash
python -m pytest tests/test_check_cricsheet_updates.py --cov=check_cricsheet_updates --cov-report=term-missing
```

### Run specific test class:
```bash
python -m pytest tests/test_check_cricsheet_updates.py::TestGetLocalFiles -v
```

### Run specific test:
```bash
python -m pytest tests/test_check_cricsheet_updates.py::TestGetLocalFiles::test_returns_json_files_from_directory -v
```

## Test Coverage

Current coverage: **97%**

Uncovered lines:
- Lines 109-110: OSError exception handling in directory cleanup (edge case)
- Line 218: `if __name__ == '__main__'` entry point (not executed during imports)

## Test Strategy

- **Mocking**: HTTP requests are mocked to avoid actual network calls
- **Fixtures**: Uses pytest's `tmp_path` fixture for isolated file operations
- **Monkeypatching**: Uses `monkeypatch` to override module constants for testing
- **Edge Cases**: Tests handle empty directories, network failures, bad data, etc.
- **Integration**: Includes end-to-end tests that combine multiple functions

## Dependencies

Tests require:
- `pytest>=8.0.0`
- `pytest-cov>=6.0.0`
- `requests>=2.31.0`

All dependencies are listed in `pyproject.toml`.

## Adding New Tests

When adding tests:
1. Group related tests in classes (e.g., `TestFunctionName`)
2. Use descriptive test names that explain what is being tested
3. Include docstrings explaining the test's purpose
4. Mock external dependencies (network, filesystem when appropriate)
5. Run coverage to ensure new code is tested
6. Verify tests pass in isolation and as part of the full suite
