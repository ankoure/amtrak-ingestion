# Tests

This directory contains the test suite for the Amtrak Ingestion project.

## Structure

- `conftest.py` - Shared pytest fixtures and configuration
- `test_app.py` - Integration tests for Chalice API endpoints
- `test_constants.py` - Unit tests for constants and enums
- `test_transform.py` - Unit tests for data transformation functions
- `test_utils.py` - Unit tests for utility functions

## Running Tests

### Install test dependencies

```bash
uv sync --extra dev
```

### Run all tests

```bash
uv run pytest
```

### Run specific test file

```bash
uv run pytest tests/test_app.py
```

### Run with coverage

If you want to run tests with coverage, use pytest-cov:

```bash
uv run pytest --cov=amtraker_ingestion --cov-report=term-missing --cov-report=html
```

Then open `htmlcov/index.html` in your browser to view the coverage report.

**Note:** Coverage reporting requires pytest-cov to be properly installed.

### Run only unit tests

```bash
uv run pytest -m unit
```

### Run only integration tests

```bash
uv run pytest -m integration
```

### Run tests in verbose mode

```bash
uv run spytest -v
```

## Test Markers

Tests are organized with the following markers:

- `@pytest.mark.unit` - Fast unit tests that don't require external dependencies
- `@pytest.mark.integration` - Integration tests that test multiple components together
- `@pytest.mark.slow` - Slow running tests (useful for CI/CD pipeline optimization)

## Writing New Tests

1. Create test files with the naming pattern `test_*.py`
2. Use descriptive test class names: `Test<ComponentName>`
3. Use descriptive test function names: `test_<what_it_does>`
4. Add appropriate markers (`@pytest.mark.unit`, `@pytest.mark.integration`, etc.)
5. Use fixtures from `conftest.py` for common setup
6. Mock external dependencies (S3, APIs, etc.) to keep tests fast and reliable

## Fixtures

Common fixtures available in all tests:

- `mock_s3_client` - Mocked boto3 S3 client
- `temp_dir` - Temporary directory for file operations
- `mock_gtfs_dir` - Mock GTFS directory with sample files
- `sample_train_data` - Sample Amtraker API response data
- `mock_env_vars` - Mock environment variables
- `chalice_client` - Chalice test client for API testing

## CI/CD

Tests run automatically on pull requests. Ensure all tests pass before merging.
