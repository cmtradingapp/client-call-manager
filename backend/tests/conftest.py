import os

# Set required env vars before any app module is imported so
# pydantic-settings validation succeeds during tests.
_test_env = {
    "MSSQL_SERVER": "localhost",
    "MSSQL_DATABASE": "testdb",
    "MSSQL_USERNAME": "testuser",
    "MSSQL_PASSWORD": "testpassword",
    "MSSQL_DRIVER": "ODBC Driver 18 for SQL Server",
    "INTERNAL_API_BASE_URL": "http://internal-api.test",
    "INTERNAL_API_KEY": "test-api-key",
    "ELEVENLABS_API_KEY": "test-elevenlabs-key",
    "ELEVENLABS_AGENT_ID": "test-agent-id",
    "ELEVENLABS_FROM_NUMBER": "+15551234567",
    "CORS_ORIGINS": '["http://localhost:5173"]',
}

for key, value in _test_env.items():
    os.environ.setdefault(key, value)
