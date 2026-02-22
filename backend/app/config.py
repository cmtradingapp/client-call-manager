from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Set MOCK_MODE=true to run without real MSSQL / internal API / ElevenLabs
    mock_mode: bool = False

    # MSSQL — required when mock_mode=False
    mssql_server: str = "localhost"
    mssql_database: str = "clients"
    mssql_username: str = "sa"
    mssql_password: str = ""
    mssql_driver: str = "ODBC Driver 18 for SQL Server"

    # CRM API — used to fetch phone numbers per client
    crm_api_base_url: str = "https://apicrm.cmtrading.com/SignalsCRM"
    crm_api_token: str = ""

    # ElevenLabs — required when mock_mode=False
    elevenlabs_api_key: str = ""
    elevenlabs_agent_id: str = ""
    elevenlabs_from_number: str = "+15550000000"

    # CORS
    cors_origins: List[str] = ["http://localhost:5173", "http://localhost"]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def mssql_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.mssql_driver}}};"
            f"SERVER={self.mssql_server};"
            f"DATABASE={self.mssql_database};"
            f"UID={self.mssql_username};"
            f"PWD={self.mssql_password};"
            f"TrustServerCertificate=yes;"
        )


settings = Settings()
