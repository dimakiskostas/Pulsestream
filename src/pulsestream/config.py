from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_path: str = "pulsestream.duckdb"
    queue_maxsize: int = 1000
    model_config = SettingsConfigDict(env_prefix="PULSE_", env_file_encoding="utf-8")

    class Config:
        env_prefix = "PULSE_"


settings = Settings()
