from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    api_base_url: str = Field("https://hub.juheapi.com", env="API_BASE_URL")
    api_key: str = Field(..., env="API_KEY")
    api_secret: str = Field("", env="API_SECRET")
    db_path: str = Field("data/pipeline.duckdb", env="DB_PATH")
    landing_dir: str = Field("data/landing", env="LANDING_DIR")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
