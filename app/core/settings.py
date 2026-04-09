from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    APP_TITLE: str = "Jobs API"
    APP_HOST: str = "127.0.0.1"
    APP_PORT: int = 8000
    APP_RELOAD: bool = True

    DB_HOST: str = "localhost"
    DB_PORT: int = 5432

    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str 
    DB_ECHO: bool = False

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_CACHE_TTL: int = 15

    @property
    def database_url_async(self) -> str:
        return (
            f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @property
    def database_url_sync(self) -> str:
        return (
        f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASSWORD}"
        f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}"

settings = Settings()