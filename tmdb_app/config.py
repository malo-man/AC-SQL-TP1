import os

from dotenv import load_dotenv
from pydantic import BaseModel, SecretStr


class Settings(BaseModel):
    postgres_user: str
    postgres_password: SecretStr
    postgres_db: str
    postgres_host: str
    postgres_port: int
    tmdb_token: SecretStr
    tmdb_language: str

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password.get_secret_value()}"
        )


def load_settings() -> Settings:
    """Lit le fichier .env (s'il existe) puis les variables d'environnement."""
    load_dotenv()
    return Settings(
        postgres_user=os.getenv("POSTGRES_USER", "tmdb"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "tmdb"),
        postgres_db=os.getenv("POSTGRES_DB", "tmdb"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5433")),
        tmdb_token=os.getenv("TMDB_READ_ACCESS_TOKEN", ""),
        tmdb_language=os.getenv("TMDB_LANGUAGE", "fr-FR"),
    )
