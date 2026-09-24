from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# =====================================================================
# Modèles de l'API TMDB (entrée)
# =====================================================================


class TmdbModel(BaseModel):
    """Base commune : on ignore les champs TMDB qu'on ne stocke pas."""

    model_config = ConfigDict(extra="ignore")


class Genre(TmdbModel):
    id: int
    name: str


class Collection(TmdbModel):
    id: int
    name: str
    poster_path: str | None = None
    backdrop_path: str | None = None


class ProductionCompany(TmdbModel):
    id: int
    name: str
    origin_country: str | None = None
    logo_path: str | None = None

    @field_validator("origin_country", mode="before")
    @classmethod
    def empty_to_none(cls, value: str | None) -> str | None:
        """TMDB renvoie "" quand le pays est inconnu : on le convertit en NULL."""
        return value or None


class ProductionCountry(TmdbModel):
    iso_3166_1: str
    name: str


class SpokenLanguage(TmdbModel):
    iso_639_1: str
    name: str | None = None
    english_name: str | None = None


class Person(TmdbModel):
    """Champs communs aux membres du casting et de l'équipe technique."""

    id: int
    credit_id: str
    name: str
    original_name: str | None = None
    gender: int | None = None
    known_for_department: str | None = None
    popularity: float | None = None
    profile_path: str | None = None


class CastMember(Person):
    character: str | None = None
    order: int | None = None


class CrewMember(Person):
    department: str | None = None
    job: str | None = None


class Credits(TmdbModel):
    cast: list[CastMember] = Field(default_factory=list)
    crew: list[CrewMember] = Field(default_factory=list)


class MovieSummary(TmdbModel):
    """Film tel que renvoyé par les listes (/movie/popular, /search/movie...)."""

    id: int
    title: str
    original_title: str | None = None
    release_date: date | None = None
    vote_average: float | None = None
    popularity: float | None = None

    @field_validator("release_date", mode="before")
    @classmethod
    def empty_date_to_none(cls, value: str | None) -> str | None:
        """TMDB renvoie "" pour une date inconnue, Pydantic attend None."""
        return value or None


class MoviePage(TmdbModel):
    """Page de résultats paginée renvoyée par TMDB."""

    page: int
    total_pages: int
    total_results: int
    results: list[MovieSummary]


class MovieDetails(MovieSummary):
    """Film complet (/movie/{id}?append_to_response=credits)."""

    imdb_id: str | None = None
    original_language: str | None = None
    overview: str | None = None
    tagline: str | None = None
    status: str | None = None
    runtime: int | None = None
    budget: int | None = None
    revenue: int | None = None
    vote_count: int | None = None
    adult: bool = False
    homepage: str | None = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    belongs_to_collection: Collection | None = None
    genres: list[Genre] = Field(default_factory=list)
    production_companies: list[ProductionCompany] = Field(default_factory=list)
    production_countries: list[ProductionCountry] = Field(default_factory=list)
    spoken_languages: list[SpokenLanguage] = Field(default_factory=list)
    credits: Credits = Field(default_factory=Credits)


# =====================================================================
# Modèles de lecture en base (sortie)
# =====================================================================


class MovieRow(BaseModel):
    """Ligne résumée d'un film stocké en base."""

    id: int
    title: str
    release_date: date | None
    vote_average: Decimal | None
    runtime: int | None
    genres: str | None


class MovieDetailRow(BaseModel):
    """Fiche complète d'un film stocké en base."""

    id: int
    title: str
    original_title: str | None
    tagline: str | None
    overview: str | None
    release_date: date | None
    runtime: int | None
    budget: int | None
    revenue: int | None
    vote_average: Decimal | None
    vote_count: int | None
    collection: str | None
    genres: str | None
    companies: str | None
    countries: str | None
    languages: str | None
    directors: str | None
    fetched_at: datetime


class CastRow(BaseModel):
    """Acteur d'un film avec son rôle."""

    name: str
    character: str | None
    cast_order: int | None


class TableCount(BaseModel):
    """Nombre de lignes d'une table."""

    table_name: str
    total: int


class GenreStat(BaseModel):
    """Statistiques agrégées par genre."""

    name: str
    movies: int
    avg_vote: Decimal | None
