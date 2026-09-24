from typing import Any, Literal, Self

import httpx

from tmdb_app.models import MovieDetails, MoviePage

BASE_URL = "https://api.themoviedb.org/3"

MovieList = Literal["popular", "top_rated"]


class TmdbError(RuntimeError):
    """Erreur renvoyée par l'API TMDB"""


class TmdbClient:
    """Encapsule les appels à TMDB et valide les réponses avec Pydantic."""

    def __init__(self, token: str, language: str = "fr-FR") -> None:
        """Prépare un client HTTP authentifié avec le Read Access Token."""
        if not token:
            raise TmdbError("TMDB_READ_ACCESS_TOKEN est vide : renseigne-le dans .env")
        self._http = httpx.Client(
            base_url=BASE_URL,
            headers={"Authorization": f"Bearer {token}", "accept": "application/json"},
            params={"language": language},
            timeout=15.0,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Ferme la connexion HTTP."""
        self._http.close()

    def _get(self, path: str, **params: Any) -> dict[str, Any]:
        """Effectue un GET et renvoie le JSON, ou lève TmdbError en cas d'échec."""
        response = self._http.get(path, params=params)
        if response.is_error:
            try:
                message = response.json().get("status_message", response.text)
            except ValueError:
                message = response.text
            raise TmdbError(f"{response.status_code} sur {path} : {message}")
        return response.json()

    def list_movies(self, kind: MovieList, page: int = 1) -> MoviePage:
        """Récupère une page de films populaires ou les mieux notés."""
        return MoviePage.model_validate(self._get(f"/movie/{kind}", page=page))

    def search_movies(self, query: str, page: int = 1) -> MoviePage:
        """Recherche des films par titre."""
        return MoviePage.model_validate(
            self._get("/search/movie", query=query, page=page, include_adult=False)
        )

    def get_movie(self, movie_id: int) -> MovieDetails:
        """Récupère le détail d'un film avec son casting et son équipe en un seul appel."""
        data = self._get(f"/movie/{movie_id}", append_to_response="credits")
        return MovieDetails.model_validate(data)
