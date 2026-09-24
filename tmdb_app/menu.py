from collections.abc import Callable

import psycopg
from psycopg import Connection

from tmdb_app import repository
from tmdb_app.api import MovieList, TmdbClient, TmdbError
from tmdb_app.config import Settings, load_settings
from tmdb_app.models import MovieSummary

Action = Callable[[Connection, Settings], None]


def ask_int(prompt: str, default: int, minimum: int = 1, maximum: int = 500) -> int:
    """Demande un entier borné à l'utilisateur, avec une valeur par défaut."""
    raw = input(f"{prompt} [{default}] : ").strip()
    if not raw:
        return default
    try:
        return max(minimum, min(maximum, int(raw)))
    except ValueError:
        print("Valeur invalide, utilisation de la valeur par défaut.")
        return default


def import_movies(
    conn: Connection, client: TmdbClient, movies: list[MovieSummary]
) -> None:
    """Récupère le détail de chaque film de la liste puis l'enregistre en base."""
    for index, summary in enumerate(movies, start=1):
        try:
            details = client.get_movie(summary.id)
            repository.save_movie(conn, details)
            print(
                f"  [{index}/{len(movies)}] ✔ {details.title} ({details.release_date or '?'})"
            )
        except TmdbError as error:
            print(f"  [{index}/{len(movies)}] ✘ {summary.title} : {error}")


def import_list(conn: Connection, settings: Settings, kind: MovieList) -> None:
    """Importe N pages (20 films/page) d'une liste TMDB : populaires ou mieux notés."""
    pages = ask_int("Nombre de pages à importer (20 films/page)", default=1, maximum=50)
    with TmdbClient(
        settings.tmdb_token.get_secret_value(), settings.tmdb_language
    ) as client:
        for page in range(1, pages + 1):
            result = client.list_movies(kind, page)
            print(f"Page {page}/{min(pages, result.total_pages)}")
            import_movies(conn, client, result.results)
            if page >= result.total_pages:
                break


def action_import_popular(conn: Connection, settings: Settings) -> None:
    """Action du menu : import des films populaires."""
    import_list(conn, settings, "popular")


def action_import_top_rated(conn: Connection, settings: Settings) -> None:
    """Action du menu : import des films les mieux notés."""
    import_list(conn, settings, "top_rated")


def action_search(conn: Connection, settings: Settings) -> None:
    """Action du menu : recherche par titre puis import des films sélectionnés."""
    query = input("Titre à rechercher : ").strip()
    if not query:
        return
    with TmdbClient(
        settings.tmdb_token.get_secret_value(), settings.tmdb_language
    ) as client:
        results = client.search_movies(query).results[:10]
        if not results:
            print("Aucun résultat.")
            return
        for index, movie in enumerate(results, start=1):
            year = movie.release_date.year if movie.release_date else "????"
            print(f"  {index:>2}. {movie.title} ({year}) — id {movie.id}")
        raw = input(
            "Numéros à importer (ex. 1,3), 'all' ou vide pour annuler : "
        ).strip()
        if not raw:
            return
        if raw.lower() == "all":
            selected = results
        else:
            indexes = {int(x) for x in raw.split(",") if x.strip().isdigit()}
            selected = [m for i, m in enumerate(results, start=1) if i in indexes]
        import_movies(conn, client, selected)


def action_list_movies(conn: Connection, settings: Settings) -> None:
    """Action du menu : affiche les films présents en base."""
    limit = ask_int("Nombre de films à afficher", default=20)
    movies = repository.list_movies(conn, limit)
    if not movies:
        print("La base est vide : importe d'abord des films.")
        return
    print(f"{'ID':>8}  {'Sortie':<10}  {'Note':>4}  {'Durée':>5}  Titre — Genres")
    for m in movies:
        release = m.release_date.isoformat() if m.release_date else "?"
        vote = f"{m.vote_average:.1f}" if m.vote_average is not None else "-"
        runtime = f"{m.runtime}m" if m.runtime else "-"
        print(
            f"{m.id:>8}  {release:<10}  {vote:>4}  {runtime:>5}  {m.title} — {m.genres or ''}"
        )


def action_movie_detail(conn: Connection, settings: Settings) -> None:
    """Action du menu : fiche détaillée d'un film en base (relations + casting)."""
    raw = input("ID TMDB ou titre du film : ").strip()
    if not raw:
        return
    if raw.isdigit():
        movie_id = int(raw)
    else:
        matches = repository.find_movies_by_title(conn, raw)
        if not matches:
            print("Aucun film en base ne correspond à ce titre.")
            return
        if len(matches) == 1:
            movie_id = matches[0].id
        else:
            for index, m in enumerate(matches, start=1):
                year = m.release_date.year if m.release_date else "????"
                print(f"  {index:>2}. {m.title} ({year}) — id {m.id}")
            choice = ask_int("Numéro du film", default=1, maximum=len(matches))
            movie_id = matches[choice - 1].id
    detail = repository.get_movie_detail(conn, movie_id)
    if detail is None:
        print("Film introuvable en base.")
        return
    print(f"\n{detail.title}  ({detail.original_title})")
    if detail.tagline:
        print(f"« {detail.tagline} »")
    rows: list[tuple[str, object]] = [
        ("Sortie", detail.release_date),
        ("Durée", f"{detail.runtime} min" if detail.runtime else None),
        ("Note", f"{detail.vote_average} ({detail.vote_count} votes)"),
        ("Budget", f"{detail.budget:,} $" if detail.budget else None),
        ("Recettes", f"{detail.revenue:,} $" if detail.revenue else None),
        ("Saga", detail.collection),
        ("Genres", detail.genres),
        ("Réalisation", detail.directors),
        ("Production", detail.companies),
        ("Pays", detail.countries),
        ("Langues", detail.languages),
        ("Importé le", detail.fetched_at.strftime("%Y-%m-%d %H:%M")),
    ]
    for label, value in rows:
        print(f"  {label:<12}: {value if value is not None else '-'}")
    if detail.overview:
        print(f"\n{detail.overview}")
    cast = repository.get_movie_cast(conn, movie_id)
    if cast:
        print("\nCasting principal :")
        for actor in cast:
            print(f"  - {actor.name} : {actor.character or '?'}")


def action_stats(conn: Connection, settings: Settings) -> None:
    """Action du menu : volumétrie des tables et statistiques par genre."""
    print("Volumétrie :")
    for count in repository.count_tables(conn):
        print(f"  {count.table_name:<28} {count.total:>8}")
    stats = repository.genre_stats(conn)
    if stats:
        print("\nFilms par genre :")
        for stat in stats:
            print(
                f"  {stat.name:<20} {stat.movies:>5} films   note moy. {stat.avg_vote}"
            )


MENU: dict[str, tuple[str, Action]] = {
    "1": ("Importer les films populaires", action_import_popular),
    "2": ("Importer les films les mieux notés", action_import_top_rated),
    "3": ("Rechercher un film par titre et l'importer", action_search),
    "4": ("Lister les films en base", action_list_movies),
    "5": ("Voir la fiche d'un film en base (ID ou titre)", action_movie_detail),
    "6": ("Statistiques de la base", action_stats),
}


def run() -> None:
    """Boucle principale : affiche le menu et exécute l'action choisie."""
    settings = load_settings()
    try:
        conn = psycopg.connect(settings.conninfo, autocommit=True)
    except psycopg.OperationalError as error:
        print(
            f"Connexion à PostgreSQL impossible ({error}).\nLance `docker compose up -d`."
        )
        return

    with conn:
        while True:
            print("\n===== TMDB → PostgreSQL =====")
            for key, (label, _) in MENU.items():
                print(f"  {key}. {label}")
            print("  0. Quitter")
            choice = input("> ").strip()
            if choice in {"0", "q"}:
                break
            entry = MENU.get(choice)
            if entry is None:
                print("Choix inconnu.")
                continue
            try:
                entry[1](conn, settings)
            except TmdbError as error:
                print(f"Erreur TMDB : {error}")
            except psycopg.Error as error:
                print(f"Erreur PostgreSQL : {error}")
            except KeyboardInterrupt:
                print("\nAction interrompue.")
