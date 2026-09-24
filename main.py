"""Point d'entrée : `python main.py` (équivalent à `python -m tmdb_app`)."""

from tmdb_app.menu import run

if __name__ == "__main__":
    try:
        run()
    except (KeyboardInterrupt, EOFError):
        print("\nAu revoir.")
