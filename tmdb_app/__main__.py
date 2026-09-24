from tmdb_app.menu import run

if __name__ == "__main__":
    try:
        run()
    except (KeyboardInterrupt, EOFError):
        print("\nAu revoir.")
