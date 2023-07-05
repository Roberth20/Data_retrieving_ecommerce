"""Inicializacion de aplicacion."""
from App import create_full_app

app = create_full_app(test_config=False)

if __name__ == "__main__":
    app.run()