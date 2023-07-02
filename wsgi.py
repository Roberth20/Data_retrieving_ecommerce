"""Inicializacion de aplicacion."""
from App import create_app

app = create_app(test_config=True)

if __name__ == "__main__":
    app.run()