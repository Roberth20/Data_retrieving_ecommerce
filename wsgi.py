"""Inicializacion de aplicacion."""
from App import create_app

app = create_app(test_config=False)

if __name__ == "__main__":
    app.run()