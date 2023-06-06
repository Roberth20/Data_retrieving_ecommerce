from App.auth import auth
from flask_security import auth_required
from flask import current_app

@auth.get("/")
@auth_required("basic")
def main_auth():
    return f"testing: client_id : {current_app.config['CLIENT_ID']}"
