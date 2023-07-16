from App.auth import auth
from flask_security import auth_required
from flask import current_app, request, render_template
from App.models.auth import auth_app
import json
from App.extensions.db import db
from App.auth.funcs import encrypt
import requests

@auth.route("/", methods=["GET", "POST"])
@auth_required("basic")
def main_auth():
    if request.method == "POST":
        code = request.form['code']
        if "ac" != code[:2]:
            return render_template("auth/error.html", message="Identificacion de token no valida.")
        if len(code.split("-")) != 6:
            return render_template("auth/error.html", message="Formato de token no valido.")
        
        # Definimos url de acceso
        url = "https://app.multivende.com/oauth/access-token"
        # Creamos la informacion a enviar
        payload = {
            "client_id": current_app.config["CLIENT_ID"],
            "client_secret": current_app.config["CLIENT_SECRET"],
            "grant_type": "authorization_code",
            "code": code
        }
        # Definimos los headers del POST
        headers = {
        'cache-control': 'no-cache',
        'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        try:
            # Guardamos la informacion requerida y logueamos
            token = response.json()["token"]
            expiresAt = response.json()["expiresAt"]
            encrypted = encrypt(token, current_app.config["SECRET_KEY"])
            authentication = auth_app(token = encrypted, expire=expiresAt)
            db.session.add(authentication)
            db.session.commit()
            return render_template("auth/success.html")
        except Exception as e:
            current_app.logger.error("Error en la autenticacion")
            return render_template("auth/error.html", message=str(e)+response.text)
    
    return render_template("auth/main.html")

@auth.route("/auto", methods=["GET", "POST"])
@auth_required("basic")
def auto_auth():
    return