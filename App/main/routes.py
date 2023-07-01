from App.main import bp
from flask import render_template, current_app
from flask_security import auth_required
from flask import request, redirect
from App.models.auth import auth_app
from App.extensions.db import db
from datetime import datetime
from App.auth.funcs import decrypt
import requests

@bp.route("/")
def index():
    return render_template("index.html")

@bp.route("/main")
@auth_required("basic")
def main():
    return render_template("main.html")

@bp.route("/authorize")
@auth_required("basic")
def authorization():
    return redirect("https://app.multivende.com/apps/authorize?response_type=code&client_id=114858424934&redirect_uri=http://data-retrieving-ecommerce-dev.us-east-2.elasticbeanstalk.com/main")

@bp.route("/webhook", methods=["POST"])
def webhook():
    if request.method == "POST":
        raw = request.get_json()
        print("Notificacion recibida: ", raw)
        return "Webhook received"
    
@bp.route("/test")
@auth_required("basic")
def test():
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    # Check if exists token
    if last_auth == None:
        return render_template("update/token_error.html")
    diff = datetime.utcnow() - last_auth.expire
    # The token expired
    if diff.total_seconds()/3600 > 6:
        return render_template("update/token_error.html")
    
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    headers = {
            'Authorization': f'Bearer {token}'
    }
    url = f"https://app.multivende.com/api/d/info"
    response = requests.request("GET", url=url, headers=headers)
    return response.text