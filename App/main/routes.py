from App.main import bp
from flask import render_template
from flask_security import auth_required
from flask import request, redirect

@bp.route("/")
def index():
    return render_template("index.html")

@bp.route("/main")
@auth_required("basic")
def main():
    return render_template("main.html")

#@bp.route("/authorize")
#@auth_required("basic")
#def authorization():
 #   return redirect("https://app.multivende.com/apps/authorize?response_type=code&client_id=114858424934&redirect_uri=http://data-retrieving-ecommerce-dev.us-east-2.elasticbeanstalk.com/auth")

@bp.route("/webhook", methods=["POST"])
def webhook():
    if request.method == "POST":
        raw = request.get_json()
        print("Notificacion recibida: ", raw)
        return "Webhook received"