from App.main import bp
from flask import render_template
from flask_security import auth_required

@bp.route("/")
@auth_required("basic")
def index():
    return render_template("index.html")