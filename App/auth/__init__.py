from flask import Blueprint

auth = Blueprint("auth", __name__)

from App.auth import routes