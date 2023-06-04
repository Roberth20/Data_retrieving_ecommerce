from flask import Blueprint

update = Blueprint("update", __name__)

from App.update import routes