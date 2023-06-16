from flask import Blueprint

cupdate = Blueprint("create_update", __name__)

from App.create_update import routes