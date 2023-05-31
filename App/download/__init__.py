from flask import Blueprint

download = Blueprint("download", __name__)

from App.download import routes