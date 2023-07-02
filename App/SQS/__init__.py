from flask import Blueprint

sqs = Blueprint("sqs", __name__)

from App.SQS import routes