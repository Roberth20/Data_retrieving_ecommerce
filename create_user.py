from App.extensions.security import user_datastore
from flask_security import hash_password
from flask import current_app
from App.extensions.db import db
import App

app = App.create_app()

with app.app_context():
    user_datastore.create_user(email=current_app.config["EMAIL"], 
                               password=hash_password(current_app.config["PASSWORD"]),
                              username=current_app.config["USERNAME"])
    db.session.commit()