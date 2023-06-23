from App.extensions.security import user_datastore
from flask_security import hash_password
from flask import current_app
from App.extensions.db import db
import App

app = App.create_app()

with open("App/create_tables.sql") as f:
    querys = f.read().split(";")

with app.app_context():
    db.create_all()
    for query in querys:
        stmt = db.text(query)
        db.session.execute(stmt)
    db.session.commit()

with app.app_context():
    if not user_datastore.find_user(email="robmago080617@gmail.com"):
        user_datastore.create_user(email=current_app.config["EMAIL"], 
                                   password=hash_password(current_app.config["PASSWORD"]),
                                  username=current_app.config["USERNAME"])
        db.session.commit()