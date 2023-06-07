from App.extensions.db import db

class auth_app(db.Model):
    __tablename__ = "auth"
    token = db.Column(db.Text, nullable=False, primary_key=True)
    expire = db.Column(db.DateTime, nullable=False)