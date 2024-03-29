from App.extensions.db import db

class auth_app(db.Model):
    __tablename__ = "auth"
    id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.Text, nullable=False)
    expire = db.Column(db.DateTime, nullable=False)
    refresh_token = db.Column(db.String(39), nullable=False)
    