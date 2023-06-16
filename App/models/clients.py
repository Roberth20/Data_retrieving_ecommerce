from App.extensions.db import db

class clients(db.Model):
    __tablename__ = "clients"
    id = db.Column(db.Integer, primary_key=True, nullable=False)
    name = db.Column(db.String(42), nullable=False)
    mail = db.Column(db.String(64))
    phone = db.Column(db.String(12))
    items = db.Column(db.String(255), nullable=False)