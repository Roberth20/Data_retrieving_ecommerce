from App.extensions.db import db

class ids(db.Model):
    __tablename__ = "ids"
    id = db.Column(db.String(36), nullable=False, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    type = db.Column(db.String(9), nullable=False)