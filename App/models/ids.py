from App.extensions.db import db

class ids(db.Model):
    __tablename__ = "ids"
    id = db.Column(db.String(36), nullable=False, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    type = db.Column(db.String(9), nullable=False)
    
class customs_ids(db.Model):
    __tablename__ = "customs_ids"
    entry = db.Column(db.Integer, nullable=False, primary_key=True)
    id_set = db.Column(db.String(36), nullable=False)
    name_set = db.Column(db.String(36), nullable=False)
    id = db.Column(db.String(36))
    name = db.Column(db.String(96))
    option_id = db.Column(db.String(36))
    option_name = db.Column(db.String(60))
    