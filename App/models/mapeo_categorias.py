from App.extensions.db import db

class Mapeo_categorias(db.Model):
    __tablename__ = "Mapeo_Categorias"
    id = db.Column(db.Integer, primary_key=True)
    Multivende = db.Column(db.String(150), nullable=False)
    MercadoLibre = db.Column(db.String(150), nullable=False)
    Falabella = db.Column(db.String(220), nullable=False)
    Ripley = db.Column(db.String(200), nullable=False)
    Paris = db.Column(db.String(120), nullable=False)
    Paris_Familia = db.Column(db.String(30), nullable=False)
    
    def __repr__(self):
        return f"<Mapeo de categoria para {self.Multivende}>"