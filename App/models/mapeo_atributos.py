from App.extensions.db import db

class Mapeo_Paris(db.Model):
    __tablename__ = "Mapeo_Paris"
    id = db.Column(db.Integer, primary_key=True)
    Mapeo = db.Column(db.String(50), nullable=False)
    Atributo = db.Column(db.String(50), nullable=False)
    
    def __repr__(self):
        return f"<Mapeo de atributos Paris para {self.Atributo}>"
    
class Mapeo_Falabella(db.Model):
    __tablename__ = "Mapeo_Falabella"
    id = db.Column(db.Integer, primary_key=True)
    Mapeo = db.Column(db.String(45), nullable=False)
    Atributo = db.Column(db.String(36), nullable=False)
    
    def __repr__(self):
        return f"<Mapeo de atributos Falabella para {self.Atributo}>"
    
class Mapeo_MercadoLibre(db.Model):
    __tablename__ = "Mapeo_MercadoLibre"
    id = db.Column(db.Integer, primary_key=True)
    Mapeo = db.Column(db.String(70), nullable=False)
    Atributo = db.Column(db.String(40), nullable=False)
    
    def __repr__(self):
        return f"<Mapeo de atributos Mercado Libre para {self.Atributo}>"
    
class Mapeo_Ripley(db.Model):
    __tablename__ = "Mapeo_Ripley"
    id = db.Column(db.Integer, primary_key=True)
    Mapeo = db.Column(db.String(45), nullable=False)
    Atributo = db.Column(db.String(40), nullable=False)
    
    def __repr__(self):
        return f"<Mapeo de atributos Ripley para {self.Atributo}>"