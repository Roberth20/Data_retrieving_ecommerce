from App.extensions.db import db

class Atributos_Falabella(db.Model):
    __tablename__ = "Atributos_Falabella"
    id = db.Column(db.Integer, primary_key=True)
    Label = db.Column(db.String(256), nullable =False)
    Category = db.Column(db.String(256), nullable=False)
    GroupName = db.Column(db.String(256))
    isMandatory = db.Column(db.Boolean, nullable=False)
    isGlobalAttribute = db.Column(db.Boolean, nullable=False)
    Description = db.Column(db.Text)
    ProductType = db.Column(db.String(64))
    AttributeType = db.Column(db.String(12), nullable=False)
    Options = db.Column(db.Text)
    
    def __repr__(self):
        return f"<Falabella atributo de {self.Category}>"
    
class Atributos_MercadoLibre(db.Model):
    __tablename__ = "Atributos_MercadoLibre"
    id = db.Column(db.Integer, primary_key = True)
    Label = db.Column(db.String(120), nullable=False)
    Category = db.Column(db.String(60), nullable=False)
    Tags = db.Column(db.String(120), nullable=False)
    Hierarchy = db.Column(db.String(20), nullable=False)
    AttributeType = db.Column(db.String(15), nullable=False)
    GroupName = db.Column(db.String(64), nullable=False)
    Options = db.Column(db.Text)
    
    def __repr__(self):
        return f"<Mercado Libre atributo de {self.Category}>"
    
class Atributos_Paris(db.Model):
    __tablename__ = "Atributos_Paris"
    id = db.Column(db.Integer, primary_key = True)
    Label = db.Column(db.String(64), nullable=False)
    Category = db.Column(db.String(40), nullable=False)
    Family = db.Column(db.String(20), nullable=False)
    AttributeType = db.Column(db.String(16), nullable=False)
    GroupName = db.Column(db.String(20))
    isMandatory = db.Column(db.Boolean, nullable=False)
    
    def __repr__(self):
        return f"<Paris atributo de {self.Category}>"
    
class Atributos_Ripley(db.Model):
    __tablename__ = "Atributos_Ripley"
    id = db.Column(db.Integer, primary_key = True)
    Label = db.Column(db.String(64), nullable=False)
    Category = db.Column(db.String(80), nullable=False)
    RequirementLevel = db.Column(db.String(8), nullable=False)
    AttributeType = db.Column(db.String(10), nullable=False)
    Options = db.Column(db.String(120))
    Variant = db.Column(db.Boolean, nullable=False)