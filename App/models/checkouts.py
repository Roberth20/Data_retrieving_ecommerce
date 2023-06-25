from App.extensions.db import db

class checkouts(db.Model):
    __tablename__ = "Ventas"
    id = db.Column(db.Integer, nullable=False, primary_key=True)
    cantidad = db.Column(db.Integer, nullable=False)
    codigo_producto = db.Column(db.String(36))
    costo_envio = db.Column(db.Float, nullable=False)
    estado_boleta = db.Column(db.String(16))
    estado_entrega = db.Column(db.String(9), nullable=False)
    estado_venta = db.Column(db.String(9), nullable=False)
    fecha = db.Column(db.DateTime, nullable=False)
    id_venta = db.Column(db.String(36), nullable=False)
    id_hijo_producto = db.Column(db.String(36), nullable=False)
    id_padre_producto = db.Column(db.String(36), nullable-False)
    mail = db.Column(db.String(120))
    market = db.Column(db.String(12), nullable=False)
    n_venta = db.Column(db.String(16), nullable=False)
    nombre_cliente = db.Column(db.String(120), nullable=False)
    nombre_producto = db.Column(db.String(120), nullable=False)
    phone = db.Column(db.String(15), nullable=False)
    precio = db.Column(db.Integer, nullable=False)
    url_boleta = db.Column(db.String(160))
    
class deliverys(db.Model):
    __tablename__ = "Deliverys"
    id = db.Column(db.Integer, nullable=False, primary_key=True)
    n_seguimiento = db.Column(db.String(10), nullable=False)
    codigo = db.Column(db.String(15), nullable=False)
    codigo_venta = db.Column(db.String(15), nullable=False)
    courier = db.Column(db.String(12), nullable=False)
    delivery_status = db.Column(db.String(9), nullable=False)
    direccion = db.Column(db.String(80))
    impresion_etiqueta = db.Column(db.String(11), nullable=False)
    fecha_despacho = db.Column(db.DateTime, nullable=False)
    fecha_promesa = db.Column(db.DateTime)
    id_venta = db.Column(db.String(36), nullable=False)
    status_etiqueta = db.Column(db.String(5), nullable=False)
    n_venta = db.Column(db.String(15))