import sqlalchemy.orm as db
from sqlalchemy import Column, Integer, Text, DateTime, String, Float, ForeignKey, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship


# Making the models
class Base(db.DeclarativeBase):
    pass

class auth_app(Base):
    __tablename__ = "auth"
    id = Column(Integer, primary_key=True)
    token = Column(Text, nullable=False)
    expire = Column(DateTime, nullable=False)
    refresh_token = Column(String(39), nullable=False)


class checkouts(Base):
    __tablename__ = "Ventas"
    id = Column(Integer, nullable=False, primary_key=True)
    cantidad = Column(Integer, nullable=False)
    codigo_producto = Column(String(36))
    costo_envio = Column(Float, nullable=False)
    estado_boleta = Column(String(16))
    estado_entrega = Column(String(9), nullable=False)
    estado_venta = Column(String(9), nullable=False)
    fecha = Column(DateTime, nullable=False)
    id_venta = Column(String(36), nullable=False)
    id_hijo_producto = Column(String(36), nullable=False)
    id_padre_producto = Column(String(36), nullable=False)
    mail = Column(String(120))
    market = Column(String(12), nullable=False)
    n_venta = Column(String(24), nullable=False)
    nombre_cliente = Column(String(120), nullable=False)
    nombre_producto = Column(String(120), nullable=False)
    phone = Column(String(15), nullable=True)
    precio = Column(Integer, nullable=False)
    url_boleta = Column(String(160))


class deliverys(Base):
    __tablename__ = "Deliverys"
    id = Column(Integer, nullable=False, primary_key=True)
    n_seguimiento = Column(String(24), nullable=False)
    codigo = Column(String(15), nullable=False)
    codigo_venta = Column(String(15), nullable=False)
    courier = Column(String(24), nullable=False)
    delivery_status = Column(String(9), nullable=False)
    direccion = Column(String(80))
    impresion_etiqueta = Column(String(11), nullable=False)
    fecha_despacho = Column(DateTime, nullable=False)
    fecha_promesa = Column(DateTime)
    id_venta = Column(String(36), nullable=False)
    status_etiqueta = Column(String(5), nullable=False)
    n_venta = Column(String(24))
    

class ids(Base):
    __tablename__ = "ids"
    id = Column(String(36), nullable=False, primary_key=True)
    name = Column(String(250), nullable=False)
    type = Column(String(9), nullable=False)
    
class customs_ids(Base):
    __tablename__ = "customs_ids"
    entry = Column(Integer, nullable=False, primary_key=True)
    id_set = Column(String(36), nullable=False)
    name_set = Column(String(36), nullable=False)
    id = Column(String(36))
    name = Column(String(96))
    option_id = Column(String(36))
    option_name = Column(String(60))

class Attributes(Base):
    __tablename__ = 'attributes'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    text_value: Mapped[str] = mapped_column(Text, nullable=True)
    number_value: Mapped[float] = mapped_column(Float, nullable=True)

class Product(Base):
    __tablename__ = 'products'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    id_padre: Mapped[str] = mapped_column(String(36))
    id_hijo: Mapped[str] = mapped_column(String(36))
    season: Mapped[str] = mapped_column(String(32), nullable=True)
    model: Mapped[str] = mapped_column(String(128), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    htmlDescription: Mapped[str] = mapped_column(Text, nullable=True)
    shortDescription: Mapped[str] = mapped_column(String(255), nullable=True)
    htmlShortDescription: Mapped[str] = mapped_column(String(255), nullable=True)
    warranty: Mapped[str] = mapped_column(String(24), nullable=True)
    brand: Mapped[str] = mapped_column(String(16), nullable=True)
    name: Mapped[str] = mapped_column(String(126), nullable=False)
    productCategory: Mapped[str] = mapped_column(String(180), nullable=True)
    skuName: Mapped[str] = mapped_column(String(16), nullable=True)
    color: Mapped[str] = mapped_column(String(16), nullable=True)
    size: Mapped[str] = mapped_column(String(8), nullable=True)
    sku: Mapped[str] = mapped_column(String(16), nullable=True)
    internalSku: Mapped[str] = mapped_column(String(16), nullable=True)
    width: Mapped[float] = mapped_column(Float, nullable=True)
    length: Mapped[float] = mapped_column(Float, nullable=True)
    height: Mapped[float] = mapped_column(Float, nullable=True)
    weight: Mapped[float] = mapped_column(Float, nullable=True)
    tags: Mapped[str] = mapped_column(String(255), nullable=True)
    picture: Mapped[str] = mapped_column(String(180), nullable=True)
    CBarra01: Mapped[str] = mapped_column(String(24), nullable=True)
    CBarra02: Mapped[str] = mapped_column(String(24), nullable=True)
    CBarra03: Mapped[str] = mapped_column(String(24), nullable=True)
    CBarra04: Mapped[str] = mapped_column(String(24), nullable=True)
    CBarra05: Mapped[str] = mapped_column(String(24), nullable=True)
    CBarra06: Mapped[str] = mapped_column(String(24), nullable=True)


    attributes = relationship('Attributes', secondary = 'association_table')

association_table = Table(
    "association_table",
    Base.metadata,
    Column("id_product", ForeignKey("products.id"), primary_key=True),
    Column("id_atributo", ForeignKey("attributes.id"), primary_key=True),
)