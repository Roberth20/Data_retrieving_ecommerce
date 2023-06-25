"""Inicializacion de base de datos.

Crea una instancia de la aplicacion y crea todas las tablas preparadas con SQLAlchemy.
Adicionalmente, ejecuta todos los comandos de creacion en sql para las tablas que no
se construyeron con los Modelos. Ejemplo: Productos_standard.

Agrega el usuario predeterminado que tendra acceso a la aplicacion.
"""
# Import all the funcionalities
from App.extensions.security import user_datastore
from flask_security import hash_password
from flask import current_app
from App.extensions.db import db
import App

# Create app
app = App.create_app(test_config=False)

# Read all sql commands
with open("App/create_tables.sql") as f:
    querys = f.read().split(";")

# Create all tables
with app.app_context():
    db.create_all()
    for query in querys:
        stmt = db.text(query)
        db.session.execute(stmt)
        
    db.session.commit()

# If the user is not in the database, create it
with app.app_context():
    if not user_datastore.find_user(email=current_app.config["EMAIL"]):
        user_datastore.create_user(email=current_app.config["EMAIL"], 
                                   password=hash_password(current_app.config["PASSWORD"]),
                                  username=current_app.config["USERNAME"])
        db.session.commit()