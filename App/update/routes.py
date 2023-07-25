"""Modulo con los endpoints relacionados a la actualizacion de los datos"""

from App.update import update
import pandas as pd
from flask import render_template, request, redirect
from App.models.mapeo_atributos import *
from App.extensions.db import db
from App.extensions.celery import celery
from App.update.funcs import *
from App.models.mapeo_categorias import Mapeo_categorias
from flask_security import auth_required
from App.models.auth import auth_app
from App.auth.funcs import decrypt
from flask import current_app
from datetime import datetime, timedelta
from App.models.clients import clients
from App.models.checkouts import checkouts, deliverys
import requests
from App.models.productos import get_products
from App.get_data.Populate_tables import upload_data_products
import numpy as np

ALLOWED_EXTENSIONS = ["xlsx"]

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@update.get("/")
@auth_required("basic")
def update_main():
    return render_template("update/main.html")

@update.route("/paris", methods=["GET", "POST"])
@auth_required("basic")
def update_paris():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            # Load data
            df = pd.read_excel(file)
            db_paris = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            # Check if data is valid
            if ~df.columns.isin(db_paris.columns).all():
                return render_template("update/error.html")

            check_differences_and_upload_maps(df, db_paris, db, Mapeo_Paris)
            
            return render_template("update/success.html", market = "Paris")
    
    return render_template("update/sample.html", market="Paris")

@update.route("/falabella", methods=["GET", "POST"])
@auth_required("basic")
def update_falabella():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            # Load data
            df = pd.read_excel(file)
            db_falabella = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Falabella.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            # Check if data is valid
            if ~df.columns.isin(db_falabella.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_maps(df, db_falabella, db, Mapeo_Falabella)
            
            return render_template("update/success.html", market = "Falabella")
    
    return render_template("update/sample.html", market="Falabella")

@update.route("/mercadolibre", methods=["GET", "POST"])
@auth_required("basic")
def update_mercadolibre():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            # Load data
            df = pd.read_excel(file)
            db_mlc = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_MercadoLibre.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            # Check if data is valid
            if ~df.columns.isin(db_mlc.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_maps(df, db_mlc, db, Mapeo_MercadoLibre)
            
            return render_template("update/success.html", market = "Mercado Libre")
    
    return render_template("update/sample.html", market="Mercado Libre")

@update.route("/ripley", methods=["GET", "POST"])
@auth_required("basic")
def update_ripley():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            # Load data
            df = pd.read_excel(file)
            db_ripley = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Ripley.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            # Check if data is valid
            if ~df.columns.isin(db_ripley.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_maps(df, db_ripley, db, Mapeo_Ripley)
            
            return render_template("update/success.html", market = "Ripley")
    
    return render_template("update/sample.html", market="Ripley")

@update.route("/map_cat", methods=["GET", "POST"])
@auth_required("basic")
def update_mapcat():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            # Load data
            df = pd.read_excel(file)
            db_cat = pd.DataFrame([[m.id, m.Multivende, m.MercadoLibre, m.Falabella,
                       m.Ripley, m.Paris, m.Paris_Familia] for m in Mapeo_categorias.query.all()], 
                      columns=["Id",'Categoria Multivende', 'Categoria Mercadolibre', 'Categoria Falabella',
       'Categoria Ripley ', 'Categoria Paris', 'Paris Familia'])
            # Check if data is valid
            if ~df.columns.isin(db_cat.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_cats(df, db_cat, db, Mapeo_categorias)
            
            return render_template("update/success.html", market = "Mapeo categorias")
    
    return render_template("update/sample.html", market="Mapeo categorias")

@update.get("/products")
@auth_required("basic")
def update_products():
    # Get last token
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    # Check if exists token
    if last_auth == None:
        return render_template("update/token_error.html")
    diff = datetime.utcnow() - last_auth.expire
    # The token expired
    if diff.total_seconds()/3600 > 6:
        return render_template("update/token_error.html")
    
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    
    current_app.logger.debug("Sending to worker task: update_products")
    celery.send_task("App.task.long_task.update_products", [token, current_app.config["MERCHANT_ID"]])
        
    return render_template("update/products_updated.html")

@update.route("products_file", methods=["GET", "POST"])
@auth_required("basic")
def update_products_from_file():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            # Load data
            df = pd.read_excel(file)
            db_products = get_products()
            mask = ~df.columns.isin(["IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"])
            # Check if data is valid
            if ~df.columns[mask].isin(db_products.columns).all():
                return render_template("update/error.html")
            
            # Delete old data
            stmt = db.text("DELETE FROM Productos_standard")
            db.session.execute(stmt)
            stmt = db.text("DELETE FROM Productos_MercadoLibre")
            db.session.execute(stmt)
            stmt = db.text("DELETE FROM Productos_Paris")
            db.session.execute(stmt)
            stmt = db.text("DELETE FROM Productos_Falabella")
            db.session.execute(stmt)
            stmt = db.text("DELETE FROM Productos_Ripley")
            db.session.execute(stmt)
            stmt = db.text("DELETE FROM Renombre_categorias")
            db.session.execute(stmt)
            db.session.commit()

            # Replace with new data
            message = upload_data_products(df, db)
            
            return render_template("update/success.html", market = "Productos")
    
    return render_template("update/sample.html", market="Productos")

    

@update.get("/clients")
@auth_required("basic")
def clients_data():
    # Retrieve data from the available marketplaces
    fl_customers = get_data_falabella(current_app.config["FALABELLA_USER"], current_app.config["FALABELLA_API_KEY"])
    if type(fl_customers) == str:
        return fl_customers
    pr_customers = get_data_paris(current_app.config["PARIS_API_KEY"])
    if type(pr_customers) == str:
        return pr_customers
    rp_customers = get_data_ripley(current_app.config["RIPLEY_API_KEY"])
    if type(rp_customers) == str:
        return rp_customers
    
    data = pd.concat([pr_customers, fl_customers, rp_customers], axis=0)
    data.reset_index(drop=True, inplace=True)
    # Check if already exists and add to DB
    for i, row in data.iterrows():
        customer = clients(id = i, name=row["Name"], mail=row["Mail"], phone=row["Phone"], items=row["Items"])
        c = db.session.get(clients, i)
        if c == None:
            db.session.add(customer)
        elif c.name == customer.name:
            continue
        else:
            c = customer
    db.session.commit()
    
    return render_template("update/success_client.html")

@update.route("/checkouts", methods=["GET"])
@auth_required("basic")
def update_checkouts():
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    
    # Check if token exists
    if last_auth == None:
        return render_template("update/token_error.html")
    diff = datetime.utcnow() - last_auth.expire
    # Check is token expired
    if diff.total_seconds()/3600 > 6:
        return render_template("update/token_error.html")
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    
    # Last time updated
    result = db.session.scalar(db.select(checkouts).order_by(checkouts.fecha.desc()))
    last_update = result.fecha - timedelta(days=28) # One month before to update changes of recents sells
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    last = last_update.strftime("%Y-%m-%dT%H:%M:%S")
    
    current_app.logger.debug("Sending to worker task: update_checkouts")
    celery.send_task("App.task.long_task.update_checkouts", [token, current_app.config["MERCHANT_ID"], last, now])
    
    return render_template("update/checkouts.html")

@update.get("/deliverys")
@auth_required("basic")
def update_ventas():
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    # Check if token exists
    if last_auth == None:
        return render_template("update/token_error.html")
    diff = datetime.utcnow() - last_auth.expire
    # Check is token expired
    if diff.total_seconds()/3600 > 6:
        return render_template("update/token_error.html")
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    
    # Last time updated
    result = db.session.scalar(db.select(deliverys).order_by(deliverys.fecha_despacho.desc()))
    last_update = result.fecha_despacho - timedelta(days=28) # One month before to update changes of recents sells
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    last = last_update.strftime("%Y-%m-%dT%H:%M:%S")
    
    current_app.logger.debug("Sending to worker task: update_deliverys")
    celery.send_task("App.task.long_task.update_deliverys", [token, current_app.config["MERCHANT_ID"], last, now])
    
    return render_template("update/delivery.html")


@update.route("/ids", methods=["GET"])
@auth_required("basic")
def update_ids():    
    current_app.logger.debug("Getting token")
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    # Check if token exists
    if last_auth == None:
        return render_template("update/token_error.html")
    diff = datetime.utcnow() - last_auth.expire
    # Check is token expired
    if diff.total_seconds()/3600 > 6:
        return render_template("update/token_error.html")
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    
    current_app.logger.debug("Sending to worker task: upload_ids")
    celery.send_task("App.task.long_task.upload_ids", [token, current_app.config["MERCHANT_ID"], "ids"])
    
    return render_template("update/success-ids.html", message="marcas, tallas, categorias, colores, tags y garantias")
    
@update.route("/custom_ids", methods=["GET"])
@auth_required("basic")
def update_custom_ids():
    current_app.logger.debug("Getting token")
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    # Check if token exists
    if last_auth == None:
        return render_template("update/token_error.html")
    diff = datetime.utcnow() - last_auth.expire
    # Check is token expired
    if diff.total_seconds()/3600 > 6:
        return render_template("update/token_error.html")
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    
    current_app.logger.debug("Sending to worker task: upload_ids")
    celery.send_task("App.task.long_task.upload_ids", [token, current_app.config["MERCHANT_ID"], "customs_ids"])
    
    return render_template("update/success-ids.html", message="Custom attributes")
    
    