from App.update import update
import pandas as pd
from flask import render_template, request, redirect
from App.models.mapeo_atributos import *
from App.extensions.db import db
from App.update.funcs import *
from App.models.mapeo_categorias import Mapeo_categorias
from flask_security import auth_required

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
            df = pd.read_excel(file)
            db_paris = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            if ~df.columns.isin(db_paris.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload(df, db_paris, db, Mapeo_Paris)
            
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
            df = pd.read_excel(file)
            db_falabella = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Falabella.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            if ~df.columns.isin(db_falabella.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload(df, db_falabella, db, Mapeo_Falabella)
            
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
            df = pd.read_excel(file)
            db_mlc = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_MercadoLibre.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            if ~df.columns.isin(db_mlc.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload(df, db_mlc, db, Mapeo_MercadoLibre)
            
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
            df = pd.read_excel(file)
            db_ripley = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Ripley.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            if ~df.columns.isin(db_ripley.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload(df, db_ripley, db, Mapeo_Ripley)
            
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
            df = pd.read_excel(file)
            db_cat = pd.DataFrame([[m.id, m.Multivende, m.MercadoLibre, m.Falabella,
                       m.Ripley, m.Paris, m.Paris_Familia] for m in Mapeo_categorias.query.all()], 
                      columns=["Id","Multivende", "MercadoLibre", "Falabella", "Ripley",
                              "Paris", "Paris_Familia"])
            if ~df.columns.isin(db_cat.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_Cats(df, db_cat, db, Mapeo_categorias)
            
            return render_template("update/success.html", market = "Mapeo categorias")
    
    return render_template("update/sample.html", market="Mapeo categorias")