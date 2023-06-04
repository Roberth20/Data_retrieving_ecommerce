from App.update import update
import pandas as pd
from flask import render_template, request, redirect
from App.models.mapeo_atributos import *
from App.db import db
from App.update.funcs import check_differences_and_upload

ALLOWED_EXTENSIONS = ["xlsx"]

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@update.get("/")
def update_main():
    return render_template("update/main.html")

@update.route("/maps/paris", methods=["GET", "POST"])
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
            db_paris = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
                      columns=["Mapeo", "Atributo"])
            if df.columns not in db_paris.columns:
                # TODO: ERROR
                return "ERROR"
            
            check_differences_and_upload(df, db_paris, db, Mapeo_Paris)
            
            return "Good"
    
    return render_template("update/sample.html", market="Paris")