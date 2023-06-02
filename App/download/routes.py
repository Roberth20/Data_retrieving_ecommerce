from flask import render_template, Response
from App.download import download
import pandas as pd
from App.models.mapeo_atributos import * 
from App.models.mapeo_categorias import Mapeo_categorias
import io

@download.get("/")
def index():
    return render_template("download/main.html")

@download.get("/products")
def download_products():
    return  "<h1>Under construction</h1>"    


############################################################################################
@download.get("/maps")
def download_maps():
    return  render_template("download/mapeo_atributos.html")

@download.get("/maps/paris")
def download_paris():
    df = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
                      columns=["Mapeo", "Atributo"])
    
    buffer = io.BytesIO()
    df.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Mapeo atributos Paris.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

@download.get("/maps/falabella")
def download_falabella():
    df = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Falabella.query.all()], 
                      columns=["Mapeo", "Atributo"])
    
    buffer = io.BytesIO()
    df.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Mapeo atributos Falabella.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

@download.get("/maps/mercadolibre")
def download_mercadolibre():
    df = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_MercadoLibre.query.all()], 
                      columns=["Mapeo", "Atributo"])
    
    buffer = io.BytesIO()
    df.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Mapeo atributos Mercado Libre.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

@download.get("/maps/ripley")
def download_ripley():
    df = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Ripley.query.all()], 
                      columns=["Mapeo", "Atributo"])
    
    buffer = io.BytesIO()
    df.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Mapeo atributos Ripley.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)
    
    
###############################################################################################
@download.get("/map_cat")
def download_map_cat():
    df = pd.DataFrame([[m.Multivende, m.MercadoLibre, m.Falabella,
                       m.Ripley, m.Paris, m.Paris_Familia] for m in Mapeo_categorias.query.all()], 
                      columns=["Multivende", "MercadoLibre", "Falabella", "Ripley",
                              "Paris", "Paris_Familia"])
    
    buffer = io.BytesIO()
    df.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Mapeo categorias.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)