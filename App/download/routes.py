from flask import render_template, Response
from App.download import download
import pandas as pd
from App.models.mapeo_atributos import * 
from App.models.mapeo_categorias import Mapeo_categorias
from App.models.productos import get_products
from App.models.atributos_market import * 
from App.models.clients import clients
from App.download.help_func import col_color, missing_info
from flask_security import auth_required
import io

@download.get("/")
@auth_required("basic")
def index():
    return render_template("download/main.html")

@download.get("/products")
@auth_required("basic")
def download_products():
    products = get_products()
    # Cargamos los mapeos
    map_att =  {"PR": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
                      columns=["Mapeo", "Atributo"]), 
                "RP": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Ripley.query.all()], 
                      columns=["Mapeo", "Atributo"]),
                "MLC": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_MercadoLibre.query.all()], 
                      columns=["Mapeo", "Atributo"]), 
                "FL": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Falabella.query.all()], 
                      columns=["Mapeo", "Atributo"])}
    
    atts = {"MLC": pd.DataFrame([[m.Label, m.AttributeType, m.Category] for m in Atributos_MercadoLibre.query.all()],
                               columns = ["Label", "Value", "Category"]),
           "FL": pd.DataFrame([[m.Label, m.Options, m.Category] for m in Atributos_Falabella.query.all()],
                             columns = ["Label", "Values", "Category"]),
           "RP": pd.DataFrame([[m.Label, m.Category] for m in Atributos_Ripley.query.all()],
                             columns = ["Label", "Category"]),
           "PR": pd.DataFrame([[m.Label, m.Family] for m in Atributos_Paris.query.all()],
                             columns = ["Label", "Category"])}
    
    maps = pd.DataFrame([[m.Multivende, m.MercadoLibre, m.Falabella, m.Ripley, m.Paris, m.Paris_Familia] 
                        for m in Mapeo_categorias.query.all()], 
                        columns = ["Multivende", "MercadoLibre", "Falabella", "Ripley", "Paris", "Paris Familia"])
    std_transformation = pd.DataFrame({
        "Original": products.columns[:20],
        "Nuevo": ["Temporada", "Modelo", "Descripción", "Descripción html", "Descripción corta",
                  "Descripción corta html", "Garantía", "Marca", "Nombre", "Categoría de producto",
                 "Nombre Sku", "Color", "Tamaño", "SKU", "SKU interno", "Ancho", "Largo",
                 "Alto", "Peso", "tags"]
    })
    std_transformation.loc[len(std_transformation), :] = ["size", "Talla"]
    
    # Preparamos el buffer
    buffer = io.BytesIO()
    # Generamos la tabla de datos con los colores y filtros adecuados
    #
    # La funcion missing_info dedicada a la logica del color puede sser consultada
    # en el documento func.py
    products.style.applymap_index(col_color, axis=1)\
            .apply(missing_info, maps=maps, atts = atts, map_att=map_att, std_transformation=std_transformation, axis=1)\
            .to_excel(buffer, index=False)
    headers = {
        'Content-Disposition': 'attachment; filename=output.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)


############################################################################################
@download.get("/maps")
@auth_required("basic")
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
@auth_required("basic")
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
@auth_required("basic")
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
@auth_required("basic")
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
@auth_required("basic")
def download_map_cat():
    df = pd.DataFrame([[m.Multivende, m.MercadoLibre, m.Falabella,
                       m.Ripley, m.Paris, m.Paris_Familia] for m in Mapeo_categorias.query.all()], 
                      columns=['Categoria Multivende', 'Categoria Mercadolibre', 'Categoria Falabella',
       'Categoria Ripley ', 'Categoria Paris', 'Paris Familia'])
    
    buffer = io.BytesIO()
    df.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Mapeo categorias.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

###################################################################################################
@download.get("/customs_attributes")
@auth_required("basic")
def download_customs_attribute():
    from App.models.ids import customs_ids
    
    customs_data = pd.DataFrame([[c.id_set, c.name_set, c.id, c.name, c.option_id,
                                 c.option_name] for c in customs_ids.query.all()],
                               columns = ["id_set", "name_set", "id", "name", "option_id",
                                         "option_name"])
    buffer = io.BytesIO()
    customs_data.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Atributos customs.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

#####################################################################################################
@download.get("/clients")
@auth_required("basic")
def download_clients():
    client_data = pd.DataFrame([[c.name, c.mail, c.phone, c.items] for c in clients.query.all()],
                              columns = ["Nombre", "Correo", "Telefono", "Items"])

    buffer = io.BytesIO()
    client_data.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=clientes.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)