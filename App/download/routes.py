"""Modulo de manejo para todas las descargas"""
# Import all the stuff
from flask import render_template, Response, current_app, send_file
from App.download import download
import pandas as pd
from App.models.mapeo_atributos import * 
from App.models.mapeo_categorias import Mapeo_categorias
from App.models.productos import get_products
from App.models.clients import clients
from App.models.checkouts import checkouts, deliverys
from flask_security import auth_required
from App.models.atributos_market import * 
from App.download.help_func import *
from App.download.help_func import col_color, missing_info
import io

@download.get("/")
@auth_required("basic")
def index():
    return render_template("download/main.html")

#@download.get("/products_test")
#def dpt():
 #   current_app.logger.info("Retrieving data of products from DB")
    # Load products
  #  products = get_products()
    
    # Cargamos los mapeos
   # map_att =  {"PR": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
    #                  columns=["Mapeo", "Atributo"]), 
     #           "RP": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Ripley.query.all()], 
      #                columns=["Mapeo", "Atributo"]),
       #         "MLC": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_MercadoLibre.query.all()], 
        #              columns=["Mapeo", "Atributo"]), 
         #       "FL": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Falabella.query.all()], 
          #            columns=["Mapeo", "Atributo"])}
    
#    atts = {"MLC": pd.DataFrame([[m.Label, m.AttributeType, m.Category] for m in Atributos_MercadoLibre.query.all()],
#                               columns = ["Label", "Value", "Category"]),
#           "FL": pd.DataFrame([[m.Label, m.Options, m.Category] for m in Atributos_Falabella.query.all()],
#                             columns = ["Label", "Values", "Category"]),
#           "RP": pd.DataFrame([[m.Label, m.Category] for m in Atributos_Ripley.query.all()],
#                             columns = ["Label", "Category"]),
#           "PR": pd.DataFrame([[m.Label, m.Family] for m in Atributos_Paris.query.all()],
#                             columns = ["Label", "Category"])}
    
#    maps = pd.DataFrame([[m.Multivende, m.MercadoLibre, m.Falabella, m.Ripley, m.Paris, m.Paris_Familia] 
#                        for m in Mapeo_categorias.query.all()], 
#                        columns = ["Multivende", "MercadoLibre", "Falabella", "Ripley", "Paris", "Paris Familia"])
#    std_transformation = pd.DataFrame({
#        "Original": products.columns[:20],
#        "Nuevo": ["Temporada", "Modelo", "Descripción", "Descripción html", "Descripción corta",
#                  "Descripción corta html", "Garantía", "Marca", "Nombre", "Categoría de producto",
#                 "Nombre Sku", "Color", "Tamaño", "SKU", "SKU interno", "Ancho", "Largo",
#                 "Alto", "Peso", "tags"]
#    })
#    std_transformation.loc[len(std_transformation), :] = ["size", "Talla"]
#    products.reset_index(inplace=True)
    
#    buffer = io.BytesIO()
#    
#    products.style.applymap_index(col_color, axis=1)\
#            .apply(missing_info, maps=maps, atts = atts, map_att=map_att, std_transformation=std_transformation, axis=1)\
#            .to_excel("output.xlsx", index=False)
    
    #headers = {
      #  'Content-Disposition': 'attachment; filename=output.xlsx',
     #   'Content-type': 'application/vnd.ms-excel'
    #}
    #return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)
#    return "hello"
    


@download.get("/products")
@auth_required("basic")
def download_products():
    return send_file("output.xlsx")
    

############################################################################################
# Sub-menu to download maps
@download.get("/maps")
@auth_required("basic")
def download_maps():
    return  render_template("download/mapeo_atributos.html")

@download.get("/maps/paris")
def download_paris():
    # Retrieving data
    current_app.logger.info("Retrieving data of Mapeo Paris from DB")
    df = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
                      columns=["Mapeo", "Atributo"])
    
    # Sending data
    current_app.logger.info("Sending data Mapeo Paris")
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
    # Retrieving data
    current_app.logger.info("Retrieving data of Mapeo Falabella from DB")
    df = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Falabella.query.all()], 
                      columns=["Mapeo", "Atributo"])
    
    # Sending data
    current_app.logger.info("Sending data Mapeo Falabella")
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
    # Retrieving data
    current_app.logger.info("Retrieving data of Mapeo MercadoLibre from DB")
    df = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_MercadoLibre.query.all()], 
                      columns=["Mapeo", "Atributo"])
    
    # Sending data
    current_app.logger.info("Sending data Mapeo MercadoLibre")
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
    # Retrieving data
    current_app.logger.info("Retrieving data of Mapeo Ripley from DB")
    df = pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Ripley.query.all()], 
                      columns=["Mapeo", "Atributo"])
    
    # Sending data
    current_app.logger.info("Sending data Mapeo Ripley")
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
    # Retrieving data
    current_app.logger.info("Retrieving data of Mapeo categorias from DB")
    df = pd.DataFrame([[m.Multivende, m.MercadoLibre, m.Falabella,
                       m.Ripley, m.Paris, m.Paris_Familia] for m in Mapeo_categorias.query.all()], 
                      columns=['Categoria Multivende', 'Categoria Mercadolibre', 'Categoria Falabella',
       'Categoria Ripley ', 'Categoria Paris', 'Paris Familia'])
    
    # Sending data
    current_app.logger.info("Sending data Mapeo categorias")
    buffer = io.BytesIO()
    df.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Mapeo categorias.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

###################################################################################################

@download.get("/clients")
@auth_required("basic")
def download_clients():
    # Retrieving data
    current_app.logger.info("Retrieving data of clientes from DB")
    client_data = pd.DataFrame([[c.name, c.mail, c.phone, c.items] for c in clients.query.all()],
                              columns = ["Nombre", "Correo", "Telefono", "Items"])

    # Sending data
    current_app.logger.info("Sending data clientes")
    buffer = io.BytesIO()
    client_data.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=clientes.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

@download.get("/ventas")
def download_checkouts():
    # Retrieving data
    current_app.logger.info("Retrieving data of checkouts from DB")
    checkout_data = pd.DataFrame([[c.cantidad, c.codigo_producto, c.costo_envio,
                                  c.estado_boleta, c.estado_entrega, c.estado_venta, 
                                  c.fecha, c.id_venta, c.id_hijo_producto,
                                  c.id_padre_producto, c.mail, c.market,
                                  c.n_venta, c.nombre_cliente, c.nombre_producto,
                                  c.phone, c.precio, c.url_boleta] for c in checkouts.query.all()], 
                                 columns = ["cantidad", "codigo producto", "costo envio", "estado boleta",
                                           "estado entrega", "estado venta", "fecha", "id venta", "id hijo producto",
                                           "id padre producto", "mail", "market", "numero venta", 
                                            "nombre cliente", "nombre producto", "telefono", "precio",
                                           "url boleta"])
    
    # Sending data
    current_app.logger.info("Sending data checkouts")
    buffer = io.BytesIO()
    checkout_data = checkout_data.sort_values("fecha", ascending=False)
    checkout_data.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=ventas.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

@download.get("/deliverys")
def download_deliverys():
    # Retrieving data
    current_app.logger.info("Retrieving data of deliverys from DB")
    delivery_data = pd.DataFrame([[c.n_seguimiento, c.codigo, c.codigo_venta,
                                  c.courier, c.delivery_status, c.direccion, 
                                  c.impresion_etiqueta, c.fecha_despacho, c.fecha_promesa,
                                  c.id_venta, c.status_etiqueta, c.n_venta] 
                                  for c in deliverys.query.all()], 
                                 columns = ["N seguimiento", "codigo", "codigo venta", "courier",
                                           "delivery status", "direccion", "impresion etiqueta", "fecha despacho",
                                           "fecha promesa", "id venta", "status etiqueta", "N venta"])
    
    # Sending data
    current_app.logger.info("Sending data deliverys")
    buffer = io.BytesIO()
    delivery_data = delivery_data.sort_values("fecha despacho", ascending=False)
    delivery_data.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=deliverys.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)


###################################################################################################
###########    SECCION DE ENDPOINTS ESCONDIDOS (NO UTILES, SOLO DESARROLLO)   #####################
###################################################################################################
@download.get("/customs_attributes")
@auth_required("basic")
def download_customs_attribute():
    # Retrieving data
    current_app.logger.info("Retrieving data of custom_attributes from DB")
    engine = db.engine
    with engine.connect() as connection:
        customs_data = pd.read_sql_table("customs_ids", connection)
    # Sending data
    current_app.logger.info("Sending data custom_attributes")
    buffer = io.BytesIO()
    customs_data.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Atributos customs.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

###########################################################################################################################
@download.get("/customs_attributes2")
@auth_required("basic")
def download_customs_attribute2():
    # Retrieving data
    current_app.logger.info("Retrieving data of custom_attributes from DB")
    
    engine = db.engine
    with engine.connect() as connection:
        customs_data = pd.read_sql_table("customs_ids", connection)
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
    map_att =  {"PR": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
                      columns=["Mapeo", "Atributo"]), 
                "RP": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Ripley.query.all()], 
                      columns=["Mapeo", "Atributo"]),
                "MLC": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_MercadoLibre.query.all()], 
                      columns=["Mapeo", "Atributo"]), 
                "FL": pd.DataFrame([[m.Mapeo, m.Atributo] for m in Mapeo_Falabella.query.all()], 
                      columns=["Mapeo", "Atributo"])}
    l = []
    for market in maps.columns[1:5]:
        for cat in maps[market]:
            if market == "MercadoLibre":
                name = cat.split(" - ")[-1]
                info = "MLC", name
            elif market == "Falabella":
                words = cat.split(" > ")
                words[-1] = words[-1].replace(u"\xa0", "")
                info = "FL", words[-1]
            elif market == 'Ripley':    
                words = cat.split(" > ")
                words[-1] = words[-1].replace(u"\xa0", "")
                info = "RP", words[-1]
            else:
                cat2 = maps[cat == maps['Paris']].iloc[:, -1].values
                if len(cat2) >= 1:
                    cat2 = cat2[0]
                # Make adjustments
                name = cat2.replace(u"\xa0", "")
                info = "PR", name

            lm = get_attributes(info, atts)
            atm = get_att_map(lm, map_att)
            if market == "MercadoLibre":
                cd = customs_data[(customs_data['name'].isin(atm[1])) & (customs_data['name_set'].str.contains("HB"))]
            elif market == "Falabella":
                cd = customs_data[(customs_data['name'].isin(atm[1])) & (customs_data['name_set'].str.contains("Falabella"))]
            elif market == "Ripley":
                cd = customs_data[(customs_data['name'].isin(atm[1])) & (customs_data['name_set'].str.contains("Ripley"))]
            else:
                cd = customs_data[(customs_data['name'].isin(atm[1])) & (customs_data['name_set'].str.contains("Paris"))]

            if cd[cd['option_id'].notna()].shape[0] > 0:
                tmp = cd[['name', 'option_name', 'name_set']][cd['option_id'].notna()].copy()
                tmp["category"] = maps[maps[market] == cat]['Multivende'].values[0]
                l.append(tmp.reset_index(drop=True))
            
    data = pd.concat(l).reset_index(drop=True).drop_duplicates()
    
    # Sending data
    current_app.logger.info("Sending data custom_attributes")
    buffer = io.BytesIO()
    data.to_excel(buffer, index = False)
    
    headers = {
        'Content-Disposition': 'attachment; filename=Atributos customs.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)