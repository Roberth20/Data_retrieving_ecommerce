"""Modulo con los endpoints relacionados a la actualizacion de los datos"""

from App.update import update
import pandas as pd
from flask import render_template, request, redirect
from App.models.mapeo_atributos import *
from App.extensions.db import db
from App.update.funcs import *
from App.models.mapeo_categorias import Mapeo_categorias
from flask_security import auth_required
from App.models.auth import auth_app
from App.auth.funcs import decrypt
from flask import current_app
from App.models.productos import get_products
from App.get_data.Populate_tables import upload_data_products
from datetime import datetime, timedelta
from App.models.clients import clients
from App.models.ids import ids, customs_ids
from App.models.checkouts import checkouts, deliverys
import requests
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
    url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/all-product-attributes"
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
    headers = {
            'Authorization': f'Bearer {token}'
    }
    # Get data
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        return "Hubo un error.\n" + response.text
    # Obtenemos dos grupos de atributos, lo separamos
    att = response["customAttributes"]
    att_std = ["Season", "model", "description", "htmlDescription", "shortDescription",
                "htmlShortDescription", "Warranty", "Brand", "name", "ProductCategory", "sku_name", "color",
                "size", "sku", "internalSku", "width", "length", "height", "weight", "IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO", "tags", "picture"]
    # Transformamos los nombres para mayor comodidad
    att_names = [item["name"]+"-"+item["CustomAttributeSet.name"] for item in att]
    att_short_names = [item["name"] for item in att]

    # Obtenemos la lista de todos los productos.
    current_app.logger.info("Solicitando productos")
    url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/products/light/p/1"
    response = requests.request("GET", url, headers=headers).json()
    data = response["entries"]
    pages = response["pagination"]["total_pages"]
    # Los productos se organizan en paginas, pasamos por todas, guardando los resultados
    for p in range(pages-1):
        url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/products/light/p/{p+2}"
        response = requests.request("GET", url, headers=headers).json()
        data += response["entries"]

    # Extraemos los id de cada uno
    ids = [item["_id"] for item in data]

    # Para cada producto, guardamos sus atributos
    current_app.logger.info("Solicitando atributos de productos.")
    data = []
    for i in ids:
        url = f"https://app.multivende.com/api/products/{i}?_include_product_picture=true"
        response = requests.request("GET", url, headers=headers).json()
        data.append(response)

    # Dentro de los atributos normales, extraemos los atributos hechos por el usuario
    current_app.logger.info("Procesando atributos personales")
    customs = []
    for d in data:
        tmp_dict = {}
        for at in d["CustomAttributeValues"]:
            tmp_dict[at["CustomAttribute"]["name"]] =  at["text"]

        tmp_dict["tags"] = []
        for tag in d["ProductTags"]:
            tmp_dict["tags"].append(tag["Tag"]["name"])

        customs.append(tmp_dict)
        d.pop("CustomAttributeValues")

    # Unimos cada conjunto de atributos con sus custom atributos
    info = []
    for i in range(len(data)):
        info.append(data[i] | customs[i])

    # Hay atributos estandar que tienen informacion anidada, extraemos la misma
    current_app.logger.info("Procesando atributos estandar")
    all_data = []
    for i in info:
        sku = i["code"]
        i["sku_name"] = sku
        i["IDENTIFICADOR_PADRE"] = i["_id"]
        # Cuando el atributo no tiene valor, no posee informacion anidada
        # evitamos errores tomando este aspecto en consideracion
        try:
            brand = i["Brand"]["name"]
        except:
            brand = None
        i["Brand"] = brand
        try:
            cat = i["ProductCategory"]["name"]
        except:
            cat = None
        i["ProductCategory"] = cat
        try:
            war = i["Warranty"]["name"]
        except:
            war = None
        i["Warranty"] = war
        try:
            tag = i["ProductTags"][0]["Tag"]["name"]
        except:
            tag = None
        i["tags"] = tag
        try:
            picture = i["ProductPictures"][0]["url"]
        except:
            picture = None
        i["picture url"] = picture
        # Extraemos la misma informacion para cada version de producto
        for pv in i["ProductVersions"]:
            j = i.copy()
            tmp_dict = {}
            for at in pv["CustomAttributeValues"]:
                tmp_dict[at["CustomAttribute"]["name"]] =  at["text"]
            pv.pop("CustomAttributeValues")
            j = j | tmp_dict
            j["color"] = pv["Color"]["name"]
            j["size"] = pv["Size"]["name"]
            j["sku"] = pv["code"]
            j["internalSku"] = pv["internalCode"]
            j["height"] = pv["height"]
            j["length"] = pv["length"]
            j["weight"] = pv["weight"]
            j["width"] = pv["width"]
            j["IDENTIFICADOR_HIJO"] = pv["_id"]
            j.pop("ProductVersions")
            all_data.append(j)

    # Generamos la tabla de datos
    df = pd.DataFrame(all_data, columns = att_std + att_short_names)
    df.columns = att_std + att_names

    # Limpiamos atributos de informacion que no es relavante o complementaria
    # de la base de datos de multivende
    markets = []
    for market in ["HB", "Falabella", "Ripley", "Paris", "Shopify"]:
        for c in df.columns:
            if market in c:
                markets.append(c)

    basics = [d for d in df.columns if d not in markets]
    df = df[basics+markets]

    # Limpiamos columnas duplicadas
    df.drop(columns = df.columns[df.columns.duplicated()], inplace =True)
    data = get_products()
    #print(df.columns.isin(data.columns))
    df = df[df.columns[df.columns.isin(data.columns)]]
    diff = df[~df.isin(data)].dropna(how="all")
    if diff.shape[0] == 0:
        return render_template("update/products.html")
    
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
    return render_template("update/products_updated", message=message)

@update.get("/clients")
@auth_required("basic")
def clients_data():
    # Retrieve data from the available marketplaces
    fl_customers = get_data_falabella(current_app.config["FALABELLA_USER"], current_app.config["FALABELLA_API_KEY"])
    if type(fl_customers) == str:
        return fl_customers
    pr_customers = get_data_paris(current_app.config["PARIS_API_KEY"])
    if type(pl_customers) == str:
        return pl_customers
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
    # Last time updated
    result = db.session.scalar(db.select(checkouts).order_by(checkouts.fecha.desc()))
    last_update = result.fecha - timedelta(days=28) # One month before to update changes of recents sells
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    last = last_update.strftime("%Y-%m-%dT%H:%M:%S")
    url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/checkouts/light/p/1?_updated_at_from={last}&_updated_at_to={now}"
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
    headers = {
            'Authorization': f'Bearer {token}'
    }
    # Get data
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        return "Error" + response.text
    
    pages = response["pagination"]["total_pages"]
    ids= []
    # First all ids
    for p in range(0, pages):
        url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/checkouts/light/p/{p+1}?_updated_at_from={last}&_updated_at_to={now}"
        data = requests.get(url, headers=headers).json()
        for d in data["entries"]:
            ids.append(d["_id"])
    
    # Now the information completed
    ventas = []
    for id in ids:
        tmp = {}
        url = f"https://app.multivende.com/api/checkouts/{id}"
        checkout = requests.get(url, headers=headers).json()
        tmp["fecha"] = checkout["soldAt"]
        tmp["nombre"] = checkout["Client"]["fullName"]
        tmp["n venta"] = checkout["CheckoutLink"]["externalOrderNumber"] # Numero de orden en marketplace
        tmp["id"] = checkout["CheckoutLink"]["CheckoutId"] # Codigo en multivende
        tmp["estado entrega"] = checkout["deliveryStatus"]
        tmp["costo de envio"] = checkout["DeliveryOrderInCheckouts"][0]["DeliveryOrder"]["cost"]
        tmp["market"] = checkout["origin"]
        tmp["mail"] = checkout["Client"]["email"]
        tmp["phone"] = checkout["Client"]["phoneNumber"]
        # Try to find the billing files
        try:
            url = f"https://app.multivende.com/api/checkouts/{id}/electronic-billing-documents/p/1"
            billing = requests.get(url, headers=headers).json()
            tmp["estado boleta"] = billing["entries"][-1]["ElectronicBillingDocumentFiles"][-1]["synchronizationStatus"]
            tmp["url boleta"] = billing["entries"][-1]["ElectronicBillingDocumentFiles"][-1]["url"]
        except:
            tmp["estado boleta"] = None
            tmp["url boleta"] = None
        
        # Getting all status of ventas
        tmp["estado venta"] = []
        for status in checkout["CheckoutPayments"]:
            tmp["estado venta"].append(status["paymentStatus"])
        # For each item we split the checkout
        for product in checkout["CheckoutItems"]:
            item = tmp.copy()
            item["codigo producto"] = product["code"]
            item["nombre producto"] = product["ProductVersion"]["Product"]["name"]
            item["id padre producto"] = product["ProductVersion"]["ProductId"]
            item["id hijo producto"] = product["ProductVersionId"]
            item["cantidad"] = product["count"]
            item["precio"] = product["gross"]
            ventas.append(item)

    # Load data to be processed
    df = pd.DataFrame(ventas)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["fecha"] = df["fecha"].dt.tz_convert(None)
    df= df.fillna(np.nan)
    for i in df["estado venta"].index:
        df.loc[i, "estado venta"] = df["estado venta"][i][-1]
        
    df = df.where(df.notna(), None)
    
    check_difference_and_update_checkouts(df, checkouts, db)
    
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
    
    # Get marketplace connections
    url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/marketplace-connections/p/1"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    current_app.logger.info("Solicitando ids de marketplaces connections")
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        return "Error" + response.text
    data=[]
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    for conn in response["entries"]:
        url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/delivery-orders/documents/p/1?_delivery_statuses=completed&_delivery_statuses=pending&_shipping_label_print_statuses=not_printed&_shipping_label_status=ready&include_only_delivery_order_with_traking_number=true&_marketplace_connection_id={conn['_id']}&_updated_at_from={last}&_updated_at_to={now}"
        # Get shipping labels for connections
        try:
            info = requests.get(url, headers=headers).json()
        except:
            current_app.logger.info(f"No shipping labels from {requests.get(url, headers=headers).text}")
            
        if len(info["entries"]) > 0:
            pages = info["pagination"]["total_pages"]
            for p in range(0, pages):
                url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/delivery-orders/documents/p/{p}?_delivery_statuses=completed&_delivery_statuses=pending&_shipping_label_print_statuses=not_printed&_shipping_label_status=ready&include_only_delivery_order_with_traking_number=true&_marketplace_connection_id={conn['_id']}&_updated_at_from={last}&_updated_at_to={now}"
                entries = requests.get(url, headers=headers).json()
                # Store important data
                for entry in entries["entries"]:
                    tmp = {}
                    tmp["fecha promesa"] = entry["promisedDeliveryDate"]
                    tmp["direccion"] = entry["deliveryAddress"]
                    tmp["codigo"] = entry["code"]
                    tmp["courier"] = entry["courierName"]
                    tmp["fecha despacho"] = entry["handlingDateLimit"]
                    tmp["delivery status"] = entry["deliveryStatus"]
                    tmp["N seguimiento"] = entry["trackingNumber"]
                    tmp["codigo venta"] = entry["DeliveryOrderInCheckouts"][0]["Checkout"]["code"]
                    tmp["id venta"] = entry["DeliveryOrderInCheckouts"][0]["CheckoutId"]
                    tmp["status etiqueta"] = entry["shippingLabelStatus"]
                    #tmp["etado impresion etiqueta"] = entry["shippingLabelPrintStatus"]
                    data.append(tmp)
    
    # Create dataframe and adjust formats
    df = pd.DataFrame(data)
    df.fillna(np.nan, inplace=True)
    df["fecha despacho"] = pd.to_datetime(df["fecha despacho"])
    df["fecha despacho"] = df["fecha despacho"].dt.tz_convert(None)
    df["fecha promesa"] = pd.to_datetime(df["fecha promesa"])
    df["fecha promesa"] = df["fecha promesa"].dt.tz_convert(None)
    
    # Load data from checkouts
    ventas = pd.DataFrame([[v.id_venta, v.n_venta] for v in checkouts.query.all()],
                         columns = ["id", "n venta"])
    # Add n venta to df
    for i in df.index:
        if (ventas["id"] == df.loc[i, "id venta"]).any():
            df.loc[i, "n venta"] = ventas["n venta"][ventas["id"] == df.loc[i, "id venta"]].values[0]
    
    # Fill empty values with None
    df = df.replace({np.NaN: None})
    # Clear duplicated
    df = df.drop_duplicates()
    # Only store the items with n venta (a checkout registered)
    df = df[df["n venta"].notna()]
    
    check_diferences_and_update_deliverys(df, deliverys, db)
    
    return render_template("update/delivery.html")


@update.route("/ids", methods=["GET", "POST"])
@auth_required("basic")
def update_ids():
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
            for i, row in df.iterrows():
                new_ids = ids(id=row["_id"], name=row["name"], type=row["type"])
                db.session.add(new_ids)
            
            db.session.commit()
            
            return render_template("update/success.html", market = "Ids")
    
    return render_template("update/sample.html", market="Ids")

###################################################################################################
###############################          DELEVOPING-PURPOSE         ###############################
###################################################################################################
@update.route("/custom_ids", methods=["GET", "POST"])
@auth_required("basic")
def update_custom_ids():
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
            df = df.where(df.notna(), None)
            for i, row in df.iterrows():
                # Update data
                c_ids = customs_ids(id_set = row["id_set"], name_set = row["name_set"], id = row["id"],
                                   name = row["name"], option_name = row["option_name"], option_id = row["option_id"])
                db.session.add(c_ids)
            
            db.session.commit()
            
            return render_template("update/success.html", market = "custom Ids")
    
    return render_template("update/sample.html", market="custom Ids")

    