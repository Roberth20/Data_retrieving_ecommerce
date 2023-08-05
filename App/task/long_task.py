from App.extensions.celery import celery
import time
import requests
import pandas as pd
import json
from App.models.checkouts import checkouts, deliverys
from App.extensions.db import db
import numpy as np
from App.update.funcs import check_difference_and_update_checkouts
from App.update.funcs import check_diferences_and_update_deliverys
from flask import current_app
from App.models.productos import get_products
from App.get_data.Populate_tables import upload_data_products
from App.models.auth import auth_app
from App.models.ids import ids, customs_ids
from App.update.funcs import get_customs_attributes
from App.update.funcs import get_data_brands
from App.update.funcs import get_data_warranties
from App.update.funcs import get_data_tags
from App.update.funcs import get_data_colors
from App.update.funcs import get_data_categories
from App.update.funcs import get_data_size
from App.models.mapeo_atributos import * 
from App.models.atributos_market import * 
from App.models.mapeo_categorias import Mapeo_categorias
from App.download.help_func import col_color, missing_info

@celery.task
def celery_long_task(duration):
    for i in range(duration):
        print(i)
        time.sleep(5)
          
@celery.task
def update_deliverys(token, merchant_id, last, now):
    # Get marketplace connections
    url = f"https://app.multivende.com/api/m/{merchant_id}/marketplace-connections/p/1"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    #current_app.logger.info("Solicitando ids de marketplaces connections")
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        return "Error" + response.text
    
    data=[]
    s = len(response["entries"])
    for i, conn in enumerate(response["entries"]):
        print(f"Working on update deliverys... connection {i}/{s}")
        url = f"https://app.multivende.com/api/m/{merchant_id}/delivery-orders/documents/p/1?_delivery_statuses=completed&_delivery_statuses=pending&_shipping_label_print_statuses=not_printed&_shipping_label_status=ready&include_only_delivery_order_with_traking_number=true&_marketplace_connection_id={conn['_id']}&_updated_at_from={last}&_updated_at_to={now}"
        # Get shipping labels for connections
        try:
            info = requests.get(url, headers=headers).json()
        except:
            print(f"No shipping labels from {requests.get(url, headers=headers).text}")
            #current_app.logger.info(f"No shipping labels from {requests.get(url, headers=headers).text}")
        
        if len(info["entries"]) > 0:
            pages = info["pagination"]["total_pages"]
            for p in range(0, pages):
                print(f"Working on update deliverys... page {p+1}/{pages}")
                url = f"https://app.multivende.com/api/m/{merchant_id}/delivery-orders/documents/p/{p+1}?_delivery_statuses=completed&_delivery_statuses=pending&_shipping_label_print_statuses=not_printed&_shipping_label_status=ready&include_only_delivery_order_with_traking_number=true&_marketplace_connection_id={conn['_id']}&_updated_at_from={last}&_updated_at_to={now}"
                entries = requests.get(url, headers=headers).json()
                # Store important data
                l = len(entries["entries"])
                for i, entry in enumerate(entries["entries"]):
                    print(f"Working on update deliverys... {i}/{l}")
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
                    tmp["estado impresion etiqueta"] = entry["shippingLabelPrintStatus"]
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
    print("Deliverys updated")
            
@celery.task
def update_checkouts(token, merchant_id, last, now):
    url = f"https://app.multivende.com/api/m/{merchant_id}/checkouts/light/p/1?_updated_at_from={last}&_updated_at_to={now}"
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
        print(f"Working to get ids... {p+1}/{pages}")
        url = f"https://app.multivende.com/api/m/{merchant_id}/checkouts/light/p/{p+1}?_updated_at_from={last}&_updated_at_to={now}"
        data = requests.get(url, headers=headers).json()
        for d in data["entries"]:
            ids.append(d["_id"])
    
    # Now the information completed
    ventas = []
    total_ids = len(ids)
    for i, id in enumerate(ids):
        print(f"Working to get data... {i}/{total_ids}")
        tmp = {}
        url = f"https://app.multivende.com/api/checkouts/{id}"
        checkout = requests.get(url, headers=headers)
        try:
            checkout = checkout.json()
        except:
            current_app.logger.error(f"Error: {checkout.text}")
        
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
        
    df = df.replace({np.NaN: None})
    
    check_difference_and_update_checkouts(df, checkouts, db)
    print("Updated checkouts database")
    
@celery.task
def update_products(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/all-product-attributes"
    headers = {
            'Authorization': f'Bearer {token}'
    }
    # Get data
    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except:
        current_app.logger.error("Hubo un error: "+response.text)
        return
    
    # Obtenemos dos grupos de atributos, lo separamos
    att = response["customAttributes"]
    att_std = ["Season", "model", "description", "htmlDescription", "shortDescription",
                "htmlShortDescription", "Warranty", "Brand", "name", "ProductCategory", "sku_name", "color",
                "size", "sku", "internalSku", "width", "length", "height", "weight", "IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO", "tags", "picture"]
    # Transformamos los nombres para mayor comodidad
    att_names = [item["name"]+"-"+item["CustomAttributeSet.name"] for item in att]
    att_short_names = [item["name"] for item in att]
    
    # Obtenemos la lista de todos los productos.
    current_app.logger.debug("Solicitando productos")
    url = f"https://app.multivende.com/api/m/{merchant_id}/products/light/p/1"
    response = requests.request("GET", url, headers=headers).json()
    data = response["entries"]
    pages = response["pagination"]["total_pages"]
    # Los productos se organizan en paginas, pasamos por todas, guardando los resultados
    for p in range(pages-1):
        url = f"https://app.multivende.com/api/m/{merchant_id}/products/light/p/{p+2}"
        response = requests.request("GET", url, headers=headers).json()
        data += response["entries"]

    # Extraemos los id de cada uno
    ids = [item["_id"] for item in data]
    
    # Para cada producto, guardamos sus atributos
    current_app.logger.debug("Solicitando atributos de productos.")
    data = []
    for i in ids:
        url = f"https://app.multivende.com/api/products/{i}?_include_product_picture=true"
        response = requests.request("GET", url, headers=headers).json()
        data.append(response)

    # Dentro de los atributos normales, extraemos los atributos hechos por el usuario
    current_app.logger.debug("Procesando atributos personales")
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
    current_app.logger.debug("Procesando atributos estandar")
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
    df2 = df.set_index(["IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"])
    #df = df[df.columns[df.columns.isin(data.columns)]]
    #df.to_excel("test_products.xlsx")
    diff = df2[~df2.isin(data)].dropna(how="all")
    
    if diff.shape[0] == 0:
        return current_app.logger.debug("Los productos ya se encuentran actualizados.")
    
    # Replace with new data
    message = upload_data_products(df, db)
    
    celery.send_task("App.task.long_task.prepare_excel")
    
    
@celery.task
def update_db():
    from App.extensions.db import db
    import pandas as pd
    from flask import current_app
    from App.models.auth import auth_app
    from datetime import datetime, timedelta
    from App.auth.funcs import decrypt
    
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    # Check if token exists
    if last_auth == None:
        current_app.logger.info("There is not token available.")
        return
    diff = datetime.utcnow() - last_auth.expire
    # Check is token expired
    if diff.total_seconds()/3600 > 6:
        current_app.logger.info("The token expired.")
        return
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    
    # Last time updated
    result = db.session.scalar(db.select(checkouts).order_by(checkouts.fecha.desc()))
    last_update = result.fecha - timedelta(days=28) # One month before to update changes of recents sells
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    last = last_update.strftime("%Y-%m-%dT%H:%M:%S")
    
    current_app.logger.info("Updating Checkouts")
    celery.send_task("App.task.long_task.update_checkouts", [token, current_app.config["MERCHANT_ID"], last, now])
    #update_checkouts.delay(token, current_app.config['MERCHANT_ID'], last, now)
    
    # Last time updated
    result = db.session.scalar(db.select(deliverys).order_by(deliverys.fecha_despacho.desc()))
    last_update = result.fecha_despacho - timedelta(days=28) # One month before to update changes of recents sells
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    last = last_update.strftime("%Y-%m-%dT%H:%M:%S")
    
    current_app.logger.info("Updating Deliverys")
    celery.send_task("App.task.long_task.update_deliverys", [token, current_app.config["MERCHANT_ID"], last, now])
    #update_deliverys.delay(token, current_app.config['MERCHANT_ID'], last, now)

    
@celery.task
def update_token():
    from App.models.auth import auth_app
    from App.auth.funcs import encrypt, decrypt
    print("Updating token")
    current_app.logger.info("Updating token")
    url = "https://app.multivende.com/oauth/access-token"
    # Get last token
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    # Check if exists token
    if last_auth == None:
        print("No hay token disponible")
        return
    
    refresh_token = last_auth.refresh_token
    
    payload = json.dumps({
        "client_id": current_app.config["CLIENT_ID"],
        "client_secret": current_app.config["CLIENT_SECRET"],    
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    )
    headers = {
        'cache-control': 'no-cache',
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=payload)
    try:
        # Guardamos la informacion requerida y logueamos
        token = response.json()["token"]
        expiresAt = response.json()["expiresAt"]
        refresh_token = response.json()["refreshToken"]
        encrypted = encrypt(token, current_app.config["SECRET_KEY"])
        authentication = auth_app(token = encrypted, expire=expiresAt, refresh_token=refresh_token)
        db.session.add(authentication)
        db.session.commit()
        print("Actualizacion de token exitosa")
        current_app.logger.info('Actualizacion de token exitosa')
    except Exception as e:
        print("Error en la autenticacion de actualizacion: ", e)
        current_app.logger.error("Error en la autenticacion de actualizacion: ", e)
        
@celery.task
def upload_ids(token, merchant_id, model):
    if model == "ids":        
        current_app.logger.info("Getting data from brands")
        brands = get_data_brands(token, merchant_id)
        if type(brands) == str:
            current_app.logger.error(f"Error: {brands}")
            return
        
        current_app.logger.info("Getting data from warranties")
        warr = get_data_warranties(token, merchant_id)
        if type(warr) == str:
            current_app.logger.error(f"Error: {warr}")
            return

        current_app.logger.info("Getting data from tags")
        tags = get_data_tags(token, merchant_id)
        if type(tags) == str:
            current_app.logger.error(f"Error: {tags}")
            return
        
        current_app.logger.info("Getting data from colors")
        colors = get_data_colors(token, merchant_id)
        if type(colors) == str:
            current_app.logger.error(f"Error: {colors}")
            return
        
        current_app.logger.info("Getting data from categories")
        cats = get_data_categories(token, merchant_id)
        if type(cats) == str:
            current_app.logger.error(f"Error: {cats}")
            return

        current_app.logger.info("Getting data from sizes")
        size = get_data_size(token, merchant_id)
        if type(size) == str:
            current_app.logger.error(f"Error: {size}")
            return
        
        df = pd.concat([brands, warr, tags, colors, cats, size], ignore_index=True)
        
        current_app.logger.info("Uploading to DB")
        for i, row in df.iterrows():
            result = db.session.scalar(db.select(ids).where(ids.id == row["_id"]))
            if result == None:
                new_ids = ids(name = row["name"], id = row["_id"], type=row["type"])
                db.session.add(new_ids)
        db.session.commit()
        
        current_app.logger.info("Successful upload customs_ids to DB.")
    elif model == "customs_ids":
        current_app.logger.info("Retrieving custom attributes")
        data = get_customs_attributes(token, merchant_id)
        
        current_app.logger.info("Uploading to DB")
        for i, row in data.iterrows():
            result = db.session.scalar(db.select(customs_ids).where(customs_ids.id == row["id"]))
            if result == None:
                c_ids = customs_ids(id_set = row["id_set"], name_set = row["name_set"], id = row["id"],
                                   name = row["name"], option_name = row["option_name"], option_id = row["option_id"])
                db.session.add(c_ids)
        db.session.commit()
        
        current_app.logger.info("Successful upload customs_ids to DB.")
    else:
        current_app.logger.error(f"Error: model {model} no es valido.")
        
@celery.task
def prepare_excel():
    current_app.logger.info("Retrieving data of products from DB")
    # Load products
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
    products.reset_index(inplace=True)
    
    products.style.applymap_index(col_color, axis=1)\
            .apply(missing_info, maps=maps, atts = atts, map_att=map_att, std_transformation=std_transformation, axis=1)\
            .to_excel("App/output.xlsx", index=False)