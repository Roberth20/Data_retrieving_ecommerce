"""Modulo para el envio de productos actualizados en el servidor a la base
de datos de Multivende, actualizando los mismos en cada Marketplace.

NOTA: Solo actualiza productos previamente creados. Para crear productos o versiones nuevas
se debe cambiar el 'status', hacer ajustes de inventario y precios.
"""
# Import all the things
from App.create_update import cupdate
from flask import render_template, current_app
from flask_security import auth_required
from App.create_update.funcs import get_serialized_data
from App.models.productos import get_products
from App.models.ids import ids, customs_ids
from App.extensions.db import db
from App.models.auth import auth_app
from datetime import datetime
from App.auth.funcs import decrypt
import requests
import pandas as pd
import numpy as np
import json

# Landing page for the upload
@cupdate.get("/")
@auth_required("basic")
def confirmation():
    return render_template("create_update/main.html")

# Endpoint to prepare data and upload
@cupdate.get("/send")
@auth_required("basic")
def send_form():
    # Get the last token info registed on the DB
    current_app.logger.debug("Retrieving Token")
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    diff = datetime.utcnow() - last_auth.expire
    
    # Check if exists a token
    if last_auth == None:
        current_app.logger.warning("There is not token in the DB")
        return render_template("update/token_error.html")
    
    # Check if the token expired (after 6 hours)
    if diff.total_seconds()/3600 > 6:
        current_app.logger.debug("The token expired.")
        return render_template("update/token_error.html")
    
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    
    # Load data
    current_app.logger.debug("Getting data of products to send.")
    products = get_products()
    id_data = pd.DataFrame([[i.id, i.name, i.type] for i in ids.query.all()],
                          columns = ["id", "name", "type"])
    engine = db.engine
    with engine.connect() as connection:
        customs_data = pd.read_sql_table("customs_ids", connection)
        
    std = products.columns[:21]
    
    current_app.logger.debug("Preparing data to send json of products")
    # Prepare to send data of each product on the database
    message = ""
    data = {}
    markets = ["Ripley", "Falabella", 'Paris', 'MercadoLibre']
    for i in range(products.shape[0]):
        # Get product
        p = products.iloc[i, :].copy()
        p = p.fillna(np.nan)
        #if p.name[0] == "a3409a4a-e181-4cb1-a09b-8af0f5968e55":
         #   print(f"Pasando {p['name']}")
          #  continue
        # Set variable for the customs attribute by marketplace
        customs_att = {}
        for m in markets:
            # Get the data ready to sent from attribute by market
            custom_p, custom_v = get_serialized_data(p, customs_data, std, m)
            if type(custom_p) == str:
                current_app.logger.debug(f"Aborting by {custom_p}, {p.name[0]}")
                return render_template("create_update/error.html", message = f"Cancelando envio por {custom_p}")
                #print(" ".join(custom_v.split("_")))
            customs_att[m] = custom_p, custom_v
        
        # Preparing all info to be inserted in the upload template
        tags = None
        if pd.notna(p["tags"]):
            tags = []
            for tag in p['tags'].split(";"):
                if tag[0] == " ":
                    tag = tag[1:]
                if tag[-1] == " ":
                    tag = tag[:-1]
                try:
                    tags.append({
                          "text": tag,
                          "_id": id_data["id"][(id_data["name"] == tag) & (id_data["type"] == "tag")].values[0]
                      })
                except:
                    current_app.logger.debug(f"Tag: {tag} not found. {p['tags']}")
        productCategory = None
        if pd.notna(p["ProductCategory"]):
            productCategory = id_data["id"][id_data["name"] == p["ProductCategory"]].values[0]
        warranty = None
        if pd.notna(p["Warranty"]):
            warranty = id_data["id"][id_data["name"] == p["Warranty"]].values[0]
        size = None
        if pd.notna(p["size"]):
            size = id_data["id"][(id_data["name"] == p["size"]) & (id_data["type"] == "size")].values[0]
        brand = None
        if pd.notna(p["Brand"]):
            try:
                brand =  id_data["id"][(id_data["name"] == p["Brand"]) & (id_data["type"] == "brand")].values[0]
            except:
                current_app.logger.debug(f"ID of brand: {p['Brand']} not in database")
                return render_template("create_update/error.html", message = f"Cancelando envio por: ID of brand: {p['Brand']} not in database")
        color = None
        if pd.notna(p["color"]):
            color =  id_data["id"][id_data["name"] == p["color"]].values[0]
        p = p.where(p.notna(), None)
        # Inserting data in the template
        payload = json.dumps({
          "name": p["name"],
          "alias": p["name"],
          "BrandId": brand,
          "model": p["model"],
          "SeasonId": None, # All are None
          "description": p["description"],
          "ProductVersions": [
            {
              "_id": p.name[1],
              "code": p["sku"],
              #"SizeId": size,
              "ColorId": color,
              "status": "created",
              "internalCode": p["internalSku"],
              "CodeTypeId": None,
              "InternalCodeTypeId": None,
              "position": 0,
              "width": p["width"],
              "length": p["length"],
              "height": p["height"],
              "weight": p["weight"],
              "Size": {
                "_id": size,
                "name": p["size"]
              },
              "CodeType": None,
              "InternalCodeType": None,
              "Color": {
                "_id": color,
                "name": p["color"]
              },
                "custom_v":0,
              #"CustomAttributeValues": custom_v,
              #"allImages": [],
              "InventoryTypeId": "791a6654-c5f2-11e6-aad6-2c56dc130c0d",
              "isDefaultVerson":True
            },
          ],
          "ProductCategoryId": productCategory,
          "code": p["sku_name"],
          "internalCode": None,
          "CodeTypeId": None,
          "shortDescription": p["shortDescription"],
          "htmlDescription": p["htmlDescription"],
          "htmlShortDescription": p["htmlShortDescription"],
        ###### Tags son solo uno o ninguno en todos los productos
          "tags": tags,
          "WarrantyId": warranty,
          "ShippingClassId": None,
          "custom_p":0,
          #"CustomAttributeValues": custom_p,
          "InventoryTypeId": "791a6654-c5f2-11e6-aad6-2c56dc130c0d",
          "InternalCodeTypeId": None,
          "status": "created"
        })
        
        # Add customs attributes values separated by marketplace with valid format
        t = '"CustomAttributeValues": {'
        v = '"CustomAttributeValues": {'
        for x in [customs_att[k][0] for k in customs_att.keys() if customs_att[k][0] != {}]:
            for kx in x.keys():
                t += f'{json.dumps(kx)}: {json.dumps(x[kx])}, '
        
        for y in [customs_att[k][1] if customs_att[k][1] != {} else 0 for k in customs_att.keys()]:
            if y == 0:
                continue
            for ky in y.keys():
                if ky == "932bc231-7add-4a76-9d4a-5ea36f33cccd" or ky == "6d4452c8-0cd6-4ef4-9c86-f86ab705e607":
                    v += f"{json.dumps(ky)}: {json.dumps(str(int(y[ky])))}, "
                else:
                    v += f"{json.dumps(ky)}: {json.dumps(y[ky])}, "
                
        if len(t) > 26:
            t = t[:-2] + "}, "
        else:
            t += "},"
        if len(v) > 26:
            v = v[:-2] + "}, "
        else:
            v += "}, "
        payload = payload.replace('"custom_p": 0,', t)
        payload = payload.replace('"custom_v": 0,', v)
        
        data[p.name[0]] = payload
    
    current_app.logger.debug("Sending request PUT to update products at Multivende") # UPDATE
    for k in data.keys():
        headers = {
          'Content-Type': 'application/json',
          'Authorization': f'Bearer {token}'
        }
        # Sending request 
        url = f"https://app.multivende.com/api/products/{k}" # UPDATE
        #url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/products" # CREATE 
        #current_app.logger.info("Sending request POST to update products at Multivende") # CREATE
        response = requests.request("PUT", url, headers=headers, data=data[k]) # UPDATE
        #print(response.status_code)
        #response = requests.request("POST", url, headers=headers, data=payload) # CREATE
        # Check there was an error and abort sending data
        if response.status_code != 201:
            current_app.logger.error(f"Aborting sending data for reason: {response.reason}")
            message = response.reason + response.text + "\n\n" + data[k]
            return render_template("create_update/error.html", message=message)
        else:
            message += p["name"] + " OK \n"
    current_app.logger.debug("Data sent without problems")
    return render_template("create_update/success.html")

# Endpoint to prepare data and upload
@cupdate.get("/send_test")
@auth_required("basic")
def send_form_test():
    # Get the last token info registed on the DB
    current_app.logger.info("Retrieving Token")
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    diff = datetime.utcnow() - last_auth.expire
    
    # Check if exists a token
    if last_auth == None:
        current_app.logger.warning("There is not token in the DB")
        return render_template("update/token_error.html")
    
    # Check if the token expired (after 6 hours)
    if diff.total_seconds()/3600 > 6:
        current_app.logger.info("The token expired.")
        return render_template("update/token_error.html")
    
    # Decrypt token
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    
    # Load data
    current_app.logger.info("Getting data of products to send.")
    products = get_products()
    id_data = pd.DataFrame([[i.id, i.name, i.type] for i in ids.query.all()],
                          columns = ["id", "name", "type"])
    customs_data = pd.DataFrame([[c.id_set, c.name_set, c.id, c.name, c.option_id,
                                 c.option_name] for c in customs_ids.query.all()],
                               columns = ["id_set", "name_set", "id", "name", "option_id",
                                         "option_name"])
    std = products.columns[:20]
    
    # Prepare to send data of each product on the database
    message = None
    current_app.logger.info("Preparing data to send json of products")
    for i in range(products.shape[0]):
        # Get product
        p = products.iloc[i, :].copy()
        p = p.fillna(np.nan)
        
        # Get data in better format to work
        custom_p, custom_v = get_serialized_data(p, customs_data, std)
        if p.name[0] != "e5b56f6c-d5ed-428f-b7f1-95e2fcfa55bb":
            continue
        # Preparing all info to be inserted in the upload template
        tags = []
        if pd.notna(p["tags"]):
            tags.append({
                  "text": p["tags"],
                  "_id": id_data["id"][id_data["name"] == p["tags"]].values[0]
              })
        productCategory = None
        if pd.notna(p["ProductCategory"]):
            productCategory = id_data["id"][id_data["name"] == p["ProductCategory"]].values[0]
        warranty = None
        if pd.notna(p["Warranty"]):
            warranty = id_data["id"][id_data["name"] == p["Warranty"]].values[0]
        size = None
        if pd.notna(p["size"]):
            size = id_data["id"][id_data["name"] == p["size"]].values[0]
        brand = None
        if pd.notna(p["Brand"]):
            try:
                brand =  id_data["id"][id_data["name"] == p["Brand"]].values[0]
            except:
                current_app.logger.info(f"ID of brand: {brand} not in database")
        color = None
        if pd.notna(p["color"]):
            color =  id_data["id"][id_data["name"] == p["color"]].values[0]
        p = p.where(p.notna(), None)
        # Inserting data in the template
        payload = json.dumps({
          "name": p["name"],
          "alias": p["name"],
          "BrandId": brand,
          "model": p["model"] + " test",
          "SeasonId": None, # All are None
          "description": p["description"] + (10*" test"),
          "ProductVersions": [
            {
              "_id": p.name[1],
              "code": p["sku"]+" test",
              "SizeId": size,
              "ColorId": color,
              "status": "waiting-for-creation",
              "internalCode": p["internalSku"],
              "CodeTypeId": None,
              "InternalCodeTypeId": None,
              "position": 0,
              "width": p["width"],
              "length": p["length"],
              "height": p["height"],
              "weight": p["weight"],
              "Size": {
                "_id": size,
                "name": p["size"]
              },
              "CodeType": None,
              "InternalCodeType": None,
              "Color": {
                "_id": color,
                "name": p["color"]
              },
              "CustomAttributeValues": custom_v,
              #"allImages": [],
              "InventoryTypeId": "791a6654-c5f2-11e6-aad6-2c56dc130c0d",
              "isDefaultVerson":True
            },
          ],
          "ProductCategoryId": productCategory,
          "code": p["sku_name"],
          "internalCode": None,
          "CodeTypeId": None,
          "shortDescription": p["shortDescription"],
          "htmlDescription": p["htmlDescription"],
          "htmlShortDescription": p["htmlShortDescription"],
        ###### Tags son solo uno o ninguno en todos los productos
          "tags": tags,
          "WarrantyId": warranty,
          "ShippingClassId": None,
          "CustomAttributeValues": custom_p,
          "InventoryTypeId": "791a6654-c5f2-11e6-aad6-2c56dc130c0d",
          "InternalCodeTypeId": None,
          "status": "created"
        })
        headers = {
          'Content-Type': 'application/json',
          'Authorization': f'Bearer {token}'
        }

        # Sending request 
        url = f"https://app.multivende.com/api/products/{p.name[0]}" # UPDATE
        #url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/products" # CREATE 
        #current_app.logger.info("Sending request POST to update products at Multivende") 
        current_app.logger.info("Sending request PUT to update products at Multivende")
        response = requests.request("PUT", url, headers=headers, data=payload) # UPDATE
        #response = requests.request("POST", url, headers=headers, data=payload)
        
        # Check there was an error and abort sending data
        if response.status_code != 201:
            current_app.logger.error(f"Aborting sending data for reason: {response.reason}")
            message = response.reason + " " + p["name"] + " " + response.text + " " + response.request.body
            return render_template("create_update/error.html", message=message)
        
        message = p["name"] + " OK"
        #break    
    current_app.logger.info("Data sent without problems")
    return render_template("create_update/success.html")