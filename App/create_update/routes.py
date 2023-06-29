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
    for i in range(products.shape[0]):
        # Get product
        p = products.iloc[i, :].copy()
        p = p.fillna(np.nan)
        
        # Get data in better format to work
        current_app.logger.info("Preparing data to send json of products")
        custom_p, custom_v = get_serialized_data(p, customs_data, std)
        
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
            brand =  id_data["id"][id_data["name"] == p["Brand"]].values[0]
        color = None
        if pd.notna(p["color"]):
            color =  id_data["id"][id_data["name"] == p["color"]].values[0]
        
        # Inserting data in the template
        payload = json.dumps({
          "name": p["name"],
          "alias": None,
          "BrandId": brand,
          "model": p["model"],
          "SeasonId": None, # All are None
          "description": p["description"],
          "ProductVersions": [
            {
              "_id": p.name[1],
              "code": p["sku"],
              "SizeId": size,
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
              "InventoryType": {
                "_id": "791a6654-c5f2-11e6-aad6-2c56dc130c0d",
                "name": "INVENTORY_TYPES.Normal.Name",
                "code": "_normal_inventory_type",
                "description": "INVENTORY_TYPES.Normal.Description",
                "position": 0,
                "tags": "NULL",
                "status": "waiting-for-creation",
                "createdAt": None,
                "updatedAt": None
              },
              "CustomAttributeValues": custom_v,
              #"allImages": [],
              "InventoryTypeId": "791a6654-c5f2-11e6-aad6-2c56dc130c0d"
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
          "OfficialStoreId": None,
          "InventoryTypeId": "791a6654-c5f2-11e6-aad6-2c56dc130c0d",
          "InternalCodeTypeId": None,
          "status": "waiting-for-creation"
        })
        headers = {
          'Content-Type': 'application/json',
          'Authorization': f'Bearer {token}'
        }
        
        # Sending request 
        #url = f"https://app.multivende.com/api/products/{p.name[0]}" UPDATE
        url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/products"
        current_app.logger.info("Sending request PUT to update products at Multivende")
        #response = requests.request("PUT", url, headers=headers, data=payload) UPDATE
        response = requests.request("POST", url, headers=headers, data=payload)
        
        # Check there was an error and abort sending data
        #if response.status_code != 201:
         #   current_app.logger.error(f"Aborting sending data for reason: {response.reason}")
        message = response.reason + " " + response.text
        return render_template("create_update/error.html", message=message)
        
        message = p["name"] + " OK"
    
    current_app.logger.info("Data sent without problems")
    return render_template("create_update/success.html")