from sqlalchemy.orm import Session
from sqlalchemy import select, update
from cryptography.fernet import Fernet
import pandas as pd
import numpy as np
import logging
import sys
import requests

# Setting up logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s: %(message)s', stream=sys.stdout,
                    level=logging.INFO)

def encrypt(data, key):
    f = Fernet(key.encode())
    encoded = data.encode()
    return f.encrypt(encoded)

def decrypt(encrypted, key):
    f = Fernet(key.encode())
    decrypted = f.decrypt(encrypted)
    return decrypted.decode()

def get_data_brands(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/brands/p/1"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except Exception as e:
        logger.error(f"Error: {response.text}")
        sys.exit()
        
    brands = pd.DataFrame(response["entries"])
    brands["type"] = "brand"
    brands = brands[["_id", "name", "type"]]
    return brands

def get_data_warranties(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/warranties"    
    headers = {
            'Authorization': f'Bearer {token}'
        }

    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
        
    warranties = pd.DataFrame(response["entries"])
    warranties["type"] = "warranty"
    warranties = warranties[["_id", "name", "type"]]
    return warranties

def get_data_tags(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/tags/p/1"    
    headers = {
            'Authorization': f'Bearer {token}'
        }
    
    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
        
    if response['pagination']['total_pages'] > 1:
        data = []
        for i in range(response['pagination']['total_pages']):
            url = f"https://app.multivende.com/api/m/{merchant_id}/tags/p/{i+1}"
            response2 = requests.request("GET", url, headers=headers).json()
            data += response2['entries']
    else:
        data = response['entries']
        
    tags = pd.DataFrame(data)
        
    tags = pd.DataFrame(response["entries"])
    tags["type"] = "tag"
    tags = tags[["_id", "name", "type"]]
    return tags

def get_data_colors(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/colors/p/1"    
    headers = {
            'Authorization': f'Bearer {token}'
        }

    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
        
    colors = pd.DataFrame(response["entries"])
    colors["type"] = "color"
    colors = colors[["_id", "name", "type"]]
    return colors

def get_data_categories(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/product-categories/p/1"    
    headers = {
            'Authorization': f'Bearer {token}'
        }
    
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
    
    pages = response["pagination"]["total_pages"]
    data = []
    for p in range(pages):
        url = f"https://app.multivende.com/api/m/{merchant_id}/product-categories/p/{p+1}"
        headers = {
                'Authorization': f'Bearer {token}'
            }
        response = requests.request("GET", url, headers=headers)
        
        try:
            response = response.json()
        except:
            logger.error(f"Error: {response.text}")
            sys.exit()
        
        df = pd.DataFrame(response["entries"])
        data.append(df)
        
    cats = pd.concat(data, ignore_index=True)
    cats["type"] = "category"
    cats = cats[["_id", "name", "type"]]
    return cats

def get_data_size(token, merchant_id):
    url = f"https://app.multivende.com/api/m/{merchant_id}/sizes/p/1"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    response = requests.request("GET", url, headers=headers)
    
    try:
        response = response.json()
    except:
        logger.error(f"Error: {response.text}")
        sys.exit()
    
    size = pd.DataFrame(response["entries"])
    size["type"] = "size"
    size = size[["_id", "name", "type"]]
    return size

def get_customs_attributes(token, merchant_id):
    url1 = f"https://app.multivende.com/api/m/{merchant_id}/custom-attribute-sets/products"
    url2 = f"https://app.multivende.com/api/m/{merchant_id}/custom-attribute-sets/product_versions"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    response1 = requests.request("GET", url1, headers=headers)
    response2 = requests.request("GET", url2, headers=headers)
    try:
        response1 = response1.json()
    except:
        logger.error(f"Error: {response1.text}")
        sys.exit()
    try:
        response2 = response2.json()
    except:
        logger.error(f"Error: {response2.text}")
        sys.exit()

    logger.info('Procesando atributos de productos.')
    info_p = []
    for item in response1["entries"]:
        custom_att = {}
        custom_att["id_set"] = item["_id"]
        custom_att["name_set"] = item["name"]
        if len(item["CustomAttributes"]) == 0:
            custom_att["id"] = None
            custom_att["name"] = None
            custom_att["option_name"] = None
            custom_att["option_id"] = None
            info_p.append(custom_att)
            continue
        for ca in item["CustomAttributes"]:
            custom_att_p = custom_att.copy()
            custom_att_p["id"] = ca["_id"]
            custom_att_p["name"] = ca["name"]
            if ca["CustomAttributeType"]['_id'] != '763c2831-b9af-462f-8974-d401f358949c':
                custom_att_p["option_name"] = None
                custom_att_p["option_id"] = None
                info_p.append(custom_att_p)
                continue
            for op in ca["CustomAttributeOptions"]:
                custom_att_op = custom_att_p.copy()
                custom_att_op["option_name"] = op["text"]
                custom_att_op["option_id"] = op["_id"]
                info_p.append(custom_att_op)
                
                
    dfp = pd.DataFrame(info_p)

    logger.info('Procesando atributos para versiones de productos.')
    info_pv = []
    for item in response2["entries"]:
        custom_att = {}
        custom_att["id_set"] = item["_id"]
        custom_att["name_set"] = item["name"]
        if len(item["CustomAttributes"]) == 0:
            custom_att["id"] = None
            custom_att["name"] = None
            custom_att["option_name"] = None
            custom_att["option_id"] = None
            info_pv.append(custom_att)
            continue
        for ca in item["CustomAttributes"]:
            custom_att_p = custom_att.copy()
            custom_att_p["id"] = ca["_id"]
            custom_att_p["name"] = ca["name"]
            if ca["CustomAttributeType"]['_id'] != '763c2831-b9af-462f-8974-d401f358949c':
                custom_att_p["option_name"] = None
                custom_att_p["option_id"] = None
                info_pv.append(custom_att_p)
                continue
            for op in ca["CustomAttributeOptions"]:
                custom_att_op = custom_att_p.copy()
                custom_att_op["option_name"] = op["text"]
                custom_att_op["option_id"] = op["_id"]
                info_pv.append(custom_att_op)
                
    dfv = pd.DataFrame(info_pv)
    
    data = pd.concat([dfv, dfp], ignore_index=True)
    return data

def upload_data_products(df, Product, Attributes, engine):    
    # This two attribute columns are duplicated, remove one
    try:
        df.drop(df.columns[df.columns.str.contains("Material del trípode")][1], axis=1, inplace=True)
    except:
        logger.warning("The column: Material del trípode. NO se encuentra en el dataframe")
        
    try:
        df.drop("Número de focos-Ripley Productos", axis=1, inplace=True)
    except:
        logger.warning("The column: Número de focos-Ripley Productos. NO se encuentra en el dataframe")

    # Split dataframe by marketplaces and upload data to each one
    logger.info('Subiendo a la DB.')
    df = df.replace({np.nan: None})
    try:
        with Session(engine) as session:
            for id, row in df.iterrows():
                # Get product data if exists
                result = session.scalar(select(Product).where(Product.id_padre == row['IDENTIFICADOR_PADRE'] and 
                                                              Product.id_hijo == row['IDENTIFICADOR_HIJO']))
                # For each attribute of product check if key/value is in DB
                atts = row[23:-13]
                attributes = []
                for i in atts[atts.notna()].index:
                    # If is number create the correct object
                    if atts[i].replace('.', '').isdigit():
                        attribute = Attributes(name = i, number_value = float(atts[i]))
                        # Check if it's in DB
                        exists_criteria = (select(Attributes.id)
                                           .where(Attributes.name == attribute.name and 
                                                  Attributes.number_value == attribute.number_value)
                                           .exists())
                        exists_att = session.scalar(select(exists_criteria)) 
                        # Add the item to temporal list of attributes of the product
                        attributes.append(attribute)
                    else:
                        attribute = Attributes(name = i, text_value = atts[i])
                        # Check if it's in DB
                        exists_criteria = (select(Attributes.id)
                                           .where(Attributes.name == attribute.name and 
                                                  Attributes.text_value == attribute.text_value)
                                           .exists())
                        exists_att = session.scalar(select(exists_criteria))
                        # Add to the list of the product
                        attributes.append(attribute)
                    if exists_att:
                        # Add the key/value attribute if it's not in DB
                        session.add(attribute)
                if result == None:
                    # If new product, add it to DB
                    new_product = Product(id_padre = row['IDENTIFICADOR_PADRE'], 
                                          id_hijo= row['IDENTIFICADOR_HIJO'],
                                          season = row['Season'],
                                          model = row['model'],
                                          description = row['description'],
                                          htmlDescription = row['htmlDescription'],
                                          shortDescription = row['shortDescription'],
                                          htmlShortDescription = row['htmlShortDescription'],
                                          warranty = row['Warranty'],
                                          brand = row['Brand'],
                                          name = row['name'],
                                          productCategory = row['ProductCategory'],
                                          skuName = row['sku_name'],
                                          color = row['color'],
                                          size = row['size'],
                                          sku = row['sku'],
                                          internalSku = row['internalSku'],
                                          width = row['width'],
                                          length = row['length'],
                                          height = row['height'],
                                          weight = row['weight'],
                                          tags = row['tags'],
                                          picture = row['picture url'],
                                          stock = row['Stock'],
                                          PreciosHites = row['PreciosHites'], 
                                          Preciosdelista = row['Preciosdelista'],
                                          PreciosRipley = row['PreciosRipley'], 
                                          PreciosParis = row['PreciosParis'], 
                                          PreciosFalabella = row['PreciosFalabella'], 
                                          PreciosShopify = row['PreciosShopify'],
                                          PreciosHitesWithDiscount = row['PreciosHitesWithDiscount'],
                                          PreciosdelistaWithDiscount = row['PreciosdelistaWithDiscount'],
                                          PreciosRipleyWithDiscount = row['PreciosRipleyWithDiscount'],
                                          PreciosParisWithDiscount = row['PreciosParisWithDiscount'],
                                          PreciosFalabellaWithDiscount = row['PreciosFalabellaWithDiscount'],
                                          PreciosShopifyWithDiscount = row['PreciosShopifyWithDiscount'],
                                          CBarra01 = row['Codigo_de_barra_01-Shopify Productos'],
                                          CBarra02 = row['Codigo_de_barra_02-Shopify Productos'],
                                          CBarra03 = row['Codigo_de_barra_03-Shopify Productos'],
                                          CBarra04 = row['Codigo_de_barra_04-Shopify Productos'],
                                          CBarra05 = row['Codigo_de_barra_05-Shopify Productos'],
                                          CBarra06 = row['Codigo_de_barra_06-Shopify Productos'],
                                          Costo01 = row['Costo_01-Shopify Productos'])
                    for attribute in attributes:
                        # Associate the attributes objects to the new product
                        new_product.attributes.append(attribute)
                    session.add(new_product)
                else:
                    # If the product exists, reset attributes links
                    for i in range(len(result.attributes)):
                        result.attributes.pop()
                    # And a new ones (update)
                    for attribute in attributes:
                        result.attributes.append(attribute)
                    # Update product info
                    stmt = (update(Product)
                                 .where(Product.id_padre == row['IDENTIFICADOR_PADRE'] and 
                                        Product.id_hijo == row['IDENTIFICADOR_HIJO'])
                                 .values(season = row['Season'],
                                         model = row['model'],
                                         description = row['description'],
                                         htmlDescription = row['htmlDescription'],
                                         shortDescription = row['shortDescription'],
                                         htmlShortDescription = row['htmlShortDescription'],
                                         warranty = row['Warranty'],
                                         brand = row['Brand'],
                                         name = row['name'],
                                         productCategory = row['ProductCategory'],
                                         skuName = row['sku_name'],
                                         color = row['color'],
                                         size = row['size'],
                                         sku = row['sku'],
                                         internalSku = row['internalSku'],
                                         width = row['width'],
                                         length = row['length'],
                                         height = row['height'],
                                         weight = row['weight'],
                                         tags = row['tags'],
                                         picture = row['picture url'],
                                         stock = row['Stock'],
                                         PreciosHites = row['PreciosHites'], 
                                         Preciosdelista = row['Preciosdelista'],
                                         PreciosRipley = row['PreciosRipley'], 
                                         PreciosParis = row['PreciosParis'], 
                                         PreciosFalabella = row['PreciosFalabella'], 
                                         PreciosShopify = row['PreciosShopify'],
                                         PreciosHitesWithDiscount = row['PreciosHitesWithDiscount'],
                                         PreciosdelistaWithDiscount = row['PreciosdelistaWithDiscount'],
                                         PreciosRipleyWithDiscount = row['PreciosRipleyWithDiscount'],
                                         PreciosParisWithDiscount = row['PreciosParisWithDiscount'],
                                         PreciosFalabellaWithDiscount = row['PreciosFalabellaWithDiscount'],
                                         PreciosShopifyWithDiscount = row['PreciosShopifyWithDiscount'],
                                         CBarra01 = row['Codigo_de_barra_01-Shopify Productos'],
                                         CBarra02 = row['Codigo_de_barra_02-Shopify Productos'],
                                         CBarra03 = row['Codigo_de_barra_03-Shopify Productos'],
                                         CBarra04 = row['Codigo_de_barra_04-Shopify Productos'],
                                         CBarra05 = row['Codigo_de_barra_05-Shopify Productos'],
                                         CBarra06 = row['Codigo_de_barra_06-Shopify Productos'],
                                         Costo01 = row['Costo_01-Shopify Productos']))
                    session.execute(stmt)

            session.commit()
        logger.info(f"Tabla 'Productos' populada con exito.")
    except Exception as e:
        logger.error(f"La tabla 'Productos' tuvo un error {e}")
        sys.exit(0)

def check_diferences_and_update_deliverys(data, deliverys, engine):
    """Funcion para actualizacion de despachos.
    
    Input : 
    ---------
      *  data : pandas.DataFrame. Tablas de datos con las entregas a actualizar.
      
      *  checkouts : SQLAlchemy.Model. Objeto con el metadata de la tabla correspondiente a las 
      entregas. Ver App/models/*.py para mas detalle sobre los modelos definidos..
      
      *  engine : SQLAlchemy.Engine. Instancia representativa de la base de datos. 
      
    Output :
    ---------
      * None.
    """
    # Check if the delivery is in the DB
    with Session(engine) as session:
        for i, row in data.iterrows():
            result = session.scalar(select(deliverys).where(deliverys.id_venta == row["id venta"] and 
                                                              deliverys.n_venta == row["n venta"]))
            # Add the new delivery to the DB
            if result == None:
                delivery = deliverys(n_seguimiento = row["N seguimiento"], codigo = row["codigo"],
                             codigo_venta = row["codigo venta"], courier = row["courier"],
                             delivery_status = row["delivery status"], direccion = row["direccion"],
                             impresion_etiqueta = row["estado impresion etiqueta"], fecha_despacho = row["fecha despacho"],
                             fecha_promesa = row["fecha promesa"], id_venta = row["id venta"], 
                             status_etiqueta = row["status etiqueta"], n_venta = row["n venta"])
                session.add(delivery)
            # update old values                         
            else:
                stmt = (
                    update(deliverys)
                    .where(deliverys.id_venta == row["id venta"] and 
                           deliverys.n_venta == row["n venta"])
                    .values(n_seguimiento = row["N seguimiento"], codigo = row["codigo"],
                             codigo_venta = row["codigo venta"], courier = row["courier"],
                             delivery_status = row["delivery status"], direccion = row["direccion"],
                             impresion_etiqueta = row["estado impresion etiqueta"], fecha_despacho = row["fecha despacho"],
                             fecha_promesa = row["fecha promesa"], id_venta = row["id venta"], 
                             status_etiqueta = row["status etiqueta"], n_venta = row["n venta"])
                )
                session.execute(stmt)            
        
        session.commit()



def check_difference_and_update_checkouts(data, checkouts, engine):
    """Funcion para actualizacion de ventas/checkouts.
    
    Input : 
    ---------
      *  data : pandas.DataFrame. Tablas de datos con los checkouts a actualizar.
      
      *  checkouts : SQLAlchemy.Model. Objeto con el metadata de la tabla correspondiente a las 
      ventas. Ver App/models/*.py para mas detalle sobre los modelos definidos..
      
      *  engine : SQLAlchemy.Engine. Instancia representativa de la base de datos. 
      
    Output :
    ---------
      * None.
    """
    # Check if the checkout is in the DB
    with Session(engine) as session:
        for i, row in data.iterrows():
            if row["nombre"] == None:
                continue
            result = session.scalar(select(checkouts).where(checkouts.id_venta == row["id"] and 
                                                              checkouts.id_hijo_producto == row["id hijo producto"]))
            # Add the new checkout to the DB
            if result == None:
                venta = checkouts(cantidad = row["cantidad"], codigo_producto = row["codigo producto"],
                             costo_envio = row["costo de envio"], estado_boleta = row["estado boleta"],
                             estado_entrega = row["estado entrega"], estado_venta = row["estado venta"],
                             fecha = row["fecha"], id_venta = row["id"], id_hijo_producto = row["id hijo producto"],
                             id_padre_producto = row["id padre producto"], mail = row["mail"], 
                             market = row["market"], n_venta = row["n venta"], 
                             nombre_cliente = row["nombre"], nombre_producto = row["nombre producto"],
                             phone = row["phone"], precio = row["precio"],
                             url_boleta = row["url boleta"])
                session.add(venta)
            # Update the old values
            else:
                stmt = (
                    update(checkouts)
                    .where(checkouts.id_venta == row["id"] and 
                           checkouts.id_hijo_producto == row["id hijo producto"])
                    .values(cantidad = row["cantidad"], codigo_producto = row["codigo producto"],
                             costo_envio = row["costo de envio"], estado_boleta = row["estado boleta"],
                             estado_entrega = row["estado entrega"], estado_venta = row["estado venta"],
                             fecha = row["fecha"], id_venta = row["id"], id_hijo_producto = row["id hijo producto"],
                             id_padre_producto = row["id padre producto"], mail = row["mail"], 
                             market = row["market"], n_venta = row["n venta"], 
                             nombre_cliente = row["nombre"], nombre_producto = row["nombre producto"],
                             phone = row["phone"], precio = row["precio"],
                             url_boleta = row["url boleta"])
                )
                session.execute(stmt)
        session.commit()
