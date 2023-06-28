"""Funciones de utilidad para la actualizacion de datos"""
# Import imporant libraries
import datetime
import requests
import numpy as np

def check_differences_and_upload_maps(df, db_market, db, market):
    """Funcion para actualizacion de los mapeos de atributos.
    
    Cada marketplace mantiene la misma estructura, facilitando su actualizacion.
    
    NOTA: La actualizacion se realiza manualmente, las tecnicas de automatizadas no arrojaron 
    resultados viables para el tiempo disponible.
    
    Input : 
    ---------
      *  df : pandas.DataFrame. Tablas de datos con el mapeo subido por el usuario.
      
      *  db_market : pandas.DataFrame. Tabla de datos con el mapeo guardado en la base de datos.
      
      *  db : flask_sqlalchemy.SQLAlchemy. Instancia representativa de la base de datos.
      
      *  market : SQLAlchemy.Model. Objeto con el metadata de la tabla correspondiente al 
      marketplace. Ver App/models/*.py para mas detalle sobre los modelos definidos.
      
    Output :
    ---------
      * None.
    """
    # Check differences
    diff = df[~df.isin(db_market)].dropna(how="all")
    for i, row in diff.iterrows():
        # If the difference is a change in a column, update it
        if row.isna().any():
            ID = db_market[db_market.index == i].iloc[0, 0]
            mapeo = db.session.get(market, ID)
            if row[row.notna()].index[0] == "Mapeo":
                mapeo.Mapeo = row[row.notna()][0]
            else:
                mapeo.Atributo = row[row.notna()][0]
            db.session.commit()
        # Add the new row to the map
        else:
            mapeo = market(Mapeo=row.Mapeo, Atributo=row.Atributo)
            db.session.add(mapeo)
            db.session.commit()

def check_differences_and_upload_cats(df, db_cat, db, model):
    """Funcion para actualizacion de los mapeos de categorias.
    
    NOTA: La actualizacion se realiza manualmente, las tecnicas de automatizadas no arrojaron 
    resultados viables para el tiempo disponible.
    
    Input : 
    ---------
      *  df : pandas.DataFrame. Tablas de datos con el mapeo subido por el usuario.
      
      *  db_cat : pandas.DataFrame. Tabla de datos con el mapeo guardado en la base de datos.
      
      *  db : flask_sqlalchemy.SQLAlchemy. Instancia representativa de la base de datos.
      
      *  model : SQLAlchemy.Model. Objeto con el metadata de la tabla que guarda los datos de
      mapeo. Ver App/models/*.py para mas detalle sobre los modelos definidos.
      
    Output :
    ---------
      * None.
    """
    # Check differences
    diff = df[~df.isin(db_cat)].dropna(how="all")
    for i, row in diff.iterrows():
        # If the difference is in A COLUMN, update IT
        if row.isna().any():
            ID = db_cat[db_cat.index == i].iloc[0, 0]
            mapeo = db.session.get(model, ID)
            if row[row.notna()].index[0] == 'Categoria Multivende':
                mapeo.Multivende = row[row.notna()][0]
            elif row[row.notna()].index[0] == 'Categoria Mercadolibre':
                mapeo.MercadoLibre = row[row.notna()][0]
            elif row[row.notna()].index[0] == 'Categoria Falabella':
                mapeo.Falabella = row[row.notna()][0]
            elif row[row.notna()].index[0] == 'Categoria Ripley ':
                mapeo.Ripley = row[row.notna()][0]
            elif row[row.notna()].index[0] == 'Categoria Paris':
                mapeo.Paris = row[row.notna()][0]
            else:
                mapeo.Paris_Familia = row[row.notna()][0]
            db.session.commit()
        # Add rows that don't exist in the DB
        else:
            mapeo = model(Multivende=row['Categoria Multivende'], 
                        MercadoLibre=row['Categoria Mercadolibre'],
                        Falabella=row['Categoria Falabella'],
                        Ripley = row['Categoria Ripley '],
                        Paris = row['Categoria Paris'],
                        Paris_Familia = row['Paris Familia'])
            db.session.add(mapeo)
            db.session.commit()
            
def check_difference_and_update_checkouts(data, checkouts, db):
    """Funcion para actualizacion de ventas/checkouts.
    
    Input : 
    ---------
      *  data : pandas.DataFrame. Tablas de datos con los checkouts a actualizar.
      
      *  checkouts : SQLAlchemy.Model. Objeto con el metadata de la tabla correspondiente a las 
      ventas. Ver App/models/*.py para mas detalle sobre los modelos definidos..
      
      *  db : flask_sqlalchemy.SQLAlchemy. Instancia representativa de la base de datos. 
      
    Output :
    ---------
      * None.
    """
    # Check if the checkout is in the DB
    for i, row in data.iterrows():
        result = db.session.scalar(db.select(checkouts).where(checkouts.id_venta == row["id"] and 
                                                          checkouts.id_hijo_producto == row["id hijo producto"]))
        # Add the new checkout to the DB
        if result == None:
            venta = checkouts(cantidad = row["cantidad"], codigo_producto = ["codigo producto"],
                         costo_envio = row["costo de envio"], estado_boleta = row["estado boleta"],
                         estado_entrega = row["estado entrega"], estado_venta = row["estado venta"],
                         fecha = row["fecha"], id_venta = row["id"], id_hijo_producto = row["id hijo producto"],
                         id_padre_producto = row["id padre producto"], mail = row["mail"], 
                         market = row["market"], n_venta = row["n venta"], 
                         nombre_cliente = row["nombre"], nombre_producto = row["nombre producto"],
                         phone = row["phone"], precio = row["precio"],
                         url_boleta = row["url boleta"])
            db.session.add(venta)
        # Update the old values
        else:
            stmt = (
                db.update(checkouts)
                .where(checkouts.id_venta == row["id"] and 
                       checkouts.id_hijo_producto == row["id hijo producto"])
                .values(cantidad = row["cantidad"], codigo_producto = ["codigo producto"],
                         costo_envio = row["costo de envio"], estado_boleta = row["estado boleta"],
                         estado_entrega = row["estado entrega"], estado_venta = row["estado venta"],
                         fecha = row["fecha"], id_venta = row["id"], id_hijo_producto = row["id hijo producto"],
                         id_padre_producto = row["id padre producto"], mail = row["mail"], 
                         market = row["market"], n_venta = row["n venta"], 
                         nombre_cliente = row["nombre"], nombre_producto = row["nombre producto"],
                         phone = row["phone"], precio = row["precio"],
                         url_boleta = row["url boleta"])
            )
            db.session.execute(stmt)
    db.session.commit()
            
def get_data_falabella(userId, key):
    """Funcion para recopilacion de datos sobre clientes.
    
    *** De los checkouts se puede obtener informacion similar, esta se obtiene directo de
    Falabella ***
    
    Input : 
    ---------
      *  userId : str. Identificacion del vendedor en Falabella.
      
      *  key : str. Api key para acceso a falabella.
      
    Return :
    ---------
      *  pandas.DataFrame con los datos de los clientes (determinados por el contratista)
    """
    # Import useful functions
    from App.get_data.Retrieve_data_Falabella import get_response_falabella
    import pandas as pd
    # Set parameters
    parameters = {
      'UserID': userId,
      'Version': '1.0',
      'Action': "GetOrders",
      'Format':'JSON',
      'Timestamp': datetime.datetime.now().isoformat()}
    
    # Retrieve data
    info = get_response_falabella(parameters, key)
    try:
        data = info.json()
    except:
        return info.text
    orders = data["SuccessResponse"]["Body"]["Orders"]["Order"]
    customers = []
    # Get clients data
    for order in orders:
        tmp = {}
        tmp["Name"] = order["CustomerFirstName"]+" "+order["CustomerLastName"]
        tmp["Mail"] = order["AddressBilling"]["CustomerEmail"]
        if order["AddressBilling"]["CustomerEmail"] == "":
            tmp["Mail"] = order["AddressShipping"]["CustomerEmail"]        

        tmp["Phone"] = order["AddressBilling"]["Phone"]
        if order["AddressBilling"]["Phone"] == "":
            tmp["Phone"] = order["AddressShipping"]["Phone"]
        
        # Set parameter to retrieve items from the checkout
        parameters = {
          'UserID': userId,
          'Version': '1.0',
          'Action': "GetOrderItems",
          'Format':'JSON',
          'Timestamp': datetime.datetime.now().isoformat(),
          "OrderId": order["OrderId"]
        }
        response = get_response_falabella(parameters, key)
        try:
            response = response.json()
        except:
            return response.text
        response = response["SuccessResponse"]["Body"]["OrderItems"]["OrderItem"]
        if type(response) == list:
            tmp["Items"] = ", ".join(set([item["Name"] for item in response]))
            if tmp["Mail"] == "":
                tmp["Mail"] = response[0]["DigitalDeliveryInfo"]
        else:
            tmp["Items"] = response["Name"]
            if tmp["Mail"] == "":
                tmp["Mail"] = response["DigitalDeliveryInfo"]


        customers.append(tmp)
    
    return pd.DataFrame(customers)
    
def get_data_paris(key):
    """Funcion para recopilacion de datos sobre clientes.
    
    *** De los checkouts se puede obtener informacion similar, esta se obtiene directo de
    Paris ***
    
    Input : 
    ---------
      *  key : str. Api key para acceso a Paris.
      
    Return :
    ---------
      *  pandas.DataFrame con los datos de los clientes (determinados por el contratista)
    """
    import pandas as pd
    # Authenticate connection
    base_url = "https://api-developers.ecomm.cencosud.com/"

    headers = {"Content-Type": "application/json",
               "Authorization":f"Bearer {key}"}
    
    message = requests.post(base_url+"/v1/auth/apiKey", headers=headers)
    try:
        message = message.json()
    except:
        return message.text

    # Guardamos el token en una variable para facilitar su acceso.
    token = message["accessToken"]
    # Obtenemos las ordenes
    headers = {"Content-Type": "application/json",
               "Authorization":f"Bearer {token}"}

    response = requests.get(base_url+"/v1/orders", headers=headers)
    try:
        response = response.json()
    except:
        return response.text
    customers = []
    for order in response["data"]:
        tmp = {}
        tmp["Name"] = order["customer"]["name"]
        tmp["Mail"] = order["customer"]["email"]
        tmp["Phone"] = order["billingAddress"]["phone"]
        items = []
        for suborder in order["subOrders"]:
            for item in suborder["items"]:
                items.append(item["name"])

        tmp["Items"] = ", ".join(set(items))
        customers.append(tmp)
        
    return pd.DataFrame(customers)

def get_data_ripley(key):
    """Funcion para recopilacion de datos sobre clientes.
    
    *** De los checkouts se puede obtener informacion similar, esta se obtiene directo de
    Ripley ***
    
    Input : 
    ---------
      *  key : str. Api key para acceso a Ripley.
      
    Return :
    ---------
      *  pandas.DataFrame con los datos de los clientes (determinados por el contratista)
    """
    import pandas as pd
    # Definimos el url de interes
    url = "https://ripley-prod.mirakl.net/api/orders"
    header = {"Authorization": key}
    # Retrieve data
    message = requests.get(url, headers= header)
    try:
        message = message.json()
    except:
        message.text
    customers = []
    for order in  message["orders"]:
        tmp = {}
        tmp["Name"] = order["customer"]["firstname"] + " " + order["customer"]["lastname"]
        tmp["Phone"] = order["customer"]["billing_address"]["phone_secondary"]
        tmp["Items"] = ", ".join(set([product["product_title"] for product in order["order_lines"]]))
        tmp["Mail"] = order["customer_notification_email"]
        customers.append(tmp)
        
    return pd.DataFrame(customers)