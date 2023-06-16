import datetime
import requests

def check_differences_and_upload_maps(df, db_market, db, market):
    diff = df[~df.isin(db_market)].dropna(how="all")
    for i, row in diff.iterrows():
        if row.isna().any():
            ID = db_market[db_market.index == i].iloc[0, 0]
            mapeo = db.session.get(market, ID)
            if row[row.notna()].index[0] == "Mapeo":
                mapeo.Mapeo = row[row.notna()][0]
            else:
                mapeo.Atributo = row[row.notna()][0]
            db.session.commit()
        else:
            mapeo = market(Mapeo=row.Mapeo, Atributo=row.Atributo)
            db.session.add(mapeo)
            db.session.commit()

def check_differences_and_upload_cats(df, db_cat, db, model):
    diff = df[~df.isin(db_cat)].dropna(how="all")
    for i, row in diff.iterrows():
        if row.isna().any():
            ID = db_cat[db_cat.index == i].iloc[0, 0]
            mapeo = db.session.get(model, ID)
            if row[row.notna()].index[0] == "Multivende":
                mapeo.Multivende = row[row.notna()][0]
            elif row[row.notna()].index[0] == "MercadoLibre":
                mapeo.MercadoLibre = row[row.notna()][0]
            elif row[row.notna()].index[0] == "Falabella":
                mapeo.Falabella = row[row.notna()][0]
            elif row[row.notna()].index[0] == "Ripley":
                mapeo.Ripley = row[row.notna()][0]
            elif row[row.notna()].index[0] == "Paris":
                mapeo.Paris = row[row.notna()][0]
            else:
                mapeo.Paris_Familia = row[row.notna()][0]
            db.session.commit()
        else:
            mapeo = model(Multivende=row.Multivende, 
                        MercadoLibre=row.MercadoLibre,
                        Falabella=row.Falabella,
                        Ripley = row.Ripley,
                        Paris = row.Paris,
                        Paris_Familia = row.Paris_Familia)
            db.session.add(mapeo)
            db.session.commit()
            
def check_differences_and_upload_products(df, db_products, db, market):
    diff = df[~df.isin(db_market)].dropna(how="all")
    for i, row in diff.iterrows():
        if row.isna().any():
            ID = db_market[db_market.index == i].iloc[0, 0]
            mapeo = db.session.get(market, ID)
            if row[row.notna()].index[0] == "Mapeo":
                mapeo.Mapeo = row[row.notna()][0]
            else:
                mapeo.Atributo = row[row.notna()][0]
            db.session.commit()
        else:
            mapeo = market(Mapeo=row.Mapeo, Atributo=row.Atributo)
            db.session.add(mapeo)
            db.session.commit()
            
def get_data_falabella(userId, key):
    from App.get_data.Retrieve_data_Falabella import get_response_falabella
    import pandas as pd
    parameters = {
      'UserID': userId,
      'Version': '1.0',
      'Action': "GetOrders",
      'Format':'JSON',
      'Timestamp': datetime.datetime.now().isoformat()}
    

    data = get_response_falabella(parameters, key).json()
    orders = data["SuccessResponse"]["Body"]["Orders"]["Order"]
    customers = []
    for order in orders:
        tmp = {}
        tmp["Name"] = order["CustomerFirstName"]+" "+order["CustomerLastName"]
        tmp["Mail"] = order["AddressBilling"]["CustomerEmail"]
        if order["AddressBilling"]["CustomerEmail"] == "":
            tmp["Mail"] = order["AddressShipping"]["CustomerEmail"]        

        tmp["Phone"] = order["AddressBilling"]["Phone"]
        if order["AddressBilling"]["Phone"] == "":
            tmp["Phone"] = order["AddressShipping"]["Phone"]

        parameters = {
          'UserID': userId,
          'Version': '1.0',
          'Action': "GetOrderItems",
          'Format':'JSON',
          'Timestamp': datetime.datetime.now().isoformat(),
          "OrderId": order["OrderId"]
        }
        response = get_response_falabella(parameters, key).json()
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
    import pandas as pd
    base_url = "https://api-developers.ecomm.cencosud.com/"

    headers = {"Content-Type": "application/json",
               "Authorization":f"Bearer {key}"}

    message = requests.post(base_url+"/v1/auth/apiKey", headers=headers).json()

    # Guardamos el token en una variable para facilitar su acceso.
    token = message["accessToken"]
    # Obtenemos las ordenes
    headers = {"Content-Type": "application/json",
               "Authorization":f"Bearer {token}"}

    response = requests.get(base_url+"/v1/orders", headers=headers).json()
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
    import pandas as pd
    # Definimos el url de interes
    url = "https://ripley-prod.mirakl.net/api/orders"
    header = {"Authorization": key}
    message = requests.get(url, headers= header).json()
    customers = []
    for order in  message["orders"]:
        tmp = {}
        tmp["Name"] = order["customer"]["firstname"] + " " + order["customer"]["lastname"]
        tmp["Phone"] = order["customer"]["billing_address"]["phone_secondary"]
        tmp["Items"] = ", ".join(set([product["product_title"] for product in order["order_lines"]]))
        tmp["Mail"] = order["customer_notification_email"]
        customers.append(tmp)
        
    return pd.DataFrame(customers)