import pandas as pd
import json
# Para ejecutar en entorno de app

#from App.models.ids import ids
#from App.extensions.db import db
#import Ap

with open("App/excel_de_respaldo/brands.json") as f:
    data = json.load(f)
    
app = App.create_app()
with app.app_context():
    products = get_products()
    
size = pd.DataFrame(data["sizes"]["entries"])
size = size[["_id", "name"]]
size["type"] = "size"
brand = pd.DataFrame(data["brands"]["entries"])
brand = brand[["_id", "name"]]
brand["type"] = "brand"
color = pd.DataFrame(data["colors"]["entries"])
color = color[["_id", "name"]]
color["type"] = "color"
atributos = pd.DataFrame(data["atributos"])
atributos = atributos[["_id", "name"]]
atributos["type"] = "attribute"
categories = pd.DataFrame(data["categories"])
categories = categories[["_id", "name"]]
categories["type"] = "category"

ids_concat = pd.concat([size, brand, color, atributos, categories]).reset_index(drop=True)

with open("App/excel_de_respaldo/warranties.json") as f:
    data = json.load(f)
    
warranties = pd.DataFrame(data["warranty"]["entries"])
warranties = warranties[["_id", "name"]]
warranties["type"] = "warranty"

tags = pd.DataFrame(data["tags"]["entries"])
tags = tags[["_id", "name"]]
tags["type"] = "tags"

info = pd.concat([warranties, tags]).reset_index(drop=True)

with app.app_context():
    for i, row in info.iterrows():
        item = ids(id=row["_id"], name=row["name"], type=row["type"])
        db.session.add(item)
        
    db.session.commit()
    
    
with open("App/excel_de_respaldo/custom_product-1.json") as f:
    data1 = json.load(f)

with open("App/excel_de_respaldo/custom_product-2.json.json") as f:
    data2 = json.load(f)
    
with open("App/excel_de_respaldo/custom_product-3.json") as f:
    data3 = json.load(f)
    
data_p = data1["custom_p"]+data2["custom_p"]+data3["custom_p"]

with open("App/excel_de_respaldo/custom_product_versions-1.json") as f:
    data4 = json.load(f)
    
with open("App/excel_de_respaldo/custom_product_versions-2.json") as f:
    data5 = json.load(f)
    
data_pv = data4["custom_p"] + data5["custom_p"]

info_p = []
for item in data_p:
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
        if len(ca["CustomAttributeOptions"]) == 0:
            custom_att_p["option_name"] = None
            custom_att_p["option_id"] = None
            info_p.append(custom_att_p)
            continue
        for op in ca["CustomAttributeOptions"]:
            custom_att_op = custom_att_p.copy()
            custom_att_op["option_name"] = op["code"]
            custom_att_op["option_id"] = op["_id"]
            info_p.append(custom_att_op)
            
dfp = pd.DataFrame(info_p)
dfp["scope"] = "products"

info_pv = []
for item in data_pv:
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
        if len(ca["CustomAttributeOptions"]) == 0:
            custom_att_p["option_name"] = None
            custom_att_p["option_id"] = None
            info_pv.append(custom_att_p)
            continue
        for op in ca["CustomAttributeOptions"]:
            custom_att_op = custom_att_p.copy()
            custom_att_op["option_name"] = op["code"]
            custom_att_op["option_id"] = op["_id"]
            info_pv.append(custom_att_op)
            
dfv = pd.DataFrame(info_pv)
dfv["scope"] = "versions"

data = pd.concat([dfv, dfp]).reset_index(drop=True)
with app.app_context():
    from App.models.ids import customs_ids
    for i, row in data.iterrows():
        c_ids = customs_ids(id_set = row["id_set"], name_set = row["name_set"], id = row["id"],
                           name = row["name"], option_name = row["option_name"], option_id = row["option_id"],
                           scope = row["scope"])
        db.session.add(c_ids)
    db.session.commit()

print("all good")