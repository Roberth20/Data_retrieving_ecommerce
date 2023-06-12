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
print("all good")