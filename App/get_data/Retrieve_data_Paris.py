import os

### Revisamos instalaciones
try:
    import requests
except ImportError:
    print("Instalando: requests\n")
    os.system("pip install requests")
finally:
    import requests

try:
    import pandas as pd
except ImportError:
    print("Instalando: pandas\n")
    os.system("pip install pandas")
finally:
    import pandas as pd

    
# Primeros nos autenticamos, generando el access token 
# IMPORTANTE: Se esta realizando la conexion en el entorno de produccion
print("Conectando a la API.")
base_url = "https://api-developers.ecomm.cencosud.com/"

api_key = "API-KEY"

headers = {"Content-Type": "application/json",
           "Authorization":f"Bearer {api_key}"}

message = requests.post(base_url+"/v1/auth/apiKey", headers=headers).json()

# Guardamos el token en una variable para facilitar su acceso.
token = message["accessToken"]


# Obtenemos las familias de productos
headers = {"Content-Type": "application/json",
           "Authorization":f"Bearer {token}"}

response = requests.get(base_url+"/v2/families", headers=headers).json()

# Para cada familia, obtenemos sus atributos y categorias
print("Recuperando atributos...")
categories = {}
attributes = {}
for family in response["results"]:
    headers = {"Content-Type": "application/json",
           "Authorization":f"Bearer {token}"}

    category = requests.get(base_url+f"/v2/categories/family/{family['id']}", headers=headers).json()
    categories[family["name"]] = category["results"]
    
    attribute = requests.get(base_url+f"/v2/attributes/family/{family['id']}", headers=headers).json()
    attributes[family["name"]] = attribute

# Ajustamos las categorias
flat_cat = []
for key in categories.keys():
    for d in categories[key]:
        d["family"] = key
        flat_cat.append(d)

# Obtenemos los valores para los atributos que los disponen
flat_att = []
for key in attributes.keys():
    for d in attributes[key]:
        d["family"] = key
        d["status"] = d["attribute"]["status"]
        d["name"] = d["attribute"]["name"]
        d["type"] = d["attribute"]["type"]
        d["position"] = d["attribute"]["position"]
        d["isRequired"] = d["attributeValidation"]["isRequired"]
        d["attributeType"] = d["attributeValidation"]["attributeType"]["name"]
        
        headers = {"Content-Type": "application/json",
           "Authorization":f"Bearer {token}"}

        values = requests.get(base_url+f"/v2/attributes-options/attribute/{d['id']}", headers=headers).json()
        d["values"] = values["results"]
        
        flat_att.append(d)

# Generamos tabla de datos
att_paris = pd.DataFrame(flat_att)
att_paris.drop(["id", "attributeValidation", "attribute", "position"], axis=1, inplace=True)
cat_paris = pd.DataFrame(flat_cat)
cat_paris.drop("id", axis=1, inplace=True)
att_paris.to_excel("atributos_paris.xlsx", index=False)
cat_paris.to_excel("categorias-familias_paris.xlsx", index=False)

print("Proceso finalizado.")