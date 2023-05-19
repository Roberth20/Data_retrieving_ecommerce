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
base_url = "https://api-developers.ecomm.cencosud.com/v1/"

api_key = "API_KEY"

headers = {"Content-Type": "application/json",
           "Authorization":f"Bearer {api_key}"}

message = requests.post(base_url+"auth/apiKey", headers=headers).json()

# Guardamos el token en una variable para facilitar su acceso.
token = message["accessToken"]


# Obtenemos las familias de productos
headers = {"Content-Type": "application/json",
           "Authorization":f"Bearer {token}"}

response = requests.get(base_url+"family", headers=headers).json()

# Para cada familia, obtenemos sus atributos
print("Recuperando atributos...")
information = {}
for family in response["data"]:
    # Obtenemos las categorias contenidas en la familia
    url = f"https://api-developers.ecomm.cencosud.com/v1/category/family/{family['name']}" 

    headers = {"Content-Type": "application/json",
               "Authorization":f"Bearer {token}"}

    cats = requests.get(url, headers=headers)
    
    tmp = []
    # Limpiamos y ordenamos las categorias de la familia
    if cats.status_code != 200:
        print(f"La familia {family['name']} no posee subcategorias.")
    else:
        for c in cats.json():
            tmp.append(c["name"])
            
        information[family] = tmp
        
with open("cat_paris.json", "w") as f:
    json.dump(information, f)
