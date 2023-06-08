import os
import time

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

try:
    from tqdm import tqdm
except ImportError:
    print("Instalando: tqdm\n")
    os.system("pip install tqdm")
finally:
    from tqdm import tqdm

print("Conectado con la API.")
# Definimos el url de interes
url = "https://ripley-prod.mirakl.net/api/hierarchies"
# API_Key
api_key = "API KEY"
header = {"Authorization": api_key}

# Preparamos nuestro objeto de almacenamiento
attributes = []

# Obtenemos todas las categorias
print("Obteniendo categorias.")
message = requests.get(url, headers= header)
message = message.json()

print("Cargando atributos... Tiempo aproximado: 4h")
for category in tqdm(message["hierarchies"]):
    # Por cada categoria obtenemos los atributos
    url = f"https://ripley-prod.mirakl.net/api/products/attributes?hierarchy={category['code']}"
    response = requests.get(url, headers = header)
    info = response.json()

    for item in info["attributes"]:
        # Guardamos en el objeto y agregamos a cada atributo la categoria de origen
        item["category"] = category["label"]
        attributes.append(item)
    
    # La API de mirakl tiene un numero maximo de llamadas por minuto.
    # Para evitar errores y perdida de informacion, detenemos el programa 
    # despues de cada ciclo. El tiempo elegido fue determinado manualmente con prueba y error
    #
    #  No se encuentro el limite de llamadas por minuto en la documentacion
    time.sleep(10.01)

# Creamos dataframe y limpiamos
df = pd.DataFrame(attributes)
df.drop(["default_value", "example", "values", "description", "required", "hierarchy_code"], axis=1, inplace=True)
df.drop(["description_translations", "label_translations", "validations", "transformations"], axis=1, inplace=True)
df.drop(["code", "roles"], axis=1, inplace=True)
df.drop(df.index[df["category"] == "NUEVO ÁRBOL"], axis=0, inplace=True)
# Guardamos el dataframe
df.to_excel("atributos_ripley.xlsx", index=False)
print("Proceso finalizado.")