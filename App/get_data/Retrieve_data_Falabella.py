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

import datetime
import urllib
from hashlib import sha256
from hmac import HMAC
import json

# Creamos funcion de ayuda
def get_response_falabella(parameters: dict, API_KEY): 
    """Funcion de ayuda para contruir el endpoint de la API y para la generacion de la
    signature key.
    
    Input : 
    -------
    * parameters : dict, Diccionario con los parametros necesarios para el endpoint deseado.
    
    Return : 
    --------
    * str : sting de formato json con la respuesta."""
    # api_key del usuario
    api_key = API_KEY
    concatenated = urllib.parse.urlencode(sorted(parameters.items()))
    # Creacion de la signature key
    parameters['Signature'] = HMAC(api_key.encode("utf-8"), msg=concatenated.encode("utf-8"), digestmod = sha256).hexdigest()
    url = urllib.parse.urlencode(sorted(parameters.items()))
    base = "https://sellercenter-api.falabella.com?"

    headers = {"User-Agent": f"{parameters['UserID']}/Python/3.9.12"}

    return requests.get(base+url, headers=headers)

# Parametros para el llamado a la API
parameters = {
      'UserID': "<USUARIO AQUI>",
      'Version': '1.0',
      'Action': "GetCategoryTree",
      'Format':'JSON',
      'Timestamp': datetime.datetime.now().isoformat()}

# Retorno de categorias si la signature es aprobada
#print("Conectando con la API.")
#categories = get_response_falabella(parameters).json()
#categories = categories["SuccessResponse"]["Body"]["Categories"]

def category_tree_falabella(categories: list, tree: list):
    """Funcion generadora del arbol de categorias.
    
    Input : 
    -------
    * categories : list, objeto tipo lista que contiene las categorias en forma de 
    diccionario.
    * tree : list, objeto para almacenar los datos obtenidos
    
    Return :
    --------
    None
    """
    for item in categories:
        # Para cada categoria guardamos el id y el nombre.
        tree.append({"Name": item["Name"], "Id" :item["CategoryId"]})
        # En el caso que la misma tenga subcategorias
        if type(item["Children"]) != dict:
            continue
        if type(item["Children"]["Category"]) == dict:
            tree.append({"Name": item["Children"]["Category"]["Name"], "Id": item["Children"]["Category"]["CategoryId"],
                        "GlobalId": item["Children"]['GlobalIdentifier']})
            continue
        # Llamamos la funcion nuevamente
        category_tree_falabella(item["Children"]["Category"], tree)

# Generamos arbol de categorias
#print("Obteniendo categorias...")
3#tree = []
#category_tree_falabella(categories["Category"], tree)

# Preparamos el objeto para almacenar los atributos
#attributes = []
#print("Generando atributos...")
#for d in tree:
    # En cada llamado a la API se debe cambiar el parametro 
    # de la categoria objetivo
 #   parameters = {
  #    'UserID': "ID DE USUARIO",
   #   'Version': '1.0',
    #  'Action': "GetCategoryAttributes",
     # 'Format':'JSON',
      #'Timestamp': datetime.datetime.now().isoformat(),
#      'PrimaryCategory': d["Id"]}
 #   response = get_response_falabella(parameters).json()
  #  response = response["SuccessResponse"]["Body"]["Attribute"]
   # for item in response:
        # Guardamos el atributo, agregando a que categoria pertence
    #    item["Category"] = d["Name"]
     #   attributes.append(item)

# Creamos dataframe
#df = pd.DataFrame(attributes)
# Limpiamos y guardamos en excel
#df.drop(["InputType", "ExampleValue", "MaxLength", "FeedName", "GlobalIdentifier", "Name"], axis=1, inplace=True)
#df.to_excel("atributos_falabella.xlsx", index = False)
#print("Proceso finalizado.")