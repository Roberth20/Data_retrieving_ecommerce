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
def get_response_falabella(parameters: dict, api_key): 
    """Funcion de ayuda para contruir el endpoint de la API y para la generacion de la
    signature key.
    
    Input : 
    -------
    * parameters : dict, Diccionario con los parametros necesarios para el endpoint deseado.
    
    Return : 
    --------
    * str : sting de formato json con la respuesta."""
    # api_key del usuario
   # api_key = "<API KEY>"
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
print("Conectando con la API.")
#categories = get_response_falabella(parameters).json()
#categories = categories["SuccessResponse"]["Body"]["Categories"]

#with open("cat_falabella.json", "w") as f:
 #   json.dump(categories, f)
    
print("Proceso finalizado")