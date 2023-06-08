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
with open("cat_ripley.json", "w") as f:
    json.dump(message, f)