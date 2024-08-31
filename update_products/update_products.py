import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import os
import sys
import json
import requests
from models import auth_app, Product, Attributes
from datetime import datetime, timezone
from utils import *
from time import sleep

try:
    import config
except:
    import test as config

# Setting up logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s: %(message)s', stream=sys.stdout,
                    level=logging.INFO)

# Making engine
engine = create_engine(config.SQLALCHEMY_DATABASE_URI,
                       connect_args={
                            "ssl": {
                                "ca":config.ssl
                                }     
                            }
                       )

# Get data from tables
logger.info('Retrieving data from db.')
with Session(engine) as session:
    last_auth = session.scalar(select(auth_app).order_by(auth_app.expire.desc()))

if last_auth == None:
    logger.error("Failed authentication")
    sys.exit(0)
        
diff = datetime.now(timezone.utc) - last_auth.expire.replace(tzinfo=timezone.utc)
# The token expired
if diff.total_seconds()/3600 > 6:
    logger.warning('Refresh token expired.')
    sys.exit(0)

# Decrypt token
token = decrypt(last_auth.token, config.SECRET_KEY)

# Get products data
logger.info('Recolectando datos de atributos')
merchant_id = config.MERCHANT_ID
url = f"https://app.multivende.com/api/m/{merchant_id}/all-product-attributes"
headers = {
        'Authorization': f'Bearer {token}'
}
# Get data
response = requests.request("GET", url, headers=headers)
    
try:
    response = response.json()
except Exception as e:
    logger.error(f"Hubo un error {e}: "+response.text)
    sys.exit(0)

# Obtenemos dos grupos de atributos, lo separamos
att = response["customAttributes"]
att_std = ["Season", "model", "description", "htmlDescription", "shortDescription",
            "htmlShortDescription", "Warranty", "Brand", "name", "ProductCategory", "sku_name", "color",
            "size", "sku", "internalSku", "width", "length", "height", "weight", "IDENTIFICADOR_PADRE", 
            "IDENTIFICADOR_HIJO", "tags", "picture url"]
# Transformamos los nombres para mayor comodidad
att_names = [item["name"]+"-"+item["CustomAttributeSet.name"] for item in att]
    
# Obtenemos la lista de todos los productos
logger.info("Solicitando ids de productos")
url = f"https://app.multivende.com/api/m/{merchant_id}/products/light/p/1"
response = requests.request("GET", url, headers=headers).json()
data = response["entries"]
pages = response["pagination"]["total_pages"]
# Los productos se organizan en paginas, pasamos por todas, guardando los resultados
for p in range(pages-1):
    url = f"https://app.multivende.com/api/m/{merchant_id}/products/light/p/{p+2}"
    response = requests.request("GET", url, headers=headers).json()
    data += response["entries"]

# Extraemos los id de cada uno
ids = [item["_id"] for item in data]

# Para cada producto, guardamos sus atributos
logger.debug("Solicitando atributos de productos por ids.")
data = []
for i in ids:
    url = f"https://app.multivende.com/api/products/{i}?_include_product_picture=true"
    response = requests.request("GET", url, headers=headers).json()
    data.append(response)

# Dentro de los atributos normales, extraemos los atributos hechos por el usuario
logger.debug("Procesando atributos de productos")
customs = []
for d in data:
    tmp_dict = {}
    for at in d["CustomAttributeValues"]:
        name = at["CustomAttribute"]['name']+"-"+at["CustomAttribute"]['CustomAttributeSet']['name']
        tmp_dict[name] =  at["text"]

    tmp_dict["tags"] = []
    for tag in d["ProductTags"]:
        tmp_dict["tags"].append(tag["Tag"]["name"])

    customs.append(tmp_dict)
    d.pop("CustomAttributeValues")

# Unimos cada conjunto de atributos con sus custom atributos
info = []
for i in range(len(data)):
    info.append(data[i] | customs[i])

# Hay atributos estandar que tienen informacion anidada, extraemos la misma
logger.debug("Procesando atributos estandar")
all_data = []
for i in info:
    sku = i["code"]
    i["sku_name"] = sku
    i["IDENTIFICADOR_PADRE"] = i["_id"]
    # Cuando el atributo no tiene valor, no posee informacion anidada
    # evitamos errores tomando este aspecto en consideracion
    try:
        brand = i["Brand"]["name"]
    except:
        brand = None
    i["Brand"] = brand
    try:
        cat = i["ProductCategory"]["name"]
    except:
        cat = None
    i["ProductCategory"] = cat
    try:
        war = i["Warranty"]["name"]
    except:
        war = None
    i["Warranty"] = war
    try:
        tags = []
        for t in i["ProductTags"]:
            tags.append(t["Tag"]["name"])
    except:
        tags = None
    i["tags"] = tags
    picture = None
    if len(i['ProductPictures']) == 0:
        picture = None
    try:
        for p in i["ProductPictures"]:
            if p['originalFileName'][-5:] == '1.jpg' or p['originalFileName'][-5:] == '1.png':
                picture = p['url']
                break
    except:
        picture = None
    i["picture url"] = picture
    # Extraemos la misma informacion para cada version de producto
    for pv in i["ProductVersions"]:
        j = i.copy()
        tmp_dict = {}
        if 'CustomAttributeValues' in pv.keys():
            for at in pv["CustomAttributeValues"]:
                name = at["CustomAttribute"]['name']+"-"+at["CustomAttribute"]['CustomAttributeSet']['name']
                tmp_dict[name] =  at["text"]
            pv.pop("CustomAttributeValues")
        j = j | tmp_dict
        j["color"] = pv["Color"]["name"]
        j["size"] = pv["Size"]["name"]
        j["sku"] = pv["code"]
        j["internalSku"] = pv["internalCode"]
        j["height"] = pv["height"]
        j["length"] = pv["length"]
        j["weight"] = pv["weight"]
        j["width"] = pv["width"]
        j["IDENTIFICADOR_HIJO"] = pv["_id"]
        j.pop("ProductVersions")
        all_data.append(j)

logger.info('Procesando datos.')
# Generamos la tabla de datos
df = pd.DataFrame(all_data, columns = att_std + att_names)

# Limpiamos atributos de informacion que no es relavante o complementaria
# de la base de datos de multivende
markets = []
for market in ["HB", "Falabella", "Ripley", "Paris", "Shopify"]:
    for c in df.columns:
        if market in c:
            markets.append(c)

basics = [d for d in df.columns if d not in markets]
df = df[basics+markets]
    
# Limpiamos columnas duplicadas
df.drop(columns = df.columns[df.columns.duplicated()], inplace =True)
for i in range(df.shape[0]):
    df.loc[i, "tags"] = ";".join(df.loc[i, "tags"])

logger.info('Obteniendo precios y stock')
# Obtenemos las listas de precios
url = f'https://app.multivende.com/api/m/{config.MERCHANT_ID}/product-price-lists'
headers = {
        'Authorization': f'Bearer {token}'
}
price_lists = requests.request("GET", url, headers=headers).json()

# Obtenemos las bodegas
url = f'https://app.multivende.com/api/m/{config.MERCHANT_ID}/stores-and-warehouses'
warehouses = requests.request("GET", url, headers=headers).json()

# Preparamos contenedores de datos
stocks = []
prices = pd.DataFrame(columns = [''.join(p['name'].split(' ')) + extra for extra in ['', 'WithDiscount'] for p in price_lists['entries']])

for i, row in df.iterrows():
    # Para cada producto obtenemos
    for ware in warehouses['entries']:
        url = f'https://app.multivende.com/api/product-stocks/stores-and-warehouses/{ware["_id"]}/limit/1000?_code={row["IDENTIFICADOR_HIJO"]}'
        headers = {
                'Authorization': f'Bearer {token}'
        }
        product_stock = requests.request("GET", url, headers=headers).json()
        stocks.append(product_stock['entries'][0]['ProductStocks']['amount'])
        
    for list in price_lists['entries']:
        url = f'https://app.multivende.com/api/product-price/product-price-lists/{list["_id"]}/limit/1000?_code={row["IDENTIFICADOR_HIJO"]}'
        product_prices = requests.request("GET", url, headers=headers).json()
        prices.loc[i, ''.join(list['name'].split(' '))] = product_prices['entries'][0]['ProductPrices']['gross']
        prices.loc[i, ''.join(list['name'].split(' ')) + 'WithDiscount'] = product_prices['entries'][0]['ProductPrices']['priceWithDiscount']
    
    # Waiting time to avoid over-crowding connection    
    sleep(0.01)

# Se agregan a la tabla
df['Stock'] = stocks
df = pd.concat([df, prices], axis=1)

logger.info('Cargando a la base de datos.')
upload_data_products(df, Product, Attributes, engine)
