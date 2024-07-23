import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import os
import sys
import json
import requests
from datetime import datetime, timedelta
from models import auth_app, deliverys, checkouts
import pandas as pd
import numpy as np
from utils import *

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
    last_date = datetime.utcnow() - timedelta(days=14)
    result = session.scalars(select(checkouts.id_venta).where(checkouts.fecha >= last_date)).all()

if last_auth == None:
    logger.error("Failed authentication")
    sys.exit(0)
        
diff = datetime.utcnow() - last_auth.expire
# The token expired
if diff.total_seconds()/3600 > 6:
    logger.warning('Refresh token expired.')
    sys.exit(0)

# Decrypt token
token = decrypt(last_auth.token, config.SECRET_KEY)

# Get marketplace connections
logger.info('Getting of deliveries from checkouts')
data = []
merchant_id = config.MERCHANT_ID
for id in result:
    url = f"https://app.multivende.com/api/checkouts/{id}"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
        response['DeliveryOrderInCheckouts']  # Check if the json data is correct
    except Exception as e:
        logger.error(f'Error {e}: {response.text}')

    if response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['trackingNumber'] is None:
        continue

    tmp = {}
    tmp['n venta'] = response["CheckoutLink"]["externalOrderNumber"] 
    tmp['fecha promesa'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['promisedDeliveryDate']
    tmp['direccion'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['deliveryAddress']
    tmp['codigo'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['code']
    tmp['courier'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['courierName']
    tmp['fecha despacho'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['handlingDateLimit']
    tmp['delivery status'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['deliveryStatus']
    n_seguimiento = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['trackingNumber'] 
    if len(n_seguimiento) == 21:
        tmp['N seguimiento'] = n_seguimiento[3:-7]
    tmp['status etiqueta'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['shippingLabelStatus']
    tmp['estado impresion etiqueta'] = response['DeliveryOrderInCheckouts'][0]['DeliveryOrder']['shippingLabelPrintStatus']
    tmp['id venta'] = response['_id']
    tmp['codigo venta'] = response['code']
    data.append(tmp)

# Create dataframe and adjust formats
df = pd.DataFrame(data)
df.fillna(np.nan, inplace=True)

df["fecha despacho"] = pd.to_datetime(df["fecha despacho"])
df["fecha despacho"] = df["fecha despacho"].dt.tz_convert(None)
df["fecha promesa"] = pd.to_datetime(df["fecha promesa"])
df["fecha promesa"] = df["fecha promesa"].dt.tz_convert(None)

# Fill empty values with None
df = df.replace({np.NaN: None})
# Clear duplicated
df = df.drop_duplicates()
# Only store the items with n venta (a checkout registered)
df = df[df["n venta"].notna()]
# Fill empty couriers
df['courier'].fillna('Empty', inplace=True)
# Only store the items with N seguimiento and fecha despacho
df = df[df["N seguimiento"].notna()]
df = df[df["fecha despacho"].notna()]

# Check the data and load to database
logger.info('Cargando a la base de datos')
check_diferences_and_update_deliverys(df, deliverys, engine)

logger.info('Trabajo completado.')

    





