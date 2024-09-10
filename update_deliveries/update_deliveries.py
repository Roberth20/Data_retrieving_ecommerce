import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import os
import sys
import json
import requests
from datetime import datetime, timedelta, timezone
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
    last_date = datetime.now(timezone.utc) - timedelta(days=14)
    result = session.scalars(select(checkouts.id_venta).where(checkouts.fecha >= last_date)).all()

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

# Get marketplace connections
logger.info('Getting data from marketplaces')
conn_id = []
merchant_id = config.MERCHANT_ID
channels = ['mercadolibre', 'linio', 'dafiti', 'ripley', 'paris', 'fcom']
for c in channels:
    url = f"https://app.multivende.com/api/m/{merchant_id}/{c}-connections"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        logger.warning('Advertencia: ', response.text)
    if len(response['entries']) > 0:
        conn_id.append(response['entries'][0]['_id'])

# Get deliveries data
logger.info('Retrieving delivery data')
raw = []
for con_id in conn_id:
    p = 1
    url = f"https://app.multivende.com/api/m/{merchant_id}/delivery-orders/documents/p/{p}?_delivery_statuses=completed&_delivery_statuses=pending&_shipping_label_print_statuses=not_printed&_shipping_label_status=ready&include_only_delivery_order_with_traking_number=true&_marketplace_connection_id={con_id}&_updated_at_from={last_date.isoformat('T', 'seconds')[:-6]}&_updated_at_to={datetime.now(timezone.utc).isoformat('T', 'seconds')[:-6]}"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    response = requests.request("GET", url, headers=headers).json()
    # If there are not entries, go for next connection
    if response['pagination']['total_items'] == 0:
        continue
    # Save raw data
    [raw.append(entry) for entry in response['entries']]
    # If more than one page, iterate over the others
    if response['pagination']['total_pages'] > 1:
        for p in range(1, response['pagination']['total_pages']):
            url = f"https://app.multivende.com/api/m/{merchant_id}/delivery-orders/documents/p/{p+1}?_delivery_statuses=completed&_delivery_statuses=pending&_shipping_label_print_statuses=not_printed&_shipping_label_status=ready&include_only_delivery_order_with_traking_number=true&_marketplace_connection_id={con_id}&_updated_at_from={last_date.isoformat('T', 'seconds')[:-6]}&_updated_at_to={datetime.now(timezone.utc).isoformat('T', 'seconds')[:-6]}"
            headers = {
                    'Authorization': f'Bearer {token}'
                }
            response2 =  requests.request("GET", url, headers=headers).json()
            [raw.append(entry) for entry in response2['entries']]

# Process data
data = []
for r in raw:
    tmp = {}
    tmp['n venta'] = r['DeliveryOrderInCheckouts'][0]['Checkout']['CheckoutLinks'][0]['externalId']
    tmp['fecha promesa'] = r['promisedDeliveryDate']
    tmp['direccion'] = r['ShippingAddress']['address_1']
    tmp['codigo'] = r['DeliveryOrderInCheckouts'][0]['Checkout']['code']
    tmp['courier'] = r['courierName']
    tmp['fecha despacho'] = r['handlingDateLimit']
    tmp['delivery status'] = r['deliveryStatus']
    n_seguimiento = r['trackingNumber'] 
    if len(n_seguimiento) == 21:
        tmp['N seguimiento'] = n_seguimiento[3:-7]
    else:
        tmp['N seguimiento'] = n_seguimiento
    tmp['status etiqueta'] = r['shippingLabelStatus']
    tmp['estado impresion etiqueta'] = r['shippingLabelPrintStatus']
    tmp['id venta'] =  r['DeliveryOrderInCheckouts'][0]['Checkout']['_id']
    tmp['codigo venta'] = r['DeliveryOrderInCheckouts'][0]['Checkout']['code']
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

    





