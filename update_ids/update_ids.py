import logging
from sqlalchemy import select, create_engine, update
from sqlalchemy.orm import Session
import os
import sys
import json
import requests
from models import auth_app, ids, customs_ids
from datetime import datetime
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

# Getting data from ids
merchant_id = config.MERCHANT_ID
logger.info("Getting data from brands")
brands = get_data_brands(token, merchant_id)     

logger.info("Getting data from warranties")
warr = get_data_warranties(token, merchant_id)

logger.info("Getting data from tags")
tags = get_data_tags(token, merchant_id)

logger.info("Getting data from colors")
colors = get_data_colors(token, merchant_id)

logger.info("Getting data from categories")
cats = get_data_categories(token, merchant_id)

logger.info("Getting data from sizes")
size = get_data_size(token, merchant_id)

df = pd.concat([brands, warr, tags, colors, cats, size], ignore_index=True)

# Getting data from customs ids
logger.info("Retrieving custom attributes")
data = get_customs_attributes(token, merchant_id)

logger.info("Uploading to DB")
try:
    data.to_sql("customs_ids", engine, if_exists = "replace", index=False)
    logger.info(f"Tabla 'customs_ids' populada con exito.")
except Exception as e:
    logger.error(f"La tabla 'customs_ids' tuvo un error {e}")
    
with Session(engine) as session:
    for i, row in df.iterrows():
        result = session.scalar(select(ids).where(ids.id == row["_id"])) 
        if result == None:
            new_ids = ids(name = row["name"], id = row["_id"], type=row["type"])
            session.add(new_ids)
        else:
            stmt = (
                    update(ids)
                    .where(ids.id == row["_id"])
                    .values(name = row["name"], type=row["type"], id = row["_id"])
                )
            session.execute(stmt)
        session.commit()

logger.info("Successful upload customs_ids and ids to DB.")
