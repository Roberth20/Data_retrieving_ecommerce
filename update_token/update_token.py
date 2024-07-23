import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
import os
import sys
import json
import requests
from models import auth_app
from utils import *
from datetime import datetime

# Setting up logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s: %(message)s', stream=sys.stdout,
                    level=logging.INFO)

try:
    import config
except:
    import test as config

# Making engine
engine = create_engine(config.SQLALCHEMY_DATABASE_URI,
                       connect_args={
                            "ssl": {
                                "ca":config.ssl
                                }     
                            }
                       )

logger.info("Updating token")
url = "https://app.multivende.com/oauth/access-token"

# Get last token
with Session(engine) as session:
    last_auth = session.scalar(select(auth_app).order_by(auth_app.expire.desc()))
    
# Check if exists token
if last_auth == None:
    logger.info("No hay token disponible")
    sys.exit(0)

refresh_token = last_auth.refresh_token

# Prepare data
payload = json.dumps({
    "client_id": config.CLIENT_ID,
    "client_secret": config.CLIENT_SECRET,    
    "grant_type": "refresh_token",
    "refresh_token": refresh_token}
)
    
headers = {
    'cache-control': 'no-cache',
    'Content-Type': 'application/json'
}
logger.info("Realizando solicitud.")
response = requests.post(url, headers=headers, data=payload)

try:
    # Guardamos la informacion requerida y logueamos
    token = response.json()["token"]
    expiresAt = response.json()["expiresAt"]
    refresh_token = response.json()["refreshToken"]
    encrypted = encrypt(token, config.SECRET_KEY)
    authentication = auth_app(token = encrypted, expire=datetime.fromisoformat(expiresAt), refresh_token=refresh_token)
    with Session(engine) as session:
        session.add(authentication)
        session.commit()
    logger.info("Actulizacion exitosa.")
except:
    logger.error(f"Hubo un error con la actualizacion: {response.text}")
