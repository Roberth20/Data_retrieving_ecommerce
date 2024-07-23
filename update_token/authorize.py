import sys
import requests
import os
import json
import logging
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session
from models import auth_app
from utils import encrypt

try:
    import config
except:
    import test as config

# Setting up logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s: %(message)s', stream=sys.stdout,
                    level=logging.INFO)

# Making engine
engine = create_engine(config.SQLALCHEMY_DATABASE_URI)

# Get teh authorization code from script call
code = sys.argv[1]
logger.info('Requesting access token.')
# Definimos url de acceso
url = "https://app.multivende.com/oauth/access-token"
# Creamos la informacion a enviar
payload = {
            "client_id": config.CLIENT_ID,
            "client_secret": config.CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code
}
# Definimos los headers del POST
headers = {
        'cache-control': 'no-cache',
        'Content-Type': 'application/json'
}
response = requests.post(url, headers=headers, data=json.dumps(payload))

try:
    # Guardamos la informacion requerida y logueamos
    token = response.json()["token"]
    expiresAt = response.json()["expiresAt"]
    refresh_token = response.json()["refreshToken"]
    encrypted = encrypt(token, config.SECRET_KEY)
    authentication = auth_app(token = encrypted, expire=expiresAt, refresh_token=refresh_token)
    with Session(engine) as session:
        session.add(authentication)
        session.commit()
    logger.info('Token successfully actualized.')
except Exception as e:
    logger.error(f"Error en la autenticacion {e}: {response.text}")
