# Important settings for App
SECRET_KEY= "TheBestKey2023"
SQLALCHEMY_DATABASE_URI = "mysql+pymysql://multivende_app:robmago@127.0.0.1:3306/multivende"
# Random SALT, per example with secrets.SystemRandom().getrandbits(128)
SECURITY_PASSWORD_SALT = "296256819904444244464578265745165835417"
BROKER_URL = "redis://localhost:6379/0"

# Data of client in Multivende

CLIENT_ID = 1111111111
CLIENT_SECRET = "HereGoesTheSecretKey"
MERCHANT_ID = "9c4e3348-c58e-48d2-b083-xxxxxxxxxxx"

# Data of client in other marketplaces

FALABELLA_API_KEY = "HereGoesAnApiKey"
FALABELLA_USER = "heregoestheuser@from.falabella.com"
PARIS_API_KEY = "HereGoesAnApiKey"
RIPLEY_API_KEY = "HereGoesAnApiKey"

# Paramteters to create the user of the app

EMAIL = "testing@appme.com"
PASSWORD = "HelloWorld1235."
USERNAME = "Tester"

# Optional parameters for the App

SESSION_COOKIE_SAMESITE = "Strict"
REMEMBER_COOKIE_SAMESITE = "strict"
SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping":True}
SQLALCHEMY_TRACK_MODIFICATIONS = False
SECURITY_USERNAME_ENABLE = True
SECURITY_PASSWORD_SINGLE_HASH = None
SECURITY_PASSWORD_HASH = "bcrypt"
force_https=False





