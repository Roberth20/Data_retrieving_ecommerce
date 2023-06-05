from flask_security import Security, SQLAlchemyUserDatastore
from App.extensions.db import db
from App.models.users import User, Role

user_datastore = SQLAlchemyUserDatastore(db, User, Role)
security = Security()