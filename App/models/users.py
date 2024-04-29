from App.extensions.db import db
from flask_security.models import fsqla

fsqla.FsModels.set_db_info(db)

class Role(db.Model, fsqla.FsRoleMixin):
    pass
    
class User(db.Model, fsqla.FsUserMixin):
    pass
