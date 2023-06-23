import os
from flask import Flask
from App.extensions.db import db
from App.extensions.security import user_datastore, security
from App.extensions.talisman import talisman

def create_app(test_config=False):
    app = Flask(__name__, instance_relative_config=True)
    
    if test_config:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_prefixed_env()
        if 'RDS_DB_NAME' in os.environ:
            app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'.format(
                username=os.environ['RDS_USERNAME'],
                password=os.environ['RDS_PASSWORD'],
                host=os.environ['RDS_HOSTNAME'],
                port=os.environ['RDS_PORT'],
                database=os.environ['RDS_DB_NAME'],
            )
    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass    
    
    db.init_app(app)
    security.init_app(app, user_datastore)
    talisman.init_app(app, force_https=False)
    
    from App.main import bp as main_page
    app.register_blueprint(main_page)
    
    from App.download import download as download_page
    app.register_blueprint(download_page, url_prefix="/download")
    
    from App.update import update as update_page
    app.register_blueprint(update_page, url_prefix="/update")
    
    from App.auth import auth as auth_page
    app.register_blueprint(auth_page, url_prefix="/auth")
    
    from App.create_update import cupdate
    app.register_blueprint(cupdate, url_prefix="/create_update")
    return app