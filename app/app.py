# third-party imports
from flask import Flask

# local imports
from config import app_config
from service import service as service_blueprint


def create_app(config_name):
    #app = Flask(__name__, instance_relative_config=True)
    app = Flask(__name__)
    app.config.from_object(app_config[config_name])
    app.config.from_pyfile('config.py')
    app.register_blueprint(service_blueprint)
    return app
