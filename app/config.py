import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):
    """
    Common configurations
    """
    # Put any configurations here that are common across all environments
    SECRET_KEY = os.getenv('SECRET_KEY')

    # AWS Secrets


class DevelopmentConfig(Config):
    """
    Development configurations
    """
    DEBUG = True
    TESTING = True
    FLASK_ENV = 'development'


class ProductionConfig(Config):
    """
    Production configurations
    """
    DEBUG = False
    TESTING = False
    FLASK_ENV = 'production'
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
    AWS_KEY_ID = os.getenv('AWS_KEY_ID')


app_config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig
}