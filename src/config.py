from decouple import config
import os

class Config:
    SECRET_KEY = config('SECRET_KEY')

class DevelopmentConfig(Config):

    if os.environ.get('GAE_ENV') == 'standard':
        DEBUG=False
    else:
        DEBUG=True

config = {
    'development':DevelopmentConfig
}