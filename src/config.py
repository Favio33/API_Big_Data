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



tables = {
    'jobs' : {
        'id': {
            'type': int,
            'required': True,
            'pk': True
        },
        'job': {
            'type': str,
            'required': True,
            'maxLength': 100,
            'default': None
        }
    },

    'departments' : {
        'id': {
            'type': int,
            'required': True,
            'pk': True
        },
        'department': {
            'type': str,
            'required': True,
            'maxLength': 100,
            'default': None
        }
    },

    'hiredemployees' : {
        'id': {
            'type': int,
            'required': True,
            'pk': True
        },
        'name': {
            'type': str,
            'required': True,
            'maxLength': 100,
            'default': None
        },
        'datetime': {
            'type': str,
            'required': True,
            'date': True,
        },
        'department_id': {
            'type': int,
            'required': True,
            'fk': True
        },
        'job_id': {
            'type': int,
            'required': True,
            'fk': True
        }
    }
}
