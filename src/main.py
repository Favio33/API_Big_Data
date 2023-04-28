from flask import Flask
import sys
sys.path.append('./src/')
from config import config


#Routes
from routes import Route

app = Flask(__name__)


#Set flask
app.config.from_object(config['development'])

#Error Handler 404
def page_not_found(error):
    return '<h1> Page Not Found </h1>'

#Blueprints
app.register_blueprint(Route.main, url_prefix='/api/db')

app.register_error_handler(404, page_not_found)


if __name__ == '__main__':
    app.run()
