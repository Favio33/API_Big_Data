from flask import Flask
#Activate this lines when yur are deploying in GAE
#import sys
#sys.path.append('./src/')
from config import config


#Routes
from routes import Route

app = Flask(__name__)


#Set flask
app.config.from_object(config['development'])

#Error Handler 404
def page_not_found(error):
    return '<h1> Check the url! </h1>'

#Blueprintsx|
app.register_blueprint(Route.main, url_prefix='/api/db')

app.register_error_handler(404, page_not_found)


if __name__ == '__main__':
    app.run()
