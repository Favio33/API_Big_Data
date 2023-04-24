from flask import Flask
from config import config


app = Flask(__name__)


#Set flask
app.config.from_object(config['development'])

#Error Handler 404
def page_not_found(error):
    return '<h1> Page Not Found </h1>'

@app.route('/')
def hello():
    return '<h1> Raa </h1>'


app.register_error_handler(404, page_not_found)


if __name__ == '__main__':
    app.run()
