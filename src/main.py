from flask import Flask
from config import config


#Routes
from routes import deptRoute, jobRoute, employeeRoute

app = Flask(__name__)


#Set flask
app.config.from_object(config['development'])

#Error Handler 404
def page_not_found(error):
    return '<h1> Page Not Found </h1>'

#Blueprints
app.register_blueprint(deptRoute.main, url_prefix='/api/depts')
app.register_blueprint(jobRoute.main, url_prefix='/api/jobs')
app.register_blueprint(employeeRoute.main, url_prefix='/api/employees')

app.register_error_handler(404, page_not_found)


if __name__ == '__main__':
    app.run()
