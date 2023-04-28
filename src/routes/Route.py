from flask import Blueprint, jsonify, request

#Models
from models.Model import Model
from DataProcessing.Processing import Processing


main = Blueprint('model_route', __name__)

@main.route('/<table_name>/<numRows>')
def get_rows(table_name: str, numRows: int):
    try:    
        rows = Model.get_rows(table_name, numRows)
        return jsonify(rows)
    except Exception as ex:
        return jsonify({'Message':str(ex)}), 500
    


@main.route('/bulkInsert/<table_name>', methods=['POST'])
def add_rows(table_name):
    try:    
        data = request.json
        affected_rows = Model.insert_rows(table_name, data)
        if isinstance(affected_rows,int) and affected_rows > 1:
            return jsonify({"Rows inserted":affected_rows})
        else:
            return jsonify({"Affected Rows":affected_rows}), 500
    except Exception as ex:
        return jsonify({'Message':str(ex)}), 500

@main.route('/view/jobPerQ/<year>')
def get_jobPerQ(year):
    try:
        data_view = Processing.resumen_quarter(year)
        return data_view
    except Exception as ex:
        return jsonify({'Message':str(ex)})

@main.route('/view/hiredEmpReport')
def get_HiredEmpReport():
    try:
        data_view = Processing.report_hired_employes_top()
        return data_view
    except Exception as ex:
        return jsonify({'Message':str(ex)})