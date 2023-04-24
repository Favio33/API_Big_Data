from flask import Blueprint, jsonify

#Models
from models.EmployeeModel import EmployeeModel

main = Blueprint('employeeRoute', __name__)

@main.route('/<numRows>')
def get_employees(numRows: int):
    try:    
        employees = EmployeeModel.get_employees(numRows)
        return jsonify(employees)
    except Exception as ex:
        return jsonify({'Message':str(ex)}), 500