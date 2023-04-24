from flask import Blueprint, jsonify

#Models
from models.DepartmentModel import DepartmentModel

main = Blueprint('deptRoute',__name__)

@main.route('/')
def get_depts():
    try:
        depts = DepartmentModel.get_depts()
        return jsonify(depts)
    except Exception as ex:
        return jsonify({'Message': str(ex)}), 500