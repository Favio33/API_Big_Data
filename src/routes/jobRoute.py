from flask import Blueprint, jsonify

#Model
from models.JobsModel import JobsModel

main = Blueprint('jobRoyte', __name__)

@main.route('/')
def get_jobs():
    try:
        jobs = JobsModel.get_jobs()
        return jsonify(jobs)
    except Exception as ex:
        return jsonify({'Message': str(ex)}), 500