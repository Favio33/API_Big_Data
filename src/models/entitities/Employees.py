class Employees():

    def __init__(self, id, name, datetime, department_id, job_id) -> None:
        self.id = id
        self.name = name
        self.datetime = datetime
        self.department_id = department_id
        self.job_id = job_id

    def to_JSON(self):
        return {
            'id': self.id,
            'name': self.name,
            'datetime': self.datetime,
            'department_id': self.department_id,
            'job_id': self.job_id
        }
    