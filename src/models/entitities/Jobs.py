class Jobs():

    def __init__(self, id, job) -> None:
        self.id = id
        self.job = job

    def to_JSON(self) -> dict:
        return {
            'id': self.id,
            'job': self.job
        }
