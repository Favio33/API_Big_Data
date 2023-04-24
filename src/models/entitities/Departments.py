class Departments():

    def __init__ (self, id, department) -> None:
        self.id = id
        self.department = department
    
    def to_JSON(self) -> dict:
        return {
            'id': self.id,
            'department': self.department
        }