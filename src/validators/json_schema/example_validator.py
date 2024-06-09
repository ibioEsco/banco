

class TestRequest:
    __schema = {
        "type": "object",
        "properties": {
            "test": {"type": "number"}
        },
        "required": ["test"]
    }

    def __init__(self):
        pass
    def get_schema(self):
        return self.__schema