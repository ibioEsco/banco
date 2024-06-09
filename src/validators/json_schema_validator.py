from jsonschema import Draft202012Validator
from src.exception.default_exception import DefaultException

class ValidateRequest:

    @classmethod
    def process_error_data(cls, v, data):
        return [
            {
                "field_name": error.path[0] if error.path else None,
                "msn": error.message.replace('"', ''),
                "value": error.instance
            }
            for error in sorted(v.iter_errors(data), key=str)
        ]

    def json_schema_validator(self, data, schema):
        v = Draft202012Validator(schema)

        if not v.is_valid(data):
            errors = self.process_error_data(v, data)
            raise DefaultException(errors, "Validation error request", 422)
