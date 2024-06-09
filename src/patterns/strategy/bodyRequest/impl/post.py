from src.patterns.strategy.bodyRequest.strategyManager.strategy import Strategy
import json
import base64


class Post(Strategy):
    __name: str = "post"

    def __init__(self, data):
        self.__data = data

    def getName(self):
        return self.__name

    def is_string(self, variable):
        return isinstance(variable, str)

    def json_clear(self, body):
        return body.replace('/\r/g', '').replace('/\n/g', '').replace('/\t/g', '')

    def getBody(self):
        body = self.__data['body']

        if 'isBase64Encoded' in self.__data and self.__data['isBase64Encoded'] == True:
            buff = base64.b64decode(self.__data['body'])
            return json.loads(buff.decode('utf-8', 'ignore'))

        if self.is_string(body):
            return json.loads(self.json_clear(body))
        else:
            return body
