from src.patterns.strategy.bodyRequest.strategyManager.strategy import Strategy


class Get(Strategy):

    __name: str = "get"

    def __init__(self, data):
        self.__data = data

    def getName(self):
        return self.__name

    def getBody(self):
        return self.__data.get('queryStringParameters', None)