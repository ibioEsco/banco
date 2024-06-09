from abc import abstractmethod, ABC


class Strategy(ABC):

    @abstractmethod
    def __init__(self, data):
        self.__data = data

    @abstractmethod
    def getName(self):
        return self.__name

    @abstractmethod
    def getBody(self):
        pass
