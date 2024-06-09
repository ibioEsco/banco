from src.patterns.strategy.bodyRequest.strategyManager.strategy_manager import StrategyManager
from src.patterns.strategy.bodyRequest.strategyManager.strategy import Strategy
from src.patterns.strategy.bodyRequest.impl.post import Post
from src.patterns.strategy.bodyRequest.impl.get import Get
from src.exception.default_exception import DefaultException
from dataclasses import dataclass


@dataclass
class Context:
    __strategy: Strategy
    __strategyManager = StrategyManager()

    def __init__(self, typeRequest: str, event):
        self.__type = typeRequest
        self.__event = event

    def setStrategy(self):
        self.__strategyManager.addStrategy(Post(self.__event))
        self.__strategyManager.addStrategy(Get(self.__event))

    def chooseStrategy(self) -> Strategy:
        self.__strategy = self.__strategyManager.getStrategy(self.__type)

    def getAction(self):
        if self.__strategy is not None:
           return  self.__strategy.getBody()

        raise DefaultException(None, "Strategy error", 500)

