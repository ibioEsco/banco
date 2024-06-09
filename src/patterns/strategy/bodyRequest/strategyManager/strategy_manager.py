from src.patterns.strategy.bodyRequest.strategyManager.strategy import Strategy

class StrategyManager:

    def __init__(self) -> None:
        self.__strategies = []

    def addStrategy(self,strategy: Strategy):
        self.__strategies.append(strategy)
        self.__strategies = list(set(self.__strategies))

    def getStrategy(self,name) -> Strategy:
        strategies_iter = list(filter(lambda x: x.getName() == name, self.__strategies))
        return strategies_iter[0]