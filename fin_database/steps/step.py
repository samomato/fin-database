from abc import ABC
from abc import abstractmethod


class Step(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def daily_process(self, input_, utils):
        pass

    @abstractmethod
    def month_process(self, input_, utils):
        pass

    @abstractmethod
    def f_report_process(self, input_, utils):
        pass

    @abstractmethod
    def futures_process(self, input_, utils):
        pass


class StepException(Exception):
    pass
