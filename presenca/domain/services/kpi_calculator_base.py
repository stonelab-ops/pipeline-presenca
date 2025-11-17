import pandas as pd
from abc import ABC, abstractmethod

class KpiCalculatorBase(ABC):
    
    def __init__(self, report: pd.DataFrame, data_frames: dict, config: dict):
        self.report = report
        self.data = data_frames
        self.config = config
        self._initialize()

    def _initialize(self):
        pass

    @abstractmethod
    def calculate(self) -> pd.DataFrame:
        pass