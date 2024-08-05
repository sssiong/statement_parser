from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class BaseColumn(ABC):
    name: str
    drop_null: bool = False

    def process(self, ss: pd.Series, *args, **kwargs) -> pd.Series:
        ss = self._process(ss.copy(), *args, **kwargs)
        return ss

    @abstractmethod
    def _process(self, ss: pd.Series, *args, **kwargs) -> pd.Series:
        raise Exception(f'Yet to implement')
    

@dataclass
class DateColumn(BaseColumn):
    format: str = None

    def _process(self, ss: pd.Series, stmt_date: float = None, *args, **kwargs) -> pd.Series:
        
        assert self.format is not None, 'Date format required'

        if '%Y' not in self.format and stmt_date is not None:
            ss = ss.astype(str) + ' ' + str(stmt_date.year)
            fmt = self.format + ' %Y'
        else:
            fmt = self.format
        
        ss = pd.to_datetime(ss, errors='coerce', format=fmt)
        
        if stmt_date is not None:
            idx = (ss - stmt_date).dt.days > 300
            ss.loc[idx] = ss.loc[idx] - pd.DateOffset(years=1)

        return [s.date() for s in ss]


@dataclass
class NumericColumn(BaseColumn):
    flip_sign: bool = False

    def _process(self, ss: pd.Series, *args, **kwargs) -> pd.Series:

        # replace xxx- with xxx
        idx = ss.fillna('').astype(str).str.contains(r'\d+-', regex=True)
        ss.loc[idx] = ss.loc[idx].astype(str).str.replace('-', '', regex=False)

        # remove $ sign
        ss = ss.str.replace('$', '', regex=False)

        # replace x,xxx with xxxx
        ss = ss.str.replace(',', '', regex=False)
        
        # replace xxx CR$ with -xxx        
        idx = ss.fillna('').str.contains(' CR$', regex=True)
        ss.loc[idx] = '-' + ss.loc[idx].str.replace(' CR$', '', regex=True)

        # replace (xxx) with -xxx
        idx = ss.fillna('').str.contains('(', regex=False)
        ss.loc[idx] = (
            ss.loc[idx]
            .str.replace('(', '-', regex=False)
            .str.replace(')', '', regex=False)
        )

        # convert to numeric
        ss = pd.to_numeric(ss, errors='coerce')

        # flip sign
        if self.flip_sign:
            ss = - ss
        
        return ss
    

class StringColumn(BaseColumn):
    
    def _process(self, ss: pd.Series, *args, **kwargs) -> pd.Series:
        return ss.astype(str).str.replace(r'\n|\s+', ' ', regex=True)
