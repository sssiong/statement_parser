from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, List

import pandas as pd
import re

from ._columns import BaseColumn


def _get_lines(df: pd.DataFrame, sep: str = '') -> pd.Series:
    """ Concat columns """
    return (
        df.fillna('')
        .astype(str)
        .apply(sep.join, axis=1)
    )


@dataclass
class BaseTransformer(ABC):
    verbose: bool = field(default=True, kw_only=True)
    meta_dict: dict = field(default_factory=dict, kw_only=True)
    
    @abstractmethod
    def transform(self, df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        pass


###############################################################################

# data extraction transformers


@dataclass
class ExtractNewColumn(BaseTransformer):
    """ Extract specfic data point from extracted dataframe and create new column """
    name: str
    regex: str
    regex_columns: List[str] = None
    processors: List[Callable] = None

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:

        # apply regex to specific column or whole lines
        if self.regex_columns:
            lines = _get_lines(df[self.regex_columns])
            out = lines.str.extract(self.regex)#.dropna().values
        else:
            lines = _get_lines(df)
            out = lines.str.extract(self.regex)#.dropna()#.values
        
        df[self.name] = out.ffill()

        return df


@dataclass
class ExtractMeta(BaseTransformer):
    """ Extract specific data point from extracted dataframe and save to meta """
    key: str
    regex: str
    regex_columns: List[str] = None
    processors: List[Callable] = None

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:

        # skip if key is already available in meta
        if self.key in self.meta_dict:
            return df

        # apply regex to specific column or whole lines
        lines = _get_lines(df[self.regex_columns] if self.regex_columns else df)
        out = lines.str.extract(self.regex).dropna()
        
        # process extracted data
        if len(out) > 0:
            out = out.iloc[0]
            if self.processors is not None:
                for processor in self.processors:
                    out = processor(out)
            self.meta_dict[self.key] = out
            
        return df


###############################################################################

# column transformers


@dataclass
class ColumnsProcess(BaseTransformer):
    """ Convert columns to appropriate data type """
    columns: List[BaseColumn]
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df = df.copy()
        for col in self.columns:
            df[col.name] = col.process(df[col.name], **kwargs)
            if col.drop_null:
                idx = df[col.name].isnull()
                df = df[~idx]
        return df


###############################################################################

# row transformers

@dataclass
class RowsCombine(BaseTransformer):
    anchor_cols: List[str]
    concat_cols: List[str]
    first_cols: List[str] = field(default_factory=list)
    sep: str = ' '

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:

        # identify groups
        df['group_id'] = df[self.anchor_cols].notnull().any(axis=1).cumsum()

        # if anchor columns all null, return empty dataframe
        if df['group_id'].max() == 0:
            return df[df['group_id']!=0].drop(columns='group_id')
        
        # combine within same group
        grp = df.groupby('group_id')
        out = pd.concat([
            grp[self.anchor_cols].first(),
            grp[self.concat_cols].apply(lambda g: g.apply(lambda r: self.sep.join([f'{i}' for i in r]))),
            grp[self.first_cols].first(),
        ], axis=1)
        return out.reset_index(drop=True)


@dataclass
class RowsFilter(BaseTransformer):
    exclude_regex: List[str]
    column: str = None

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
 
        if df.shape[0] == 0:
            return df
        
        regex = '|'.join(self.exclude_regex)
        if self.column:
            is_invalid = df[self.column].str.contains(regex, regex=True)
        else:
            is_invalid = _get_lines(df).str.contains(regex, regex=True)
        return df[~is_invalid]
