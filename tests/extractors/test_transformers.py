from statement_parser.extractors._columns import *
from statement_parser.extractors._transformers import *

import pandas as pd
from pandas.testing import assert_frame_equal


def test_columns_process():
    given = pd.DataFrame({
        'Date': ['201010', '201011'],
        'Numeric': ['20,000', '(10,200)']
    })
    expected = pd.DataFrame({
        'Date': [d.date() for d in pd.to_datetime(['2010-10-01', '2010-11-01'])],
        'Numeric': [20000, -10200],
    })
    tfmr = ColumnsProcess([
        DateColumn('Date', format='%Y%m'),
        NumericColumn('Numeric'),
    ])
    actual = tfmr.transform(given)
    assert_frame_equal(actual, expected), actual


def test_rows_combine():
    given = pd.DataFrame({
        'A': ['A1', None],
        'B': ['B1', 'B2'],
    })
    expected = pd.DataFrame({
        'A': ['A1'],
        'B': ['B1|B2'],
    })
    tfmr = RowsCombine(anchor_cols=['A'], concat_cols=['B'], sep='|')
    actual = tfmr.transform(given)
    assert_frame_equal(actual, expected), actual


def test_rows_filter():
    given = pd.DataFrame({
        'A': ['A1', 'A2', 'A3', 'A4'],
        'B': ['B1', 'B2', 'B3', 'B4'],
    })
    expected = pd.DataFrame({
        'A': ['A1', 'A2'],
        'B': ['B1', 'B2'],
    })
    tfmr = RowsFilter(exclude_regex=['3', '4'], column='A')
    actual = tfmr.transform(given)
    assert_frame_equal(actual, expected), actual
    