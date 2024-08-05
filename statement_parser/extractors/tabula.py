from pathlib import Path
from typing import Any, Dict

import pandas as pd
import tabula

from ._base import BaseExtractor
from ._enums import BankName, StatementType, ExtractorName
from ._columns import DateColumn, NumericColumn, StringColumn
from ._transformers import *


class TabulaBaseExtractor(BaseExtractor):
    _extractor_name = ExtractorName.TABULA
    _tabula_kwargs = Dict[str, Any]
    _column_names = List[str]

    def _extract_file(self, filepath: Path) -> pd.DataFrame:
        
        # parse pdf
        extracted = tabula.read_pdf(
            input_path=filepath,
            pages='all',
            guess=False,
            stream=True,
            silent=True,
            relative_area=True,
            **self._tabula_kwargs,
        )

        # process extracted data
        data = []
        for df in extracted:

            # set column names
            assert df.shape[1] == len(self._column_names)
            df.columns = self._column_names

            # output if records are available
            if df.shape[0] > 0:
                data.append(df)
            
        # combine extracted data
        df = pd.concat(data).reset_index(drop=True)

        # apply transformers
        df = self.process_df(df)

        return df


###############################################################################


class DbsCard(TabulaBaseExtractor):
    _bank_name = BankName.DBS
    _statement_type = StatementType.CARD
    _tabula_kwargs = {'columns': [90, 500]}
    _column_names = ['TransactionDate', 'Description', 'Amount']
    _transformers = [
        ExtractNewColumn('AccountName', r'^(.*) CARD NO\.: \d{4} \d{4} \d{4} \d{4}', regex_columns=['TransactionDate', 'Description']),
        ExtractNewColumn('AccountNumber', r'CARD NO\.: (\d{4} \d{4} \d{4} \d{4})', regex_columns=['Description']),
        ExtractMeta(
            key='stmt_date',
            regex=r'(\d{2} [A-Za-z]{3} \d{4}) ',
            regex_columns=['Description'],
            processors=[lambda x: pd.to_datetime(x, format='%d %b %Y')[0]],
        ),
        ColumnsProcess([
            DateColumn('TransactionDate', format='%d %b', drop_null=True),
            StringColumn('Description'),
            NumericColumn('Amount', drop_null=True),     
            StringColumn('AccountName'),
            StringColumn('AccountNumber'),       
        ])
    ]


class DbsCasa(TabulaBaseExtractor):
    _bank_name = BankName.DBS
    _statement_type = StatementType.CASA
    _tabula_kwargs = {'columns': [90, 300, 400, 490]}
    _column_names = ['TransactionDate', 'Description', 'Withdrawal', 'Deposit', 'Balance']
    _transformers = [
        ExtractNewColumn('AccountName', r'Details of Your (.*)', regex_columns=_column_names),
        ExtractNewColumn('AccountNumber', r'Account No\.: ([\d\-]*)', regex_columns=_column_names),
        ExtractMeta(
            key='stmt_date',
            regex=r'\d{1,2} [A-Za-z]{3} \d{4}\s?to\s?(\d{1,2} [A-Za-z]{3} \d{4})',
            regex_columns=['TransactionDate', 'Description'],
            processors=[lambda x: pd.to_datetime(x, format='%d %b %Y')[0]],
        ),
        RowsCombine(
            anchor_cols=['TransactionDate', 'Withdrawal', 'Deposit', 'Balance'],
            concat_cols=['Description'],
            first_cols=['AccountName', 'AccountNumber'],
        ),
        ColumnsProcess([
            DateColumn('TransactionDate', format='%d %b', drop_null=True),
            StringColumn('Description'),
            NumericColumn('Withdrawal'),
            NumericColumn('Deposit'),
            NumericColumn('Balance'),
            StringColumn('AccountName'),
            StringColumn('AccountNumber'),
        ]),
    ]


class OcbcCard(TabulaBaseExtractor):
    _bank_name = BankName.OCBC
    _statement_type = StatementType.CARD
    _tabula_kwargs = {'columns': [180, 410]}
    _column_names = ['TransactionDate', 'Description', 'Amount']
    _transformers = [
        ExtractNewColumn('AccountName', r'^OCBC (.*)$', regex_columns=['TransactionDate']),
        ExtractNewColumn('AccountNumber', r'^(\d{4}\-\d{4}\-\d{4}\-\d{4})$', regex_columns=['Description']),
        ExtractMeta(
            key='stmt_date',
            regex=r'(\d{2}-\d{2}-\d{4}) ',
            regex_columns=['TransactionDate'],
            processors=[lambda x: pd.to_datetime(x, format='%d-%m-%Y')[0]],
        ),
        ColumnsProcess([
            DateColumn('TransactionDate', format='%d/%m', drop_null=True),
            StringColumn('Description'),
            NumericColumn('Amount', drop_null=True),
            StringColumn('AccountName'),
            StringColumn('AccountNumber'),
        ]),
    ]


class OcbcCasa(TabulaBaseExtractor):
    _bank_name = BankName.OCBC
    _statement_type = StatementType.CASA
    _tabula_kwargs = {'columns': [90, 135, 238, 300, 400, 500]}
    _column_names = ['TransactionDate', 'ValueDate', 'Description', 'Cheque', 'Withdrawal', 'Deposit', 'Balance']
    _transformers = [
        ExtractNewColumn('AccountName', r'^(.* ACCOUNT)$', regex_columns=_column_names[:3]),
        ExtractNewColumn('AccountNumber', r'^Account No\.\s?(\d*)$', regex_columns=_column_names[:3]),
        ExtractMeta(
            key='stmt_date',
            regex=r'(\d{2} [A-Z]{3} \d{4})',
            regex_columns=['Balance'],
            processors=[lambda x: pd.to_datetime(x, format='%d %b %Y')[0]],
        ),
        RowsCombine(
            anchor_cols=['TransactionDate', 'ValueDate', 'Withdrawal', 'Deposit', 'Balance'],
            concat_cols=['Description'],
            first_cols=['AccountName', 'AccountNumber'],
        ),
        ColumnsProcess([
            DateColumn('TransactionDate', format='%d %b', drop_null=True),
            DateColumn('ValueDate', format='%d %b', drop_null=True),
            StringColumn('Description'),
            NumericColumn('Withdrawal'),
            NumericColumn('Deposit'),
            NumericColumn('Balance'),
            StringColumn('AccountName'),
            StringColumn('AccountNumber'),
        ]),
    ]


class UobCard(TabulaBaseExtractor):
    _bank_name = BankName.UOB
    _statement_type = StatementType.CARD
    _tabula_kwargs = {'columns': [100, 145, 450]}
    _column_names = ['PostDate', 'TransactionDate', 'Description', 'Amount']
    _transformers = [
        ExtractNewColumn('AccountName', r'^UOB (.*)$', regex_columns=_column_names[:3]),
        ExtractNewColumn('AccountNumber', r'^(\d{4}\-\d{4}\-\d{4}\-\d{4}) .*', regex_columns=_column_names[:3]),
        ExtractMeta(
            key='stmt_date',
            regex=r'Statement Date\s?(\d{1,2}\s{1,3}[A-Z]{3}\s{1,3}\d{4})',
            processors=[lambda x: pd.to_datetime(x, format='%d %b %Y')[0]],
        ),
        RowsCombine(
            anchor_cols=['PostDate', 'TransactionDate', 'Amount'],
            concat_cols=['Description'],
            first_cols=['AccountName', 'AccountNumber'],
        ),
        ColumnsProcess([
            DateColumn('PostDate', format='%d %b', drop_null=True),
            DateColumn('TransactionDate', format='%d %b', drop_null=True),
            StringColumn('Description'),
            NumericColumn('Amount', drop_null=True),
        ]),
    ]


class UobCasa(TabulaBaseExtractor):
    _bank_name = BankName.UOB
    _statement_type = StatementType.CASA
    _tabula_kwargs = {'columns': [110, 300, 395, 495]}
    _column_names = ['TransactionDate', 'Description', 'Withdrawal', 'Deposit', 'Balance']
    _transformers = [
        ExtractNewColumn('AccountName', r'^([\w\s]*)\s?\d{3}-\d{3}-\d{3}-\d{1}$', regex_columns=_column_names[:2]),
        ExtractNewColumn('AccountNumber', r'^[\w\s]*\s?(\d{3}-\d{3}-\d{3}-\d{1})$', regex_columns=_column_names[:2]),
        ExtractMeta(
            key='stmt_date',
            regex=r'Account Overview as at (\d{2} [A-Za-z]{3} \d{4})',
            regex_columns=['TransactionDate', 'Description'],
            processors=[lambda x: pd.to_datetime(x, format='%d %b %Y')[0]],
        ),
        RowsCombine(
            anchor_cols=['TransactionDate', 'Withdrawal', 'Deposit', 'Balance'],
            concat_cols=['Description'],
            first_cols=['AccountName', 'AccountNumber'],
        ),        
        ColumnsProcess([
            DateColumn('TransactionDate', format='%d %b', drop_null=True),
            StringColumn('Description'),
            NumericColumn('Withdrawal'),
            NumericColumn('Deposit'),
            NumericColumn('Balance'),
        ]),
        RowsFilter(
            exclude_regex=['BALANCE B/F'],
            column='Description',
        )
    ]
