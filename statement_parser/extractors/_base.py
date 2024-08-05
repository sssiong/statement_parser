from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

import pandas as pd
from tqdm import tqdm

from ._enums import BankName, StatementType, ExtractorName
from ._transformers import BaseTransformer


class BaseExtractor(ABC):
    _bank_name: BankName
    _statement_type: StatementType
    _extractor_name: ExtractorName
    _transformers: List[BaseTransformer] = None
    

    def extract_files(self, filepaths: List[Path], verbose: int = 0) -> pd.DataFrame:
        """ Extract multiple PDF files """
        data = []
        for filepath in (pbar := tqdm(filepaths, disable=(verbose==0))):
            pbar.set_description(filepath.name)
            tmp = self.extract_file(filepath)
            data.append(tmp)

            # reset meta_dict between files
            for tfmr in self._transformers:
                tfmr.meta_dict = {}

        return pd.concat(data).reset_index(drop=True)


    def extract_file(self, filepath: Path) -> pd.DataFrame:
        """ Extract PDF file content to dataframe 
        
        Args:
            filepath (Path): Path to PDF file.
        """

        try:
            df = self._extract_file(filepath)
        except Exception as e:
            raise Exception(f'Extraction failed for {filepath} - {e}')
        
        assert df is not None, f'Extraction return `None` for {filepath}'
        
        df['BankName'] = self._bank_name
        df['StatementType'] = self._statement_type

        return df


    def process_df(self, df: pd.DataFrame, verbose: bool = False, **kwargs) -> Optional[pd.DataFrame]:
        """ Process extracted dataframe

        Args:
            df (pd.DataFrame): Extracted dataframe

        Returns:
            pd.DataFrame: Processed dataframe
        """
        if self._transformers is not None:
            
            for tfmr in self._transformers:
                
                if verbose:
                    print(f'\n\nProcessing {tfmr}')

                df = tfmr.transform(df, **kwargs)
                
                if verbose:
                    print(f'\n\n{df.to_markdown()}')

                if tfmr.meta_dict is not None:
                    kwargs = {**tfmr.meta_dict, **kwargs}
        
        if 'stmt_date' in kwargs:
            df['StatementDate'] = kwargs['stmt_date'].date()

        return df
    

    ###########################################################################

    
    @abstractmethod
    def _extract_file(self, filepath: Path) -> pd.DataFrame:
        raise Exception(f'NOT IMPLEMENTED!')
