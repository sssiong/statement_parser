import base64
import tempfile
from pathlib import Path

from statement_parser.extractors.tabula import *


def _process_files(bank: str, statement_type: str, pdf_files: list) -> pd.DataFrame:
    
    match bank, statement_type:
        case 'Citibank', 'Cards':
            extractor = CitiCard()
        case 'DBS', 'CASA':
            extractor = DbsCasa()
        case 'DBS', 'Cards':
            extractor = DbsCard()
        case 'OCBC', 'CASA':
            extractor = OcbcCasa()
        case 'OCBC', 'Cards':
            extractor = OcbcCard()
        case 'UOB', 'CASA':
            extractor = UobCasa()
        case 'UOB', 'Cards':
            extractor = UobCard()
        case _:
            raise ValueError(f'Unknown {bank=}, {statement_type=} combi!')

    df = extractor.extract_files(pdf_files)
    return df

def _save_base64_to_tempfile(base64_string: str, suffix: str = ".pdf") -> Path:
    file_data = base64.b64decode(base64_string)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    temp_file.write(file_data)
    temp_file.close()
    return Path(temp_file.name)
