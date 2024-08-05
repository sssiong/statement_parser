from enum import StrEnum

class BankName(StrEnum):
    DBS = 'DBS'
    OCBC = 'OCBC'
    UOB = 'UOB'

class StatementType(StrEnum):
    CASA = 'CASA'
    CARD = 'Cards'

class ExtractorName(StrEnum):
    TABULA = 'Tabula'
