import datetime

import pandas

import pandera
from pandera.typing import Index, DataFrame, Series
import pycountry


class BaseCheckSchema(pandera.SchemaModel):
    '''
    Validate generic GPAS upload CSVs.

    Built off to validate specific cases.
    '''

    # validate that batch is alphanumeric only
    batch: Series[str] = pandera.Field(str_matches=r'^[A-Za-z0-9._-]+$',
                                       coerce=True, nullable=False)

    # validate run_number is alphanumeric but can also be null
    run_number: Series[str] = pandera.Field(str_matches=r'^[A-Za-z0-9._-]+$',
                                        nullable=True, coerce=True)

    # validate sample name is alphanumeric and insist it is unique
    sample_name: Index[str] = pandera.Field(str_matches=r'^[A-Za-z0-9._-]+$',
                                     unique=True, coerce=True, nullable=False)

    # insist that control is one of positive, negative or null
    control: Series[str] = pandera.Field(nullable=True,
                                         isin=['positive','negative',None],
                                         coerce=True)

    # validate that the collection is in the ISO format, is no earlier than 01-Jan-2019 and no later than today
    collection_date: Series[pandera.DateTime] = pandera.Field(gt='2019-01-01',
                                                              le=str(datetime.date.today()),
                                                              coerce=True, nullable=False)

    # insist that the country is one of the entries in the specified lookup table
    country: Series[str] = pandera.Field(isin=[i.alpha_3 for i in pycountry.countries],
                                         coerce=True, nullable=False)

    region: Series[str] = pandera.Field(nullable=True, isin=[i.name for i in list(pycountry.subdivisions)], coerce=True)

    district: Series[str] = pandera.Field(str_matches=r'^[\sA-Za-z0-9:_-]+$', nullable=True, coerce=True)

    # insist that the tags is alphanumeric, including : as it is the delimiter
    tags: Series[str] = pandera.Field(nullable=False, str_matches=r'^[A-Za-z0-9:_-]+$', coerce=True)

    # at present host can only be human
    host: Series[str] = pandera.Field(isin=['human'], coerce=True, nullable=False)

    # at present specimen_organism can only be SARS-CoV-2
    specimen_organism: Series[str] = pandera.Field(isin=['SARS-CoV-2'], coerce=True, nullable=False)

    # at present primer_schema can only be auto
    primer_scheme: Series[str] = pandera.Field(isin=['auto'], coerce=True, nullable=False)

    # insist that instrument_platform can only be Illumina or Nanopore
    instrument_platform: Series[str] = pandera.Field(isin=['Illumina', 'Nanopore'], coerce=True, nullable=False)

    # custom method that checks that the collection_date is only the date and does not include the time
    # e.g. "2022-03-01" will pass but "2022-03-01 10:20:32" will fail
    @pandera.check("collection_date")
    def check_collection_date(cls, a):
        return ((a.dt.floor('d') == a).all())

    # custom method to check that one, and only one, instrument_platform is specified in a single upload CSV
    @pandera.check("instrument_platform")
    def check_unique_instrument_platform(cls, a):
        return len(a.unique()) == 1


    class Config:
        name = "BaseCheckSchema"
        strict = False
        coerce = True
