import os

import pandera, pandas
from pandera.typing import Index, DataFrame, Series
import pandera.extensions as extensions
import pycountry

from gpas_uploader import BaseCheckSchema


@extensions.register_check_method()
def region_is_valid(df):
    """
    Validate the region field using ISO-3166-2 (pycountry).

    Returns
    -------
    bool
        True if all regions are ok, False otherwise
    """

    def validate_region(row):
        result = pycountry.countries.get(alpha_3=row.country)

        if result is None:
            return False
        elif pandas.isna(row.region) or row.region is None:
            return True
        else:
            region_lookup = [i.name for i in pycountry.subdivisions.get(country_code=result.alpha_2)]
            return row.region in region_lookup

    df['valid_region'] = df.apply(validate_region, axis=1)

    return df['valid_region'].all()

@extensions.register_check_method()
def instrument_is_valid(df):

    if 'fastq' in df.columns:
        instrument = 'Nanopore'
    elif 'fastq2' in df.columns:
        instrument = 'Illumina'

    return (df['instrument_platform'] == instrument).all()


class IlluminaFASTQCheckSchema(BaseCheckSchema):
    '''
    Validate GPAS upload CSVs specifying paired reads (e.g Illumina).
    '''

    # gpas_batch: Series[str] = pandera.Field(str_matches=r'^[A-Za-z0-9]')
    #
    # gpas_run_number: Series[int] = pandera.Field(nullable=True, ge=0)
    #
    # gpas_sample_name: Index[str] = pandera.Field(str_matches=r'^[A-Za-z0-9]')

    # validate that the fastq1 file is alphanumeric and unique
    fastq1: Series[str] = pandera.Field(unique=True, str_matches=r'^[A-Za-z0-9/._-]+$', str_endswith='_1.fastq.gz', coerce=True, nullable=False)

    # validate that the fastq2 file is alphanumeric and unique
    fastq2: Series[str] = pandera.Field(unique=True, str_matches=r'^[A-Za-z0-9/._-]+$', str_endswith='_2.fastq.gz', coerce=True, nullable=False)

    class Config:
        region_is_valid = ()
        instrument_is_valid = ()
        name = "IlluminaFASTQCheckSchema"
        strict = True
        coerce = True



class NanoporeFASTQCheckSchema(BaseCheckSchema):
    '''
    Validate GPAS upload CSVs specifying unpaired reads (e.g. Nanopore).
    '''

    # gpas_batch: Series[str] = pandera.Field(str_matches=r'^[A-Za-z0-9]')
    #
    # gpas_run_number: Series[int] = pandera.Field(nullable=True, ge=0)
    #
    # gpas_sample_name: Index[str] = pandera.Field(str_matches=r'^[A-Za-z0-9]')

    # validate that the fastq file is alphanumeric and unique
    fastq: Series[str] = pandera.Field(unique=True, str_matches=r'^[A-Za-z0-9/._-]+$', str_endswith='.fastq.gz', coerce=True, nullable=False)

    class Config:
        region_is_valid = ()
        instrument_is_valid = ()
        name = "NanoporeFASTQCheckSchema"
        strict = True
        coerce = True


class BAMCheckSchema(BaseCheckSchema):
    '''
    Validate GPAS upload CSVs specifying BAM files.
    '''

    # gpas_batch: Series[str] = pandera.Field(str_matches=r'^[A-Za-z0-9]')
    #
    # gpas_run_number: Series[int] = pandera.Field(nullable=True, ge=0)
    #
    # gpas_sample_name: Index[str] = pandera.Field(str_matches=r'^[A-Za-z0-9]')

    # validate that the bam file is alphanumeric and unique
    bam: Series[str] = pandera.Field(unique=True, str_matches=r'^[A-Za-z0-9/._-]+$', str_endswith='.bam', coerce=True, nullable=False)

    # insist that the path to the bam exists
    # @pandera.check('bam_path')
    # def check_bam_file_exists(cls, a, error='bam file does not exist'):
    #     return all(a.map(os.path.isfile))

    class Config:
        region_is_valid = ()
        name = "BAMCheckSchema"
        strict = True
        coerce = True
