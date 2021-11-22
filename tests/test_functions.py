import pytest, validate, imp

from cleanreads import Decontamination

gpas_uploader = imp.load_source('gpas-uploader','./gpas-uploader')

from pathlib import Path

def test_validate_illumina_spreadsheet():

    samplesheet = Path('examples/illumina-samplesheet-template-good.csv')

    validss = validate.Samplesheet(samplesheet)

    print(validss.validate())
    assert validss.validate()['validation']['status'] == 'completed'

def test_validate_nanopore_spreadsheet():

    samplesheet2 = Path('examples/nanopore-samplesheet-template-good.csv')

    validss = validate.Samplesheet(samplesheet2)

    print(validss.validate())
    assert validss.validate()['validation']['status'] == 'completed'

def test_decontaminate_illumina_spreadsheet():

    Decontamination('examples/MN908947_1.fastq.gz',sample='MN908947')
    
    samplesheet = Path('examples/illumina-samplesheet-template-good.csv')

    validation = validate.Samplesheet(samplesheet)

    assert validation.validate()['validation']['status'] == 'completed'

    for sample in validation.samples:

        if sample.fq2:
            gpas_uploader.submit_illumina(sample, use_json=True)
            
        print(json.dumps(validation.make_submission()))


