import pytest, pathlib

import validate

def test_spreadsheet_validation_correctly_fails():

    with pytest.raises(Exception) as e_info:
        validate.Samplesheet('examples/none-existent-file.csv')

    # forgot file extension
    with pytest.raises(Exception) as e_info:
        validate.Samplesheet('examples/illumina-samplesheet-template-good')

    # bad CollectionDate
    with pytest.raises(Exception) as e_info:
        validate.Samplesheet('tests/illumina-samplesheet-template-fail-0.csv')

    with pytest.raises(Exception) as e_info:
        validate.Samplesheet('tests/illumina-samplesheet-template-fail-1.csv')


def test_validate_illumina_spreadsheet():

    samplesheet = pathlib.Path('examples/illumina-samplesheet-template-good.csv')

    validss = validate.Samplesheet(samplesheet)

    assert validss.validate()['validation']['status'] == 'completed'

def test_validate_nanopore_spreadsheet():

    samplesheet = pathlib.Path('examples/nanopore-samplesheet-template-good.csv')

    validss = validate.Samplesheet(samplesheet)

    assert validss.validate()['validation']['status'] == 'completed'
