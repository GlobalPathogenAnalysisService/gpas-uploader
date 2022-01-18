import pytest, pathlib
import subprocess
import validate

def test_spreadsheet_validation_correctly_fails():

    with pytest.raises(Exception) as e_info:
        validate.Samplesheet('examples/none-existent-file.csv')

    # forgot file extension
    with pytest.raises(Exception) as e_info:
        validate.Samplesheet('examples/illumina-samplesheet-template-good')

def test_validate_illumina_spreadsheet():

    samplesheet = pathlib.Path('examples/illumina-samplesheet-template-good.csv')

    validss = validate.Samplesheet(samplesheet)

    assert validss.validate()['validation']['status'] == 'completed'

def test_validate_nanopore_spreadsheet():

    samplesheet = pathlib.Path('examples/nanopore-samplesheet-template-good.csv')

    validss = validate.Samplesheet(samplesheet)

    assert validss.validate()['validation']['status'] == 'completed'

def test_samtools_ok():

    process = subprocess.Popen(
        [
            'samtools',
            'fastq',
            '-o',
            'foo.fastq',
            'examples/MN908946.bam',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
