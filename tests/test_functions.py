import pytest
import pathlib
import shutil
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

def test_riak_ok():

    if pathlib.Path("./readItAndKeep").exists():
        riak = pathlib.Path("./readItAndKeep").resolve()

    # or if there is one in the $PATH use that one
    elif shutil.which('readItAndKeep') is not None:
        riak = pathlib.Path(shutil.which('readItAndKeep'))

    process = subprocess.Popen(
        [
            riak,
            '--ref_fasta',
            'examples/MN908947.fasta',
            '--reads1',
            'examples/MN908947_1.fastq.gz',
            '--outprefix',
            'foo'
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = process.communicate()

    # insist that the above command did not fail
    assert process.returncode == 0

def test_samtools_ok():

    process = subprocess.Popen(
        [
            'samtools',
            'fastq',
            '-o',
            'foo.fastq',
            'examples/MN908947.bam',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = process.communicate()

    # insist that the above command did not fail
    assert process.returncode == 0
