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

def test_validate_bam_illumina_spreadsheet(tmp_path):

    shutil.copyfile('examples/bam-nanopore-samplesheet-template-good.csv', tmp_path / 'test-nanopore.csv')

    shutil.copyfile('examples/reference.bam', tmp_path / 'reference.bam')

    samplesheet = pathlib.Path(tmp_path / 'test-nanopore.csv')

    validss = validate.Samplesheet(samplesheet)

    assert validss.validate()['validation']['status'] == 'completed'

def test_validate_nanopore_spreadsheet():

    samplesheet = pathlib.Path('examples/nanopore-samplesheet-template-good.csv')

    validss = validate.Samplesheet(samplesheet)

    assert validss.validate()['validation']['status'] == 'completed'

def test_validate_bam_illumina_spreadsheet(tmp_path):

    shutil.copyfile('examples/bam-nanopore-samplesheet-template-good.csv', tmp_path / 'test-illumina.csv')

    shutil.copyfile('examples/reference.bam', tmp_path / 'reference.bam')

    samplesheet = pathlib.Path(tmp_path / 'test-illumina.csv')

    validss = validate.Samplesheet(samplesheet)

    assert validss.validate()['validation']['status'] == 'completed'

def test_riak_ok(tmp_path):

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
            tmp_path / pathlib.Path('foo')
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = process.communicate()

    # insist that the above command did not fail
    assert process.returncode == 0

def test_samtools_ok(tmp_path):

    # assumes samtools is in the $PATH!
    process = subprocess.Popen(
        [
            'samtools',
            'fastq',
            '-o',
            tmp_path / pathlib.Path('foo.fastq.gz'),
            'examples/reference.bam',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = process.communicate()

    # insist that the above command did not fail
    assert process.returncode == 0
