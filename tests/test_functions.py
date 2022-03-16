import pytest
import pathlib
import shutil
import subprocess
import importlib
import sys

# def test_spreadsheet_validation_correctly_fails():
#
#     with pytest.raises(Exception) as e_info:
#         validate.Batch('examples/none-existent-file.csv')
#
#     # forgot file extension
#     with pytest.raises(Exception) as e_info:
#         validate.Batch('examples/illumina-samplesheet-template-good')
#
# def test_validate_illumina_spreadsheet():
#
#     samplesheet = pathlib.Path('examples/illumina-samplesheet-template-good.csv')
#
#     validss = validate.Batch(samplesheet)
#
#     assert validss.validate()['validation']['status'] == 'completed'
#
# def test_validate_bam_illumina_spreadsheet(tmp_path):
#
#     shutil.copyfile('examples/bam-nanopore-samplesheet-template-good.csv', tmp_path / 'test-nanopore.csv')
#
#     shutil.copyfile('examples/reference.bam', tmp_path / 'reference.bam')
#
#     samplesheet = pathlib.Path(tmp_path / 'test-nanopore.csv')
#
#     validss = validate.Batch(samplesheet)
#
#     assert validss.validate()['validation']['status'] == 'completed'
#
# def test_validate_nanopore_spreadsheet():
#
#     samplesheet = pathlib.Path('examples/nanopore-samplesheet-template-good.csv')
#
#     validss = validate.Batch(samplesheet)
#
#     assert validss.validate()['validation']['status'] == 'completed'
#
# def test_validate_bam_illumina_spreadsheet(tmp_path):
#
#     shutil.copyfile('examples/bam-nanopore-samplesheet-template-good.csv', tmp_path / 'test-illumina.csv')
#
#     shutil.copyfile('examples/reference.bam', tmp_path / 'reference.bam')
#
#     samplesheet = pathlib.Path(tmp_path / 'test-illumina.csv')
#
#     validss = validate.Batch(samplesheet)
#
#     assert validss.validate()['validation']['status'] == 'completed'
#
def test_riak_ok(tmp_path):

    if pathlib.Path("./readItAndKeep").exists():
        riak = pathlib.Path("./readItAndKeep").resolve()

    # or if there is one in the $PATH use that one
    elif shutil.which('readItAndKeep') is not None:
        riak = pathlib.Path(shutil.which('readItAndKeep'))

    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        ref_genome = 'MN908947_no_polyA.fasta'
    else:
        with importlib.resources.path("gpas_uploader", 'MN908947_no_polyA.fasta') as path:
            ref_genome = str(path)

    process = subprocess.Popen(
        [
            riak,
            "--tech",
            "ont",
            '--ref_fasta',
            'gpas_uploader/MN908947_no_polyA.fasta',
            '--reads1',
            'tests/files/sample1.fastq.gz',
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

    if pathlib.Path("./samtools").exists():
        samtools = pathlib.Path("./samtools").resolve()

    # or if there is one in the $PATH use that one
    elif shutil.which('samtools') is not None:
        samtools = pathlib.Path(shutil.which('samtools'))

    # assumes samtools is in the $PATH!
    process = subprocess.Popen(
        [
            samtools,
            'fastq',
            '-o',
            tmp_path / pathlib.Path('foo.fastq.gz'),
            'tests/files/sample1.bam',
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = process.communicate()

    # insist that the above command did not fail
    assert process.returncode == 0
