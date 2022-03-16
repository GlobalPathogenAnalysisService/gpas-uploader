import pytest
import pathlib
import shutil
import subprocess
import importlib
import sys


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
