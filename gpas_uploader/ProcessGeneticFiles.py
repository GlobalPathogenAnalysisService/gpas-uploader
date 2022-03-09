#! /usr/bin/env python3

import shutil
import subprocess
import pandas
import pkg_resources
from pathlib import Path

import gpas_uploader

def locate_bam_binary():

    if Path("./samtools").exists():
        return str(Path("./samtools").resolve())

    # or if there is one in the $PATH use that one
    elif shutil.which('samtools') is not None:
        return str(Path(shutil.which('samtools')))

    else:
        raise GpasError({"BAM conversion": "samtools not found"})

def locate_riak_binary():

    # if there is a local riak use that one
    # (as will be the case inside the Electron client)
    if Path("./readItAndKeep").exists():
        return str(Path("./readItAndKeep").resolve())

    # or if there is one in the $PATH use that one
    elif shutil.which('readItAndKeep') is not None:
        return str(Path(shutil.which('readItAndKeep')))

    else:
        raise GpasError({"decontamination": "read removal tool not found"})


def convert_bam_paired_reads(row, wd):

    samtools = locate_bam_binary()

    stem = row['bam'].split('.bam')[0]

    process1 = subprocess.Popen(
        [
            samtools,
            'sort',
            '-n',
            wd / Path(row['bam'])
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    process2 = subprocess.run(
        [
            samtools,
            'fastq',
            '-N',
            '-1',
            wd / Path(stem + "_1.fastq.gz"),
            '-2',
            wd / Path(stem+"_2.fastq.gz"),
        ],
        stdin = process1.stdout,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE
    )

    # to stop a race condition
    process1.wait()

    # insist that the above command did not fail
    assert process2.returncode == 0, 'samtools command failed'

    return(pandas.Series([stem + "_1.fastq.gz", stem + "_2.fastq.gz"]))

def convert_bam_unpaired_reads(row, wd):

    samtools = locate_bam_binary()

    stem = row['bam'].split('.bam')[0]

    process = subprocess.Popen(
        [
            samtools,
            'fastq',
            '-o',
            wd / Path(stem + '.fastq.gz'),
            wd / Path(row['bam'])
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # wait for it to finish otherwise the file will not be present
    process.wait()

    # successful completion
    assert process.returncode == 0, 'samtools command failed'

    # now that we have a FASTQ, add it to the dict
    return(stem + '.fastq.gz')

def remove_pii_unpaired_reads(row, wd, outdir):

    gpas_uploader.dmsg(row.sample_name, "started", msg={"file": str(row.fastq)}, json=True)

    riak = locate_riak_binary()

    ref_genome = pkg_resources.resource_filename("gpas_uploader", 'data/MN908947_no_polyA.fasta')

    riak_command = [
        riak,
        "--tech",
        "ont",
        "--enumerate_names",
        "--ref_fasta",
        ref_genome,
        "--reads1",
        wd / Path(row.fastq),
        "--outprefix",
        str(outdir / row.sample_name),
    ]

    process = subprocess.Popen(
                riak_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

    # wait for it to finish otherwise the file will not be present
    process.wait()

    # successful completion
    assert process.returncode == 0, 'riak command failed'

    fq = outdir / f"{row.sample_name}.reads.fastq.gz"

    gpas_uploader.dmsg(row.sample_name, "completed", msg={"file": str(row.fastq), "cleaned": str(fq)}, json=True)

    return(str(fq))

def remove_pii_paired_reads(row, wd, outdir):
    gpas_uploader.dmsg(row.sample_name, "started", msg={"file": str(row.fastq1)}, json=True)
    gpas_uploader.dmsg(row.sample_name, "started", msg={"file": str(row.fastq2)}, json=True)

    riak = locate_riak_binary()

    ref_genome = pkg_resources.resource_filename("gpas_uploader", 'data/MN908947_no_polyA.fasta')

    riak_command = [
        riak,
        "--tech",
        "illumina",
        "--enumerate_names",
        "--ref_fasta",
        ref_genome,
        "--reads1",
        wd / Path(row.fastq1),
        "--reads2",
        wd / Path(row.fastq2),
        "--outprefix",
        outdir / Path(row.sample_name),
    ]

    process = subprocess.Popen(
                riak_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

    # wait for it to finish otherwise the file will not be present
    process.wait()

    # successful completion
    assert process.returncode == 0, 'riak command failed'

    fq1 = outdir / f"{row.sample_name}.reads_1.fastq.gz"
    fq2 = outdir / f"{row.sample_name}.reads_2.fastq.gz"

    gpas_uploader.dmsg(row.sample_name, "completed", msg={"file": str(row.fastq1), 'cleaned': str(fq1)}, json=True)
    gpas_uploader.dmsg(row.sample_name, "completed", msg={"file": str(row.fastq2), 'cleaned': str(fq2)}, json=True)

    return(pandas.Series([str(fq1), str(fq2)]))
