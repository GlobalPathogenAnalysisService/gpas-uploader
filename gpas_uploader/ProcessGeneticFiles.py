#! /usr/bin/env python3

import shutil
import os
import sys
import subprocess
import importlib.resources
from pathlib import Path

import pandas

import gpas_uploader

def locate_bam_binary():
    """Locate samtools by searching the $PATH and in the current folder.

    Returns
    -------
    str
        path to the samtools binary
    """
    # if there is a local samtools use that one
    # (as will be the case inside the Electron client)
    if Path("./samtools").exists():
        return str(Path("./samtools").resolve())

    # or if there is one in the $PATH use that one
    elif shutil.which('samtools') is not None:
        return str(Path(shutil.which('samtools')))

    else:
        raise gpas_uploader.GpasError({"BAM conversion": "samtools not found"})

def locate_riak_binary():
    """Locate ReadItAndKeep by searching the $PATH and in the current folder.

    Returns
    -------
    str
        path to the ReadItAndKeep binary
    """
    # if there is a local riak use that one
    # (as will be the case inside the Electron client)
    if Path("./readItAndKeep").exists():
        return str(Path("./readItAndKeep").resolve())

    # or if there is one in the $PATH use that one
    elif shutil.which('readItAndKeep') is not None:
        return str(Path(shutil.which('readItAndKeep')))

    else:
        raise gpas_uploader.GpasError({"decontamination": "read removal tool not found"})


def convert_bam_paired_reads(row, wd):
    """Convert a BAM file into a pair of FASTQ files.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row : pandas.Series
        row from pandas.DataFrame
    wd : pathlib.Path
        working directory

    Returns
    -------
    pandas.Series
        containing paths to both FASTQ files
    """

    # locate the samtools binary
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
    """Convert a BAM file into a single unpaired FASTQ file.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row : pandas.Series
        row from pandas.DataFrame
    wd : pathlib.Path
        working directory

    Returns
    -------
    str
        path to the created FASTQ file
    """
    samtools = locate_bam_binary()

    stem = row['bam'].split('.bam')[0]

    process = subprocess.Popen(
        [
            samtools,
            'fastq',
            '-0',
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

def remove_pii_unpaired_reads(row, reference_genome, wd, outdir, output_json):
    """Remove personally identifiable reads from an unpaired FASTQ file using ReadItAndKeep.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row : pandas.Series
        row from pandas.DataFrame
    wd : pathlib.Path
        working directory
    outdir : pathlib.Path
        output directory

    Returns
    -------
    str
        path to the decontaminated FASTQ file
    """
    # PWF Sprint 11 hack to push JSON decontamination block later to help EC
    # if output_json:
    #     gpas_uploader.dmsg(row.name, "started", msg={"file": str(row.fastq)}, json=True)

    riak = locate_riak_binary()

    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        ref_genome = 'MN908947_no_polyA.fasta'
    else:
        if reference_genome is None:
            with importlib.resources.path("gpas_uploader", 'MN908947_no_polyA.fasta') as path:
                ref_genome = str(path)
        else:
            ref_genome = reference_genome

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
        str(outdir / row.name),
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

    fq = outdir / f"{row.name}.reads.fastq.gz"

    # PWF Sprint 11 hack to push JSON decontamination block later to help EC
    # if output_json:
    #     gpas_uploader.dmsg(row.name, "completed", msg={"file": str(row.fastq), "cleaned": str(fq)}, json=True)

    return(str(fq))

def remove_pii_paired_reads(row, reference_genome, wd, outdir, output_json):
    """Remove personally identifiable reads from a pair of FASTQ files using ReadItAndKeep.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row : pandas.Series
        row from pandas.DataFrame
    wd : pathlib.Path
        working directory
    outdir : pathlib.Path
        output directory

    Returns
    -------
    pandas.Series
        paths to the decontaminated pair of FASTQ files
    """
    # PWF Sprint 11 hack to push JSON decontamination block later to help EC
    # if output_json:
    #     gpas_uploader.dmsg(row.name, "started", msg={"file": str(row.fastq1)}, json=True)
    #     gpas_uploader.dmsg(row.name, "started", msg={"file": str(row.fastq2)}, json=True)

    riak = locate_riak_binary()

    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        ref_genome = 'MN908947_no_polyA.fasta'
    else:
        if reference_genome is None:
            with importlib.resources.path("gpas_uploader", 'MN908947_no_polyA.fasta') as path:
                ref_genome = str(path)
        else:
            ref_genome = reference_genome

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
        outdir / Path(row.name),
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

    fq1 = outdir / f"{row.name}.reads_1.fastq.gz"
    fq2 = outdir / f"{row.name}.reads_2.fastq.gz"

    # PWF Sprint 11 hack to push JSON decontamination block later to help EC
    # if output_json:
    #     gpas_uploader.dmsg(row.name, "completed", msg={"file": str(row.fastq1), 'cleaned': str(fq1)}, json=True)
    #     gpas_uploader.dmsg(row.name, "completed", msg={"file": str(row.fastq2), 'cleaned': str(fq2)}, json=True)

    return(pandas.Series([str(fq1), str(fq2)]))
