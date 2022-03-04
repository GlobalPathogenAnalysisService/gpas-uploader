#! /usr/bin/env python3

import shutil, json, subprocess, copy, pkg_resources, platform
from pathlib import Path

import pandas
import pandera
from pandarallel import pandarallel

import gpas_uploader_validate

def build_errors(err):

    failures = err.failure_cases
    failures.rename(columns={'index':'sample'}, inplace=True)
    failures['error'] = failures.apply(format_error, axis=1)
    return(failures)


def format_error(row):
    return("problem in "+ row.column + ' field')


def locate_bam_binary():

    if Path("./samtools").exists():
        return Path("./samtools").resolve()

    # or if there is one in the $PATH use that one
    elif shutil.which('samtools') is not None:
        return Path(shutil.which('samtools'))

    else:
        raise GpasError({"BAM conversion": "samtools not found"})

def locate_riak_binary():

    # if there is a local riak use that one
    # (as will be the case inside the Electron client)
    if Path("./readItAndKeep").exists():
        return Path("./readItAndKeep").resolve()

    # or if there is one in the $PATH use that one
    elif shutil.which('readItAndKeep') is not None:
        return Path(shutil.which('readItAndKeep'))

    else:
        raise GpasError({"decontamination": "read removal tool not found"})


def build_samples(row, platform, wd):
    if platform == 'Illumina':
        return(json.dumps([str(wd / row.fastq1), str(wd / row.fastq2)]))
    elif platform == 'Nanopore':
        return(json.dumps([str(wd / row.fastq)]))
    else:
        raise GpasError('sequencing platform not recognised')


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

    riak = locate_riak_binary()

    ref_genome = pkg_resources.resource_filename("gpas_uploader", 'data/MN908947_no_polyA.fasta')

    process = subprocess.Popen(
                [
                    riak,
                    "--enumerate_names",
                    "--ref_fasta",
                    ref_genome,
                    "--reads1",
                    row.fastq,
                    "--outprefix",
                    outdir / row.index,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )


def remove_pii_paired_reads(row, wd, outdir):

    riak = locate_riak_binary()

    ref_genome = pkg_resources.resource_filename("gpas_uploader", 'data/MN908947_no_polyA.fasta')

    process = subprocess.Popen(
                [
                    riak,
                    "--enumerate_names",
                    "--ref_fasta",
                    ref_genome,
                    "--reads1",
                    wd / Path(row.fastq1),
                    "--reads2",
                    wd / Path(row.fastq2),
                    "--outprefix",
                    outdir + '/' + str(row.name),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

    # wait for it to finish otherwise the file will not be present
    process.wait()

    # successful completion
    assert process.returncode == 0, 'riak command failed'


def check_files_exist(row, file_extension, wd):
    if not (wd / row[file_extension]).is_file():
        return(file_extension + ' does not exist')
    else:
        return(None)


def check_files_exist2(df, file_extension, wd):
    df['error'] = df.apply(check_files_exist, args=(file_extension, wd,), axis=1)
    result = df[df.error.notna()]
    if result.empty:
        return(True, None)
    else:
        err = pandas.DataFrame(result.error, columns=['error'])
        err.reset_index(inplace=True)
        err.rename(columns={'name': 'sample'}, inplace=True)
        return(False, err)


class Samplesheet:

    def __init__(self, upload_csv, parallel=False):

        self.upload_csv = Path(upload_csv)
        self.wd = self.upload_csv.parent
        self.parallel = parallel

        self.errors = pandas.DataFrame(None, columns=['sample', 'error'])

        self.df = pandas.read_csv(self.upload_csv, dtype=object)
        self.df.set_index('name', inplace=True)

        # if the upload CSV contains BAMs, validate, then convert
        if 'bam' in self.df.columns:

            # validate the upload CSV
            try:
                gpas_uploader_validate.BAMCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.errors = self.errors.append(build_errors(err))

            # check that the BAM files exist in the working directory
            bam_files = copy.deepcopy(self.df[['bam']])
            files_ok, err = check_files_exist2(bam_files, 'bam', self.wd)

            if files_ok:

                self.convert_bams()

            else:
                # if the files don't exist, add to the errors DataFrame
                self.errors = self.errors.append(err)

        if 'fastq' in self.df.columns:
            self.sequencing_platform = 'Nanopore'

            try:
                gpas_uploader_validate.NanoporeFASTQCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.errors = self.errors.append(build_errors(err))

        elif 'fastq2' in self.df.columns and 'fastq1' in self.df.columns:
            self.sequencing_platform = 'Illumina'

            try:
                gpas_uploader_validate.IlluminaFASTQCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.errors = self.errors.append(build_errors(err))

        if len(self.errors) == 0:
            files = self.df.apply(build_samples,args=(self.sequencing_platform,self.wd), axis=1)
            self.samples = pandas.DataFrame(files, columns=['files'])
            self.samples.reset_index(inplace=True)

    def convert_bams(self):

        # From https://github.com/nalepae/pandarallel
        # "On Windows, Pandaral·lel will works only if the Python session is executed from Windows Subsystem for Linux (WSL)"
        # Hence disable parallel processing for Windows for now
        if platform.system() == 'Windows' or self.parallel is False:

            # run samtools to produce paired/unpaired reads depending on the technology
            if self.df.instrument_platform.unique()[0] == 'Illumina':

                self.df[['fastq1', 'fastq2']] = self.df.apply(convert_bam_paired_reads, args=(self.wd,), axis=1)

            elif self.df.instrument_platform.unique()[0] == 'Nanopore':

                self.df['fastq'] = self.df.apply(convert_bam_unpaired_reads, args=(self.wd,), axis=1)

        else:

            pandarallel.initialize(progress_bar=False, verbose=0)

            # run samtools to produce paired/unpaired reads depending on the technology
            if self.df.instrument_platform.unique()[0] == 'Illumina':

                self.df[['fastq1', 'fastq2']] = self.df.parallel_apply(convert_bam_paired_reads, args=(self.wd,), axis=1)

            elif self.df.instrument_platform.unique()[0] == 'Nanopore':

                self.df['fastq'] = self.df.parallel_apply(convert_bam_unpaired_reads, args=(self.wd,), axis=1)

        # now that we've added fastq column(s) we need to remove the bam column
        # so that the DataFrame doesn't fail validation
        self.df.drop(columns='bam', inplace=True)

    def decontaminate(self, outdir='/tmp'):

        # From https://github.com/nalepae/pandarallel
        # "On Windows, Pandaral·lel will works only if the Python session is executed from Windows Subsystem for Linux (WSL)"
        # Hence disable parallel processing for Windows for now
        if platform == 'Windows' or self.parallel is False:

            if self.sequencing_platform == 'Nanopore':
                self.df.apply(remove_pii_unpaired_reads, args=(self.wd, outdir,), axis=1)

            elif self.sequencing_platform == 'Illumina':
                self.df.apply(remove_pii_paired_reads, args=(self.wd, outdir,), axis=1)

        else:

            pandarallel.initialize(progress_bar=False, verbose=0)

            if self.sequencing_platform == 'Nanopore':
                self.df.parallel_apply(remove_pii_unpaired_reads, args=(self.wd, outdir,), axis=1)

            elif self.sequencing_platform == 'Illumina':
                self.df.parallel_apply(remove_pii_paired_reads, args=(self.wd, outdir,), axis=1)