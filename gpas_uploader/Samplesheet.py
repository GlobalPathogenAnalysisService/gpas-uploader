#! /usr/bin/env python3

import pathlib, json

import pandas
import pandera
import gpas_uploader_validate

def build_errors(errors, err):

    failures = err.failure_cases
    failures.rename(columns={'index':'sample'}, inplace=True)
    failures['error'] = failures.apply(format_error, axis=1)
    errors = errors.append(failures[['sample', 'error']])
    return(errors)


def format_error(row):
    return("problem in "+ row.column + ' field')


def build_samples(row):
    return(json.dumps([row.fastq1,row.fastq2]))


class Samplesheet:

    def __init__(self, upload_csv):

        self.upload_csv = pathlib.Path(upload_csv)
        self.errors = pandas.DataFrame(None, columns=['sample', 'error'])

        self.df = pandas.read_csv(self.upload_csv, dtype=object)
        assert 'name' in self.df.columns
        self.df.set_index('name', inplace=True)

        if 'fastq' in self.df.columns:
            self.sequencing_platform = 'Nanopore'
            self.file_type = 'fastq.gz'

            try:
                gpas_uploader_validate.NanoporeFASTQCheckSchema.validate(self.df, lazy=True)

            except pandera.errors.SchemaErrors as err:
                self.errors = build_errors(self.errors, err)

        elif 'fastq2' in self.df.columns and 'fastq1' in self.df.columns:
            self.sequencing_platform = 'Illumina'
            self.file_type = 'fastq.gz'

            try:
                gpas_uploader_validate.IlluminaFASTQCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.errors = build_errors(self.errors, err)

        elif 'bam' in self.df.columns:
            self.file_type = 'bam'

            try:
                gpas_uploader_validate.BAMCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.errors = build_errors(self.errors, err)

        if len(self.errors) == 0:
            files = self.df.apply(build_samples,axis=1)
            self.samples = pandas.DataFrame(files, columns=['files'])
            self.samples.reset_index(inplace=True)
