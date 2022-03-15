#! /usr/bin/env python3

import json
import copy
import platform
from pathlib import Path
import hashlib
import datetime

import pandas
import pandera
from pandarallel import pandarallel

import gpas_uploader

def hash_paired_reads(row, wd):
    """Calculate the MD5 and SHA hashes for two FASTQ files containing paired reads.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row: pandas.Series
        supplied by pandas.DataFrame.apply
    wd: pathlib.Path
        the working directory

    Returns
    -------
    pandas.Series
    """
    fq1md5, fq1sha = hash_fastq(wd / row.fastq1)
    fq2md5, fq2sha = hash_fastq(wd / row.fastq2)
    return(pandas.Series([fq1md5, fq1sha, fq2md5, fq2sha]))


def hash_unpaired_reads(row, wd):
    """Calculate the MD5 and SHA hashes for a FASTQ file containing unpaired reads.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row: pandas.Series supplied by pandas.DataFrame.apply
    wd: pathlib.Path of the working directory

    Returns
    -------
    pandas.Series
    """
    fqmd5, fqsha = hash_fastq(wd / row.fastq)
    return(pandas.Series([fqmd5, fqsha]))


def hash_fastq(filename):
    """Calculate the MD5 and SHA hashes of a file.

    Parameters
    ---------
    filename: pathlib.Path
        the file to hash

    Returns
    -------
    str
        MD5 hash of FASTQ file
    str
        SHA hash of FASTQ file
    """
    md5 = hashlib.md5()
    sha = hashlib.sha256()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
            sha.update(chunk)
    return md5.hexdigest(), sha.hexdigest()


def build_errors(err):
    """Parse the errors returned from the panderas class.

    Parameters
    ----------
    err: pandera.errors.SchemaErrors

    Returns
    -------
    pandas.DataFrame(columns=['gpas_sample_name', 'error_message'])
    """
    failures = err.failure_cases
    failures.rename(columns={'index':'gpas_sample_name'}, inplace=True)
    failures['error_message'] = failures.apply(format_error, axis=1)
    return(failures[['gpas_sample_name', 'error_message']])


def format_error(row):
    """Mong the panderas errors into more user-friendly error messages

    Returns
    -------
    str
        error message, defaults to 'problem in <field_name> field'
    """
    if row.check == 'column_in_schema':
        return('unexpected column ' + row.failure_case + ' found in upload CSV')
    if row.check == 'column_in_dataframe':
        return('column ' + row.failure_case + ' missing from upload CSV')
    elif row.column == 'country' and row.check[:4] == 'isin':
        return(row.failure_case + " is not a valid ISO-3166-1 country")
    elif row.column == 'region' and row.check[:4] == 'isin':
        return(row.failure_case + " is not a valid ISO-3166-2 region for the specified country")
    elif row.column == 'control' and row.check[:4] == 'isin':
        return(row.failure_case + ' in the control field is not valid: field must be either empty or contain the one of the keywords positive or negative')
    elif row.column == 'host' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword human')
    elif row.column == 'specimen_organism' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword SARS-CoV-2')
    elif row.column == 'primer_scheme' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword auto')
    elif row.column == 'instrument_platform':
        if row.gpas_sample_name is None:
            return(row.column + ' must be unique')
        if row.check[:4] == 'isin':
            return(row.column + ' can only contain one of the keywords Illumina or Nanopore')
    elif row.column == 'collection_date':
        if row.gpas_sample_name is None:
            return(row.column + ' must be in form YYYY-MM-DD and cannot include the time')
        if row.check[:4] == 'less':
            return(row.column + ' cannot be in the future')
        if row.check[:7] == 'greater':
            return(row.column + ' cannot be before 2019-01-01')
    elif row.column in ['fastq1', 'fastq2', 'fastq']:
        if row.check == 'field_uniqueness':
            return(row.column + ' must be unique in the upload CSV')
    elif row.check[:11] == 'str_matches':
        allowed_chars = row.check.split('[')[1].split(']')[0]
        return row.column + ' can only contain characters (' + allowed_chars + ')'

    if row.column is None:
        return("problem")
    else:
        return("problem in "+ row.column + ' field')


def check_files_exist(row, file_extension, wd):
    """"Check if a genetic file exists.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row : pandas.Series
    file_extension: str
        the type of genetic file, one of fastq1, fastq2 or bam
    wd: pathlib.Path
        the working directory

    Returns
    -------
    None or error message if file does not exist
    """
    if not (wd / row[file_extension]).is_file():
        return(row[file_extension] + ' does not exist')
    else:
        if (wd / row[file_extension]).stat().st_size < 100:
            return(row[file_extension] + ' is too small (< 100 bytes)')
        else:
            return(None)


def check_files_exist_in_df(df, file_extension, wd):
    """Check if the genetic files specified in upload CSV exist.

    Calls check_files_exist using the pandas.DataFrame.apply method

    Parameters
    ----------
    df: pandas.DataFrame
    file_extension: str
        the type of genetic file, one of fastq1, fastq2 or bam
    wd:

    """
    df['error_message'] = df.apply(check_files_exist, args=(file_extension, wd,), axis=1)
    result = df[df.error_message.notna()]
    if result.empty:
        return(True, None)
    else:
        err = pandas.DataFrame(result.error_message, columns=['error_message'])
        err.reset_index(inplace=True)
        err.rename(columns={'name': 'sample_name'}, inplace=True)
        return(False, err)

def check_tags(row, allowed_tags):
    """Check that the tags all match a supplied set of allowed tags.

    Designed to be used with pandas.DataFrame.apply

    Returns
    -------
    bool
        True if all match, otherwise False
    """
    tags_ok = True
    cols = row['tags'].split(':')
    for i in cols:
        if i not in allowed_tags:
            tags_ok = False
    return(tags_ok)

class Batch:
    """
    Batch of FASTQ/BAM files for upload to GPAS

    Parameters
    ----------
    upload_csv : filename
        path to the upload CSV specifying the samples to be uploaded
    run_parallel : bool
        if True, use pandarallel to remove PII reads in parallel (default False)

    The upload CSV file is stored internally as a pandas.Dataframe. If the upload CSV
    file specifies BAM files these are first converted to FASTQ files using samtools.
    The upload CSV is then validated using pandera.SchemaModels. Any errors are stored in the instance variable errors.

    Example
    -------
    >>> a = Batch('examples/illumina-upload-csv-pass.csv')
    >>> len(a.df)
    3
    >>> len(a.errors)
    0

    """
    def __init__(self, upload_csv, run_parallel=False, tags_file=None, output_json=False, reference_genome=None):

        # instance variables
        self.upload_csv = Path(upload_csv)
        self.wd = self.upload_csv.parent
        self.output_json = output_json
        self.reference_genome = reference_genome
        self.validation_errors = pandas.DataFrame(None, columns=['gpas_sample_name', 'error_message'])

        # store the upload CSV internally as a pandas.DataFrame
        self.df = pandas.read_csv(self.upload_csv, dtype=object)

        # number the runs 1,2,3..
        self.run_numbers = list(self.df.run_number.unique())
        self.run_number_lookup = {}
        for i in range(len(self.run_numbers)):
            self.run_number_lookup[self.run_numbers[i]] = i

        # create the assumed unique GPAS batch id and sample names
        self.gpas_batch = 'B-' + gpas_uploader.create_batch_name(self.upload_csv)
        self.df['gpas_batch'] = self.gpas_batch
        self.df[['gpas_sample_name', 'gpas_run_number']] = self.df.apply(gpas_uploader.assign_gpas_identifiers, args=(self.run_number_lookup,), axis=1)
        self.df.set_index('gpas_sample_name', inplace=True)

        # if the upload CSV contains BAMs, check they exist, then convert to FASTQ(s)
        if 'bam' in self.df.columns:

            # check that the BAM files exist in the working directory
            bam_files = copy.deepcopy(self.df[['bam']])
            files_ok, err = check_files_exist_in_df(bam_files, 'bam', self.wd)

            if files_ok:
                self._convert_bams()
            else:
                # if the files don't exist, add to the errors DataFrame
                self.validation_errors = self.validation_errors.append(err)

        # have to treat the upload CSV differently depending on whether it specifies
        # paired or unpaired reads
        if 'fastq' in self.df.columns:
            self.sequencing_platform = 'Nanopore'

            fastq_files = copy.deepcopy(self.df[['fastq']])
            files_ok, err = check_files_exist_in_df(fastq_files, 'fastq', self.wd)
            if not files_ok:
                self.validation_errors = self.validation_errors.append(err)

            try:
                gpas_uploader.NanoporeFASTQCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.validation_errors = self.validation_errors.append(build_errors(err))

        elif 'fastq2' in self.df.columns and 'fastq1' in self.df.columns:
            self.sequencing_platform = 'Illumina'

            for i in ['fastq1', 'fastq2']:
                fastq_files = copy.deepcopy(self.df[[i]])
                files_ok, err = check_files_exist_in_df(fastq_files, i, self.wd)
                if not files_ok:
                    self.validation_errors = self.validation_errors.append(err)

            try:
                gpas_uploader.IlluminaFASTQCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.validation_errors = self.validation_errors.append(build_errors(err))


        # allow a user to specify a file containing tags to validate against
        if tags_file is not None:
            allowed_tags = set()
            with open(tags_file, 'r') as INPUT:
                for line in INPUT:
                    allowed_tags.add(line.rstrip())

            self.df['tags_ok'] = self.df.apply(check_tags, args=(allowed_tags,), axis=1)
            a = copy.deepcopy(self.df[~self.df['tags_ok']])
            a['error_message'] = 'tags do not validate'
            a.reset_index(inplace=True)
            a = a[['gpas_sample_name', 'error_message']]
            self.validation_errors = self.validation_errors.append(a)

        self.validation_errors.set_index('gpas_sample_name', inplace=True)
        self.df.reset_index(inplace=True)
        self.df.set_index('gpas_sample_name', inplace=True)

        # no errors have been returned
        if len(self.validation_errors) == 0:

            self.valid = True
            samples = []
            for idx,row in self.df.iterrows():
                if self.sequencing_platform == 'Illumina':
                    samples.append({"sample": idx, "files": [row.fastq1, row.fastq2]})
                else:
                    samples.append({"sample": idx, "files": [row.fastq]})

            self.validation_json = {"validation": {"status": "completed", "samples": samples}}

        # errors have been returned
        else:

            self.valid = False
            errors = []
            for idx,row in self.validation_errors.iterrows():
                errors.append({"sample": idx, "error": row.error_message})

            self.validation_json = {"validation": {"status": "failure", "samples": errors}}

    def _convert_bams(self, run_parallel=False):
        """Private method that converts BAM files to FASTQ files.

        Paired or unpaired FASTQ files are produced depending on the instrument_platform
        specified in the upload CSV file. If run_parallel is True this uses pandarallel
        to run samtools in parallel, except on Windows.

        Parameters
        ----------
        run_parallel: bool
            if True, run readItAndKeep in parallel using pandarallel (default False)
        """

        # From https://github.com/nalepae/pandarallel
        # "On Windows, Pandaral·lel will works only if the Python session is executed from Windows Subsystem for Linux (WSL)"
        # Hence disable parallel processing for Windows for now
        if platform.system() == 'Windows' or run_parallel is False:

            # run samtools to produce paired/unpaired reads depending on the technology
            if self.df.instrument_platform.unique()[0] == 'Illumina':
                self.df[['fastq1', 'fastq2']] = self.df.apply(gpas_uploader.convert_bam_paired_reads, args=(self.wd,), axis=1)

            elif self.df.instrument_platform.unique()[0] == 'Nanopore':
                self.df['fastq'] = self.df.apply(gpas_uploader.convert_bam_unpaired_reads, args=(self.wd,), axis=1)

        else:

            pandarallel.initialize(progress_bar=False, verbose=0)

            # run samtools to produce paired/unpaired reads depending on the technology
            if self.df.instrument_platform.unique()[0] == 'Illumina':
                self.df[['fastq1', 'fastq2']] = self.df.parallel_apply(gpas_uploader.convert_bam_paired_reads, args=(self.wd,), axis=1)

            elif self.df.instrument_platform.unique()[0] == 'Nanopore':
                self.df['fastq'] = self.df.parallel_apply(gpas_uploader.convert_bam_unpaired_reads, args=(self.wd,), axis=1)

        # now that we've added fastq column(s) we need to remove the bam column
        # so that the DataFrame doesn't fail validation
        self.df.drop(columns='bam', inplace=True)

    def decontaminate(self, run_parallel=False, outdir=Path('/tmp/')):
        """Remove personally identifiable genetic reads from the FASTQ files in the batch.

        Parameters
        ----------
        outdir : str
            the folder where to write the decontaminated FASTQ files (Default is /tmp)
        run_parallel: bool
            if True, run readItAndKeep in parallel using pandarallel (default False)
        """

        self.decontamination_errors = pandas.DataFrame(None, columns=['gpas_sample_name', 'error_message'])

        # From https://github.com/nalepae/pandarallel
        # "On Windows, Pandaral·lel will works only if the Python session is executed from Windows Subsystem for Linux (WSL)"
        # Hence disable parallel processing for Windows for now
        if platform == 'Windows' or run_parallel is False:

            if self.sequencing_platform == 'Nanopore':
                self.df['r_uri'] = self.df.apply(gpas_uploader.remove_pii_unpaired_reads, args=(self.reference_genome, self.wd, outdir, self.output_json), axis=1)

            elif self.sequencing_platform == 'Illumina':
                self.df[['r1_uri', 'r2_uri']] = self.df.apply(gpas_uploader.remove_pii_paired_reads, args=(self.reference_genome, self.wd, outdir, self.output_json), axis=1)

        else:

            pandarallel.initialize(verbose=0)

            if self.sequencing_platform == 'Nanopore':
                self.df['r_uri'] = self.df.parallel_apply(gpas_uploader.remove_pii_unpaired_reads, args=(self.reference_genome, self.wd, outdir, self.output_json), axis=1)

            elif self.sequencing_platform == 'Illumina':
                self.df[['r1_uri', 'r2_uri']] = self.df.parallel_apply(gpas_uploader.remove_pii_paired_reads, args=(self.reference_genome, self.wd, outdir, self.output_json), axis=1)


        if self.sequencing_platform == 'Illumina':

            self.df[['r1_md5', 'r1_sha', 'r2_md5', 'r2_sha']] = self.df.apply(hash_paired_reads, args=(self.wd,), axis=1)

            for i in ['r1_uri', 'r2_uri']:
                fastq_files = copy.deepcopy(self.df[[i]])
                files_ok, err = check_files_exist_in_df(fastq_files, i, self.wd)
                if not files_ok:
                    self.decontamination_errors = self.decontamination_errors.append(err)


        elif self.sequencing_platform == 'Nanopore':
            self.df[['r_md5', 'r_sha',]] = self.df.apply(hash_unpaired_reads, args=(self.wd,), axis=1)

            fastq_files = copy.deepcopy(self.df[['r_uri']])
            files_ok, err = check_files_exist_in_df(fastq_files, 'r_uri', self.wd)
            if not files_ok:
                self.decontamination_errors = self.decontamination_errors.append(err)

        if len(self.decontamination_errors)>0:

            self.decontamination_successful = False

            errors = []
            for idx,row in self.decontamination_errors.iterrows():
                errors.append({"sample": row.gpas_sample_name, "error": row.error_message})

            self.decontamination_json  = { "submission": {
                "status": "failure",
                "samples": errors } }
        else:

            self.decontamination_successful = True

            # populate the post-decontamination JSON for passing to the Electron Client app
            self.decontamination_json = self._build_submission()

    def _build_submission(self):
        """Prepare the JSON payload for the GPAS Upload app

        Returns
        -------
            dict : JSON payload to pass to GPAS Electron upload app via STDOUT
        """

        self.df.reset_index(inplace=True)

        self.sample_sheet = copy.deepcopy(self.df[['batch', 'run_number', 'sample_name', 'gpas_batch', 'gpas_run_number', 'gpas_sample_name']])

        self.sample_sheet.rename(columns={'batch': 'local_batch', 'run_number': 'local_run_number', 'sample_name': 'local_sample_name'}, inplace=True)

        self.df.set_index('gpas_sample_name', inplace=True)

        # determine the current time and time zone
        currentTime = datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat(timespec='milliseconds')
        tzStartIndex = len(currentTime) - 6
        currentTime = currentTime[:tzStartIndex] + "Z" + currentTime[tzStartIndex:]

        samples = []
        for idx,row in self.df.iterrows():
            sample = {  "sample": idx,
                        "run_number": row.gpas_run_number,
                        "tags": row.tags.split(':'),
                        "control": row.control,
                        "collection_date": row.collection_date,
                        "country": row.country,
                        "region": row.region,
                        "district": row.district,
                        "specimen": row.specimen_organism,
                        "host": row.host,
                        "instrument": { 'platform': row.instrument_platform},
                        "primer_scheme": row.primer_scheme,
                        }
            if self.sequencing_platform == 'Illumina':
                sample['pe_reads'] = {"r1_uri": row.r1_uri,
                                      "r1_md5": row.r1_md5,
                                      "r2_uri": row.r2_uri,
                                      "r2_md5": row.r2_md5 }
            elif self.sequencing_platform == 'Nanopore':
                sample['se_reads'] = {"uri": row.r_uri,
                                      "md5": row.r_md5}
            samples.append(sample)

        return {
            "submission": {
                "status": "completed",
                "batch": {
                    "file_name": self.gpas_batch,
                    "uploaded_on": currentTime,
                    "run_numbers": [i for i in self.run_number_lookup.values()],
                    "samples": [i for i in samples],
                }
            }
        }
