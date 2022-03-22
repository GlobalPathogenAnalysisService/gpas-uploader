#! /usr/bin/env python3

import json
import copy
import re
import platform
from pathlib import Path
import hashlib
import datetime
import requests

import pandas
import pandera
from pandarallel import pandarallel
from tqdm.auto import tqdm
tqdm.pandas()

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
    fq1md5, fq1sha = hash_fastq(row.r1_uri)
    fq2md5, fq2sha = hash_fastq(row.r2_uri)
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
    fqmd5, fqsha = hash_fastq(row.r_uri)
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
    pandas.DataFrame(columns=['sample_name', 'error_message'])
    """
    failures = err.failure_cases
    failures.rename(columns={'index':'sample_name'}, inplace=True)
    failures['error_message'] = failures.apply(format_error, axis=1)
    return(failures[['sample_name', 'error_message']])


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
    elif row.check == 'region_is_valid':
        return("specified regions are not valid ISO-3166-2 regions for the specified country")
    elif row.column == 'country' and row.check[:4] == 'isin':
        return(row.failure_case + " is not a valid ISO-3166-1 country")
    elif row.column == 'region' and row.check[:4] == 'isin':
        return(row.failure_case + " is not a valid ISO-3166-2 region")
    elif row.column == 'control' and row.check[:4] == 'isin':
        return(row.failure_case + ' in the control field is not valid: field must be either empty or contain the one of the keywords positive or negative')
    elif row.column == 'host' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword human')
    elif row.column == 'specimen_organism' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword SARS-CoV-2')
    elif row.column == 'primer_scheme' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword auto')
    elif row.column == 'instrument_platform':
        if row.sample_name is None:
            return(row.column + ' must be unique')
        if row.check[:4] == 'isin':
            return(row.column + ' can only contain one of the keywords Illumina or Nanopore')
    elif row.column == 'collection_date':
        if row.sample_name is None:
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

def rename_unpaired_fastq(row):
    """Rename the unpaired FASTQ file with the GPAS sample name

    Designed to be used with pandas.DataFrame.apply

    Returns
    -------
    str
        path to the renamed FASTQ file
    """

    p = Path(row.r_uri)
    dest_file = Path(row.gpas_sample_name + '.reads.fastq.gz')
    p.rename(p.parent / dest_file)

    return str(p.parent / dest_file)

def rename_paired_fastq(row):
    """Rename the paired FASTQ files with the GPAS sample name

    Designed to be used with pandas.DataFrame.apply

    Returns
    -------
    pandas.Series
        paths to both renamed FASTQ files
    """

    p1, p2 = Path(row.r1_uri), Path(row.r2_uri)
    dest_file1 = Path(row.gpas_sample_name + '.reads_1.fastq.gz')
    dest_file2 = Path(row.gpas_sample_name + '.reads_2.fastq.gz')
    p1.rename(p1.parent / dest_file1)
    p2.rename(p2.parent / dest_file2)

    return pandas.Series([str(p1.parent / dest_file1), str(p2.parent / dest_file2),])

def upload_fastq_paired(row, url, headers):
    if not row.uploaded:
        r1 = requests.put(url + row.name + '.reads_1.fastq.gz', open(row['r1_uri'], 'rb'), headers=headers)
        r2 = requests.put(url+ row.name + '.reads_2.fastq.gz', open(row['r2_uri'], 'rb'), headers=headers)
        return (r1.ok and r2.ok)
    else:
        return True

def upload_fastq_unpaired(row, url, headers):
    if not row.uploaded:
        r = requests.put(url + row.name + '.reads.fastq.gz', open(row['r_uri'], 'rb'), headers=headers)
        return r.ok
    else:
        return True


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
    def __init__(self, upload_csv, token_file=None, environment='production', run_parallel=False, tags_file=None, output_json=False, reference_genome=None):

        assert re.match("^[A-Za-z0-9-_/.]+$", str(upload_csv)), "filename can only contain characters A-Za-z0-9-_."

        # instance variables
        self.upload_csv = Path(upload_csv)
        self.wd = self.upload_csv.parent
        self.output_json = output_json
        self.reference_genome = reference_genome

        assert environment in ['development', 'production', 'staging']
        self.environment = environment

        # store the upload CSV internally as a pandas.DataFrame
        self.df = pandas.read_csv(self.upload_csv, dtype=object)

        # since we get the permitted tags from ORDS, do not allow a user to specify a tags_file if a token is supplied
        assert not (token_file is not None and tags_file is not None), 'cannot specify both a tags file and an access token!'

        # allow a user to specify a file containing tags to validate against
        if tags_file is not None:
            self.permitted_tags = set()
            with open(tags_file, 'r') as INPUT:
                for line in INPUT:
                    self.permitted_tags.add(line.rstrip())
        else:
            self.permitted_tags = None

        if token_file is None:
            self.connect_to_oci = False
        else:
            self.connect_to_oci = True

        if self.connect_to_oci:
            self.access_token, self.headers, self.environment_urls = self._parse_access_token(token_file)
            self.user_name, self.user_organisation, self.permitted_tags = self._call_ords_userOrgDtls()

        # number the runs 1,2,3..
        self.run_number_lookup = {}

        # deal with case when they are all NaN
        if self.df.run_number.isna().all():
            self.run_number_lookup[''] = ''
        else:
            self.run_numbers = list(self.df.run_number.unique())

            gpas_run = 1
            for i in self.run_numbers:
                if pandas.notna(i):
                    self.run_number_lookup[i] = gpas_run
                    gpas_run += 1

        # determine the current time and time zone
        currentTime = datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat(timespec='milliseconds')
        tzStartIndex = len(currentTime) - 6
        self.uploaded_on = currentTime[:tzStartIndex] + "Z" + currentTime[tzStartIndex:]

    def validate(self):
        """Validate the upload CSV.

        If the upload CSV specifies BAM files, these will first be converted to FASTQ files.
        """

        self.validation_errors = pandas.DataFrame(None, columns=['sample_name', 'error_message'])

        self.df.set_index('sample_name', inplace=True)

        # if the upload CSV contains BAMs, check they exist, then convert to FASTQ(s)
        if 'bam' in self.df.columns:

            # check that the BAM files exist in the working directory
            bam_files = copy.deepcopy(self.df[['bam']])
            files_ok, err = check_files_exist_in_df(bam_files, 'bam', self.wd)

            if files_ok:
                self._convert_bams()
            else:
                # if the files don't exist, add to the errors DataFrame
                self.validation_errors = pandas.concat([self.validation_errors,err])

        # have to treat the upload CSV differently depending on whether it specifies
        # paired or unpaired reads
        if 'fastq' in self.df.columns:
            self.sequencing_platform = 'Nanopore'

            fastq_files = copy.deepcopy(self.df[['fastq']])
            files_ok, err = check_files_exist_in_df(fastq_files, 'fastq', self.wd)
            if not files_ok:
                self.validation_errors = pandas.concat([self.validation_errors,err])

            try:
                gpas_uploader.NanoporeFASTQCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.validation_errors = pandas.concat([self.validation_errors, build_errors(err)])

        elif 'fastq2' in self.df.columns and 'fastq1' in self.df.columns:
            self.sequencing_platform = 'Illumina'

            for i in ['fastq1', 'fastq2']:
                fastq_files = copy.deepcopy(self.df[[i]])
                files_ok, err = check_files_exist_in_df(fastq_files, i, self.wd)
                if not files_ok:
                    self.validation_errors = pandas.concat([self.validation_errors,err])

            try:
                gpas_uploader.IlluminaFASTQCheckSchema.validate(self.df, lazy=True)
            except pandera.errors.SchemaErrors as err:
                self.validation_errors = pandas.concat([self.validation_errors, build_errors(err)])

        self.df.reset_index(inplace=True)

        if self.permitted_tags is not None:
            self.df['tags_ok'] = self.df.apply(check_tags, args=(self.permitted_tags,), axis=1)
            a = copy.deepcopy(self.df[~self.df['tags_ok']])
            a['error_message'] = 'tags do not validate'
            a.reset_index(inplace=True)
            a = a[['sample_name', 'error_message']]
            self.validation_errors = pandas.concat([self.validation_errors,a])

        self.df.fillna(value={'run_number':'', 'control':'', 'region': '', 'district': ''}, inplace=True)

        self.validation_errors.set_index('sample_name', inplace=True)

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

    def _parse_access_token(self, token_file):
        """Parse the provided access token and store its contents

        Returns
        -------
        access_token: str
        headers: dict
        environment_urls: dict
        """

        INPUT = open(token_file)
        token_payload = json.load(INPUT)
        access_token = token_payload['access_token']
        headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
        environment_urls = {
            "development": {
                "WORLD_URL": "https://portal.dev.gpas.ox.ac.uk",
                "ORDS_PATH": "/ords/gpasdevpdb1/grep/electron",
                "DASHBOARD_PATH": "/ords/gpasdevpdb1/gpas/r/gpas-portal/lineages-voc",
                "ENV_NAME": "DEV"
            },
            "production": {
                "WORLD_URL": "https://portal.gpas.ox.ac.uk",
                "ORDS_PATH": "/ords/grep/electron",
                "DASHBOARD_PATH": "/ords/gpas/r/gpas-portal/lineages-voc",
                "ENV_NAME": ""
            },
            "staging": {
                "WORLD_URL": "https://portal.staging.gpas.ox.ac.uk",
                "ORDS_PATH": "/ords/gpasuat/grep/electron",
                "DASHBOARD_PATH": "/ords/gpas/r/gpas-portal/lineages-voc",
                "ENV_NAME": "STAGE"
            }
        }
        return(access_token, headers, environment_urls)

    def _call_ords_userOrgDtls(self):
        """Private method that calls ORDS to find out User Details

        Returns
        -------
        user_name: str
        user_organisation: str
        permitted_tags: list
        """
        # build the API URL
        url  = self.environment_urls[self.environment]['WORLD_URL'] + self.environment_urls[self.environment]['ORDS_PATH'] + '/userOrgDtls'

        # make the API call
        a = requests.get(url=url, headers=self.headers)

        # if it fails raise an Exception, otherwise parse the returned content
        if not a.ok:
            a.raise_for_status()
        else:
            result = json.loads(a.content)

        # pull out the required fields
        user_name = result['userOrgDtl'][0]['userName']
        user_organisation = result['userOrgDtl'][0]['organisation']
        permitted_tags = [i['tagName'] for i in result['userOrgDtl'][0]['tags']]

        return(user_name, user_organisation, permitted_tags)

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
                self.sequencing_platform = 'Illumina'
                self.df[['fastq1', 'fastq2']] = self.df.apply(gpas_uploader.convert_bam_paired_reads, args=(self.wd,), axis=1)

            elif self.df.instrument_platform.unique()[0] == 'Nanopore':
                self.sequencing_platform = 'Nanopore'
                self.df['fastq'] = self.df.apply(gpas_uploader.convert_bam_unpaired_reads, args=(self.wd,), axis=1)

            else:
                raise gpas_uploader.GpasError("sequencing_platform not recognised!")

        else:

            pandarallel.initialize(progress_bar=False, verbose=0)

            # run samtools to produce paired/unpaired reads depending on the technology
            if self.df.instrument_platform.unique()[0] == 'Illumina':
                self.sequencing_platform = 'Illumina'
                self.df[['fastq1', 'fastq2']] = self.df.parallel_apply(gpas_uploader.convert_bam_paired_reads, args=(self.wd,), axis=1)

            elif self.df.instrument_platform.unique()[0] == 'Nanopore':
                self.sequencing_platform = 'Nanopore'
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

        self.decontamination_errors = pandas.DataFrame(None, columns=['sample_name', 'error_message'])

        self.df.set_index('sample_name', inplace=True)

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
                    self.decontamination_errors = pandas.concat([self.decontamination_errors,err])

        elif self.sequencing_platform == 'Nanopore':
            self.df[['r_md5', 'r_sha',]] = self.df.apply(hash_unpaired_reads, args=(self.wd,), axis=1)

            fastq_files = copy.deepcopy(self.df[['r_uri']])
            files_ok, err = check_files_exist_in_df(fastq_files, 'r_uri', self.wd)
            if not files_ok:
                self.decontamination_errors = pandas.concat([self.decontamination_errors,err])

        self.df.reset_index(inplace=True)

        if self.connect_to_oci:

            # first make a list of MD5s, one per sample
            if self.sequencing_platform == 'Illumina':
                md5s = list(self.df.r1_md5)
            else:
                md5s = list(self.df.r_md5)

            # now build the data json object

            data = {
                "batch" : {
                    "organisation" : self.user_organisation,
                    "uploadedOn" : self.uploaded_on,
                    "uploadedBy" : self.user_name,
                    "samples" : md5s
                }
            }

            url = self.environment_urls[self.environment]['WORLD_URL'] + self.environment_urls[self.environment]['ORDS_PATH'] + '/createSampleGuids'

            a = requests.post(url=url, data=json.dumps(data), headers=self.headers)
            result = json.loads(a.content)
            self.gpas_result = result
            self.gpas_batch = self.gpas_result['batch']['guid']
            self.df['gpas_batch'] = self.gpas_batch

            guid_lookup = {}
            for i in self.gpas_result['batch']['samples']:
                guid_lookup[i['hash']] = i['guid']

            self.df[['gpas_sample_name', 'gpas_run_number']] = self.df.apply(gpas_uploader.assign_gpas_identifiers_oci, args=(self.run_number_lookup, guid_lookup,), axis=1)

        else:
            # create offline the assumed unique GPAS batch id and sample names
            self.gpas_batch = 'B-' + gpas_uploader.create_batch_name(self.upload_csv)
            self.df['gpas_batch'] = self.gpas_batch
            self.df[['gpas_sample_name', 'gpas_run_number']] = self.df.apply(gpas_uploader.assign_gpas_identifiers_local, args=(self.run_number_lookup,), axis=1)

        # now that the gpas identifiers have been assigned, we need to rename the
        # decontaminated FASTQ files
        if self.sequencing_platform == 'Illumina':
            self.df[['r1_uri', 'r2_uri']] = self.df.apply(rename_paired_fastq, axis=1)
        elif self.sequencing_platform == 'Nanopore':
            self.df['r_uri'] = self.df.apply(rename_unpaired_fastq, axis=1)

        if len(self.decontamination_errors)>0:

            self.decontamination_successful = False

            errors = []
            for idx,row in self.decontamination_errors.iterrows():
                errors.append({"sample": row.sample_name, "error": row.error_message})

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
                    "uploaded_on": self.uploaded_on,
                    "run_numbers": [i for i in self.run_number_lookup.values()],
                    "samples": [i for i in samples],
                }
            }
        }


    def _call_ords_PAR(self):
        """Private method that calls ORDS to get a Pre-Authenticated Request.

        The PAR url is used to upload data to the Organisation's input bucket in OCI

        Returns
        -------
        par: str
        """
        url = self.environment_urls[self.environment]['WORLD_URL'] + self.environment_urls[self.environment]['ORDS_PATH'] + '/pars'

        # make the API call
        a = requests.get(url=url, headers=self.headers)

        # if it fails raise an Exception, otherwise parse the returned content
        if not a.ok:
            a.raise_for_status()
        else:
            result = json.loads(a.content)

        return result['par']


    def submit(self):
        """Submit the samples and their metadata to GPAS for processing.

        The upload CSV must have successfully been validated and the BAM/FASTQ files decontaminated.
        """
        assert self.connect_to_oci, "can only submit samples on the command line if you have provided a valid token!"

        assert self.valid, 'the upload CSV must have been validated!'

        assert self.decontamination_successful, 'samples must first have been successfully decontaminated!'

        self.submit_errors = pandas.DataFrame(None, columns=['sample_name', 'error_message'])

        headers = {}
        headers['Authorization'] = self.headers['Authorization']
        headers['Content-Type'] = 'application/octet-stream'

        par = self._call_ords_PAR()

        bucket = par.split('/')[-3]

        assert len(bucket) == 32, "unable to extract bucket from PAR: "+bucket

        url = par + self.gpas_batch + '/'

        self.df['uploaded'] = False

        counter = 0
        samples_not_uploaded = len(self.df.loc[~self.df['uploaded']])

        while samples_not_uploaded > 0 and counter < 3:

            if self.sequencing_platform == 'Illumina':
                self.df['uploaded'] = self.df.progress_apply(upload_fastq_paired, args=(url, headers,), axis=1)
            else:
                self.df['uploaded'] = self.df.progress_apply(upload_fastq_unpaired, args=(url, headers,), axis=1)
            samples_not_uploaded = len(self.df.loc[~self.df['uploaded']])
            counter+=1

        self.submit_json = copy.deepcopy(self.decontamination_json['submission'])
        self.submit_json['batch']['bucket_name'] = bucket
        self.submit_json['batch']['uploaded_by'] = self.user_name
        self.submit_json['batch']['organisation'] = self.user_organisation

        # build the API URL
        url  = self.environment_urls[self.environment]['WORLD_URL'] + self.environment_urls[self.environment]['ORDS_PATH'] + '/batches'

        # make the API call
        a = requests.post(url=url, json=self.submit_json, headers=self.headers)

        # if it fails raise an Exception, otherwise parse the returned content
        if not a.ok:

            self.submit_errors.append(pandas.DataFrame([[None,'sending metadata JSON to ORDS failed']], columns=['sample_name', 'error_message']))

        else:
            # make the finalisation mark
            url = par + self.gpas_batch + '/upload_done.txt'

            r = requests.put(url, headers=headers)

            if not r.ok:
                self.submit_errors.append(pandas.DataFrame([[None,'uploading finalisation mark failed']], columns=['sample_name', 'error_message']))
