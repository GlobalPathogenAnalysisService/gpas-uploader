#! /usr/bin/env python3

import pathlib
import requests
import json
import gzip

import pandas
from tqdm.auto import tqdm

import gpas_uploader


class DownloadBatch:
    """
    Query the status and download specified files for the samples given in a mapping CSV file.

    Parameters
    ----------
    mapping_csv : filename
        path to the mapping CSV produced by the upload app or command line tool
    token_file : filename
        path to the token.tok file downloaded from the GPAS portal
    environment : str
        which GPAS enviroment to query. Must be one of dev, stagin or prod.
    output_json : bool
        if True, write progress JSON messages to STDOUT
    """

    def __init__(self, mapping_csv=None, token_file=None, environment='prod', output_json=False):

        self.mapping_csv = pathlib.Path(mapping_csv)
        self.output_json = output_json

        assert self.mapping_csv.is_file, 'provided CSV does not exist!'

        INPUT = open(self.mapping_csv, 'rb')
        data = INPUT.read()
        assert gpas_uploader.check_utf8(data), 'mapping CSV must be UTF-8, please check your CSV'

        self.df = pandas.read_csv(self.mapping_csv)

        if len(self.df.columns) == 6:
            self.mapping_csv_type = 'wide'
            assert all(self.df.columns.isin(['local_batch', 'local_run_number', 'local_sample_name', 'gpas_batch', 'gpas_run_number', 'gpas_sample_name'])), 'mapping CSV does not have a header of local_batch,local_run_number,local_sample_name,gpas_batch,gpas_run_number,gpas_sample_name'
        elif len(self.df.columns) == 1:
            self.mapping_csv_type = 'narrow'
            assert self.df.columns == ['gpas_sample_name'], 'single column mapping CSV should have a header of gpas_sample_name'
        else:
           raise Error('specified mapping CSV should only have 1 or 6 columns')

        self.access_token, self.headers, self.environment_urls = gpas_uploader.parse_access_token(token_file)


    def get_status(self):
        """Retrieve the status of the samples in the mapping CSV.

        Adds a column called status to the internal pandas DataFrame with the status. If output_json is
        set to True, also write out a message to STDOUT. Known statuses are
          * Uploaded, Unreleased, Released, Error (all shown in the GPAS Portal)
          * Authorization required (most likely indicating an invalid token was supplied)
          * Sample not found (gpas_sample_name most likely incorrect)
          * You do not have access to this sample
          * Unhandled error logged for support
        """
        url = 'https://portal.dev.gpas.ox.ac.uk/ords/gpasdevpdb1/gpas_pub/gpasapi'

        url += '/get_sample_detail/'

        self.df['status'] = self.df.apply(self._get_sample_status, args=(url,), axis=1)


    def _get_sample_status(self, row, url):

        url += row.gpas_sample_name
        response = requests.get(url=url, headers=self.headers)
        if response.ok:
            result = json.loads(response.content)
            status = result[0]['status']
        elif response.status_code == 401:
            status = "Authorization required"
        elif 'message' in json.loads(response.text).keys():
            status = json.loads(response.text)['message']
            status = status.replace('.','')
        else:
            status = 'Unknown'

        if self.output_json:
            gpas_uploader.dsmsg(row.gpas_sample_name, status, json=True)

        return status


    def download(self, filetype=None, outdir=None, rename=False):
        """Download the specified files (FASTA etc) using the mapping CSV

        Parameters
        ----------
        filetype : str
            the filetype to download, from ['fasta', 'json', 'bam', 'vcf']
        outdir : str
            the path to write the downloaded files
        rename : bool
            if True, rename the downloaded files to the local_sample_name. For FASTA files this includes modifying the header to include both the local_sample_name and the gpas_sample_name
        """

        assert filetype in ['fasta', 'json', 'bam', 'vcf'], 'must specify one of fasta/json/bam/vcf'

        if self.mapping_csv_type == 'narrow':
            assert not rename, "cannot rename the files to the local_sample_name if you don't provide the full mapping CSV with six fields that is output by the GPAS upload app or command line tool"

        url = 'https://portal.dev.gpas.ox.ac.uk/ords/gpasdevpdb1/gpas_pub/gpasapi'

        url += '/get_output/'

        output_dir = pathlib.Path(outdir)

        if not self.output_json:
            tqdm.pandas(desc='downloading '+filetype)

            self.df[filetype+'_downloaded'] = self.df.progress_apply(self._download_file, args=(url, filetype, output_dir, rename), axis=1)
        else:
            self.df[filetype+'_downloaded'] = self.df.apply(self._download_file, args=(url, filetype, output_dir, rename), axis=1)


    def _download_file(self, row, url, filetype, outdir, rename):
        """Private method to download a file from GPAS.

        Designed to be used with pandas.DataFrame.apply

        Parameters
        ----------
        row: pandas.Series
            supplied by pandas.DataFrame.apply
        url: str
            the base url for the GET based on the environment
        filetype: str
            one of fasta, bam, vcf, json
        outdir: pathlib.Path
            where to write the downloaded file
        rename: bool
            if True, rename the downloaded file to the local_sample_name

        Returns
        -------
        bool
            True if downloaded successfully, otherwise False
        """

        url = url + row.gpas_sample_name + '/' + filetype

        if filetype + '_downloaded' in row.keys() and row[filetype + '_downloaded']:
            return True
        elif row.status in ['Unreleased', 'Released', 'Error']:

            response = requests.get(url=url, headers=self.headers)

            if not response.ok:
                if self.output_json:
                    gpas_uploader.ddmsg(row.gpas_sample_name, filetype, json=True, msg={'status':'failure'})
                return False
            else:
                if outdir is not None:
                    filename = outdir
                else:
                    filename = pathlib.Path('.')

                if filetype == 'fasta':
                    if rename:
                        filename = str(filename / row.local_sample_name) + '.fasta.gz'
                    else:
                        filename = str(filename / row.gpas_sample_name) + '.fasta.gz'
                else:
                    if rename:
                        filename = str(filename / row.local_sample_name) + '.' + filetype
                    else:
                        filename = str(filename / row.gpas_sample_name) + '.' + filetype

                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)

                if filetype == 'fasta' and rename:
                    with gzip.open(filename, 'rb') as f:
                        file_contents = f.readline()
                        file_contents = b'>' + bytes(filename.split('.fasta.gz')[0], encoding='utf8') + b'|' + bytes(row.gpas_sample_name, encoding='utf8') + b'\n'
                        for line in f:
                            file_contents += line

                    with gzip.open(filename, 'wb') as f:
                        f.write(file_contents)
                        f.close()

                if self.output_json:
                    gpas_uploader.ddmsg(row.gpas_sample_name, filetype, json=True, msg={'status':'success'})

                return True
        else:
            return False
