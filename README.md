# gpas-uploader

Pathogen genetic sequencing data command line upload client for GPAS users.

## Installation

To download, create a virtual environment, activate it and install and run the unit tests

```
$ git clone git@github.com:GenomePathogenAnalysisService/gpas-uploader.git
$ cd gpas-uploader
$ python3 -m venv env
$ source env/bin/activate
(env) $ pip install .
(env) $ py.test
```

You will need `readItAndKeep` installed and either in your `$PATH` or in the `gpas-uploader/` folder. To do the latter issue 

```
(env) $ git clone https://github.com/GenomePathogenAnalysisService/read-it-and-keep.git
(env) $ cd read-it-and-keep/src
(env) $ make
(env) $ mv readItAndKeep ../..
(env) $ cd ../..
```

To process BAM files, you'll also need `samtools`, again either in your `$PATH` or in the `gpas-uploader/` folder. To do the latter issue 

```
(env) $ wget https://github.com/samtools/samtools/releases/download/1.14/samtools-1.14.tar.bz2
(env) $ bunzip2 samtools-1.14.tar.bz2
(env) $ tar xvf samtools-1.14.tar
(env) $ cd samtools-1.14
(env) $ ./configure
(env) $ make
(env) $ sudo make install
(env) $ cd ..
```

## Validate the GPAS upload CSV

By default the output is text

```
$ gpas-upload validate examples/illumina-fastq-upload.csv
--> All preliminary checks pass and this upload CSV can be passed to the GPAS upload app
```

but a validated JSON object can also be output

```
$ gpas-upload --json validate examples/illumina-fastq-upload.csv
{'validation': {'status': 'completed', 'samples': [{'sample': '07c00741-e10c-4e29-92c4-c84212c96cd9', 'files': ['sample1_1.fastq.gz', 'sample1_2.fastq.gz']}, {'sample': 'bddfc9c2-98eb-4188-ac06-df2cdbc5d397', 'files': ['sample2_1.fastq.gz', 'sample2_2.fastq.gz']}, {'sample': '811c8d25-5b1b-46fa-b539-bc18e9dd816a', 'files': ['sample3_1.fastq.gz', 'sample3_2.fastq.gz']}]}}
```

If the upload CSV specifies BAM files, then they will be automatically converted to FASTQ files using `samtools` in the background e.g.

```
$ gpas-upload validate examples/illumina-bam-upload.csv
```

Nanopore upload CSVs behave similarly. Here is one which fails validation for lots of reasons. These have been parsed to create user-friendly error messages that can be displayed in the GPAS Upload app.

```
$ gpas-upload validate tests/files/nanopore-bam-upload-csv-fail-1.csv
{'validation': {'status': 'failure', 'samples': [{'sample': '249eb54b-9e05-4c08-9816-3c4e3851d263', 'error': 'batch can only contain characters (A-Za-z0-9._-)'}, {'sample': '249eb54b-9e05-4c08-9816-3c4e3851d263', 'error': 'neg in the control field is not valid: field must be either empty or contain the one of the keywords positive or negative'}, {'sample': 'bf0e9317-f9bd-4fe2-8740-0b6cc9749cbb', 'error': 'collection_date cannot be before 2019-01-01'}, {'sample': '249eb54b-9e05-4c08-9816-3c4e3851d263', 'error': 'collection_date cannot be in the future'}, {'sample': None, 'error': 'collection_date must be in form YYYY-MM-DD and cannot include the time'}, {'sample': '744a615b-8233-4814-aa83-47ef1323948d', 'error': 'FR is not a valid ISO-3166-1 country'}, {'sample': '744a615b-8233-4814-aa83-47ef1323948d', 'error': 'Finistere is not a valid ISO-3166-2 region for the specified country'}, {'sample': '744a615b-8233-4814-aa83-47ef1323948d', 'error': 'tags can only contain characters (A-Za-z0-9:_-)'}, {'sample': 'bf0e9317-f9bd-4fe2-8740-0b6cc9749cbb', 'error': 'host can only contain the keyword human'}, {'sample': 'bf0e9317-f9bd-4fe2-8740-0b6cc9749cbb', 'error': 'specimen_organism can only contain the keyword SARS-CoV-2'}, {'sample': '249eb54b-9e05-4c08-9816-3c4e3851d263', 'error': 'primer_scheme can only contain the keyword auto'}, {'sample': 'bf0e9317-f9bd-4fe2-8740-0b6cc9749cbb', 'error': 'instrument_platform can only contain one of the keywords Illumina or Nanopore'}, {'sample': None, 'error': 'instrument_platform must be unique'}]}}
```

## Decontaminate the GPAS upload CSV

Assuming the above is ok and does not return errors you can then ask for the files to be decontaminated (run `ReadItAndKeep` on all the FASTQ files). The same interface is used -- JSON is written to STDOUT.

```
$ gpas-upload decontaminate examples/illumina-fastq-upload.csv
{"decontamination": {"sample": "sample1", "status": "started", "file": "sample1_1.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "started", "file": "sample1_2.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "completed", "file": "sample1_1.fastq.gz", "cleaned": "/private/tmp/sample1.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "completed", "file": "sample1_2.fastq.gz", "cleaned": "/private/tmp/sample1.reads_2.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "started", "file": "sample2_1.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "started", "file": "sample2_2.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "completed", "file": "sample2_1.fastq.gz", "cleaned": "/private/tmp/sample2.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "completed", "file": "sample2_2.fastq.gz", "cleaned": "/private/tmp/sample2.reads_2.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "started", "file": "sample3_1.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "started", "file": "sample3_2.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "completed", "file": "sample3_1.fastq.gz", "cleaned": "/private/tmp/sample3.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "completed", "file": "sample3_2.fastq.gz", "cleaned": "/private/tmp/sample3.reads_2.fastq.gz"}}
{'submission': {'batch': {'file_name': 'B-8R39222', 'uploaded_on': '2022-03-09T16:27:17.292Z+00:00', 'run_numbers': [0, 1], 'samples': [{'sample': '519fb1ba-b820-445a-a174-83330003022e', 'run_number': 0, 'tags': ['site0', 'repeat'], 'control': 'negative', 'collection_date': '2022-02-01', 'country': 'USA', 'region': 'Texas', 'district': '1124', 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/sample1.reads_1.fastq.gz', 'r1_md5': 'dfe7965a73125aabda857983bac275e3', 'r2_uri': '/private/tmp/sample1.reads_2.fastq.gz', 'r2_md5': '97526116469c99ddda3aff96f2e2cd40'}}, {'sample': '22fa2ad7-9320-4040-b186-2ec6bbb704ea', 'run_number': 1, 'tags': ['site0'], 'control': nan, 'collection_date': '2022-03-01', 'country': 'FRA', 'region': 'Finistère', 'district': nan, 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/sample2.reads_1.fastq.gz', 'r1_md5': 'dfe7965a73125aabda857983bac275e3', 'r2_uri': '/private/tmp/sample2.reads_2.fastq.gz', 'r2_md5': '97526116469c99ddda3aff96f2e2cd40'}}, {'sample': '921148c9-e9d0-4039-ad60-9afefd8ec72e', 'run_number': 1, 'tags': ['site0'], 'control': 'positive', 'collection_date': '2022-03-08', 'country': 'GBR', 'region': 'Oxfordshire', 'district': nan, 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/sample3.reads_1.fastq.gz', 'r1_md5': 'dfe7965a73125aabda857983bac275e3', 'r2_uri': '/private/tmp/sample3.reads_2.fastq.gz', 'r2_md5': '97526116469c99ddda3aff96f2e2cd40'}}]}}}
```

## Technical notes

Internally, this library uses the new `gpas_uploader.Batch` class which stores the upload CSV as a `pandas.DataFrame`. Additional columns, e.g. the GPAS batch, run and sample identifiers, are added to this dataframe and much of the functionality is achieved using the `pandas.DataFrame.apply` pattern whereby a bespoke function is applied to each row of the dataframe in turn. This also then enables the use of `pandarallel` which allows `samtools` and `ReadItAndKeep` to be run in parallel on your local computer. Since this amounts to multiple `subprocess.Popen` commands being issued, scaling performance should be good. At present `pandarallel` autodetects the number of CPUs and ignores hyperthreading -- in principle additional speedup is possible by making using of threading. Note that this functionality is automatically disabled for Windows, although as noted in the comments, `pandarallel` can be run using the Windows Linux Subsytem.

The simple `gpas-upload` script has been renamed to plan for the GPAS CLI at which point we anticipate moving to `gpas upload`. The provided `walkthrough.ipynb` shows how the `Batch` class and its methods are used within `gpas-upload`.
