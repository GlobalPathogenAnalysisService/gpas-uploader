[![Tests](https://github.com/GenomePathogenAnalysisService/gpas-uploader/actions/workflows/test.yaml/badge.svg)](https://github.com/GenomePathogenAnalysisService/gpas-uploader/actions/workflows/test.yaml)

# gpas-uploader

Pathogen genetic sequencing data command line upload client for GPAS users.

## Installation

To download `gpas-uploader`, create a virtual environment, activate it and install issue the following (note that if you are building a binary using `PyInstaller` please see the separate section below as that has different instructions to suppress a warning):

```
$ git clone git@github.com:GenomePathogenAnalysisService/gpas-uploader.git
$ cd gpas-uploader
$ python3 -m venv env
$ source env/bin/activate
(env) $ pip install . pytest
```

Before we run the unit tests you will need `readItAndKeep` installed and either in your `$PATH` or in the `gpas-uploader/` folder. To do the latter issue

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
(env) $ mv samtools ..
(env) $ cd ..
```
Finally we are at a point where we can run the `gpas-uploader` unit tests

```
(env) $ py.test tests/
```

If you now wish to produce a single binary file for distribution, please see the section after Usage.

## Usage

### Validate the GPAS upload CSV

By default the output is text

```
$ gpas-upload validate examples/illumina-fastq-upload.csv
--> All preliminary checks pass and this upload CSV can be passed to the GPAS upload app
```

but a validated JSON object can also be output (this is required for the Electron Client)

```
$ gpas-upload --json validate examples/illumina-fastq-upload.csv
{'validation': {'status': 'completed', 'samples': [{'sample': '07c00741-e10c-4e29-92c4-c84212c96cd9', 'files': ['paired1_1.fastq.gz', 'paired1_2.fastq.gz']}, {'sample': 'bddfc9c2-98eb-4188-ac06-df2cdbc5d397', 'files': ['paired2_1.fastq.gz', 'paired2_2.fastq.gz']}, {'sample': '811c8d25-5b1b-46fa-b539-bc18e9dd816a', 'files': ['paired3_1.fastq.gz', 'paired3_2.fastq.gz']}]}}
```

If the upload CSV specifies BAM files, then they will be automatically converted to FASTQ files using `samtools` in the background e.g.

```
$ gpas-upload validate examples/illumina-bam-upload.csv
--> All preliminary checks pass and this upload CSV can be passed to the GPAS upload app
```

Nanopore upload CSVs behave similarly. Here is one which fails validation for lots of reasons. These have been parsed to create user-friendly error messages that can be displayed in the GPAS Upload app.

```
(env) $ $ gpas-upload --json validate tests/files/nanopore-bam-upload-csv-fail-1.csv
{'validation': {'status': 'failure', 'samples': [{'sample': 'dd73727d-705e-41f6-a353-7640013df547', 'error': 'batch can only contain characters (A-Za-z0-9._-)'}, {'sample': 'dd73727d-705e-41f6-a353-7640013df547', 'error': 'neg in the control field is not valid: field must be either empty or contain the one of the keywords positive or negative'}, {'sample': '40cf9175-52ab-464e-b3f3-8daf0b2b44ef', 'error': 'collection_date cannot be before 2019-01-01'}, {'sample': 'dd73727d-705e-41f6-a353-7640013df547', 'error': 'collection_date cannot be in the future'}, {'sample': None, 'error': 'collection_date must be in form YYYY-MM-DD and cannot include the time'}, {'sample': '3008ab5d-b2cc-44a7-a962-e0cae94a31d5', 'error': 'FR is not a valid ISO-3166-1 country'}, {'sample': '3008ab5d-b2cc-44a7-a962-e0cae94a31d5', 'error': 'Finistere is not a valid ISO-3166-2 region for the specified country'}, {'sample': '3008ab5d-b2cc-44a7-a962-e0cae94a31d5', 'error': 'tags can only contain characters (A-Za-z0-9:_-)'}, {'sample': '40cf9175-52ab-464e-b3f3-8daf0b2b44ef', 'error': 'host can only contain the keyword human'}, {'sample': '40cf9175-52ab-464e-b3f3-8daf0b2b44ef', 'error': 'specimen_organism can only contain the keyword SARS-CoV-2'}, {'sample': 'dd73727d-705e-41f6-a353-7640013df547', 'error': 'primer_scheme can only contain the keyword auto'}, {'sample': '40cf9175-52ab-464e-b3f3-8daf0b2b44ef', 'error': 'instrument_platform can only contain one of the keywords Illumina or Nanopore'}, {'sample': None, 'error': 'instrument_platform must be unique'}]}}
```

### Decontaminate the GPAS upload CSV

Assuming the above is ok and does not return errors you can then ask for the files to be decontaminated (run `ReadItAndKeep` on all the FASTQ files). The same interface is used -- JSON is written to STDOUT.

```
(env) $ gpas-upload --json decontaminate examples/illumina-fastq-upload.csv
{"decontamination": {"sample": "sample1", "status": "started", "file": "paired1_1.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "started", "file": "paired1_2.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "completed", "file": "paired1_1.fastq.gz", "cleaned": "/private/tmp/5d171088-9fd9-4e42-9ca8-d7a7d7186575.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "completed", "file": "paired1_2.fastq.gz", "cleaned": "/private/tmp/5d171088-9fd9-4e42-9ca8-d7a7d7186575.reads_2.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "started", "file": "paired2_1.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "started", "file": "paired2_2.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "completed", "file": "paired2_1.fastq.gz", "cleaned": "/private/tmp/f11943e9-4382-4c79-8ce9-c94a70c42717.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "completed", "file": "paired2_2.fastq.gz", "cleaned": "/private/tmp/f11943e9-4382-4c79-8ce9-c94a70c42717.reads_2.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "started", "file": "paired3_1.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "started", "file": "paired3_2.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "completed", "file": "paired3_1.fastq.gz", "cleaned": "/private/tmp/9cbb0dab-d47c-48ef-8c08-a72352156406.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "completed", "file": "paired3_2.fastq.gz", "cleaned": "/private/tmp/9cbb0dab-d47c-48ef-8c08-a72352156406.reads_2.fastq.gz"}}
{'submission': {'status': 'completed', 'batch': {'file_name': 'B-8R39222', 'uploaded_on': '2022-03-15T16:31:45.778Z+00:00', 'run_numbers': [0, 1], 'samples': [{'sample': '5d171088-9fd9-4e42-9ca8-d7a7d7186575', 'run_number': 0, 'tags': ['site0', 'repeat'], 'control': 'negative', 'collection_date': '2022-02-01', 'country': 'USA', 'region': 'Texas', 'district': '1124', 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/5d171088-9fd9-4e42-9ca8-d7a7d7186575.reads_1.fastq.gz', 'r1_md5': 'dda17843b08e1314e10d013287ac8fc8', 'r2_uri': '/private/tmp/5d171088-9fd9-4e42-9ca8-d7a7d7186575.reads_2.fastq.gz', 'r2_md5': '3e1cc358bfc061249e6f5e7504f4635d'}}, {'sample': 'f11943e9-4382-4c79-8ce9-c94a70c42717', 'run_number': 1, 'tags': ['site0'], 'control': nan, 'collection_date': '2022-03-01', 'country': 'FRA', 'region': 'Finistère', 'district': nan, 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/f11943e9-4382-4c79-8ce9-c94a70c42717.reads_1.fastq.gz', 'r1_md5': 'dda17843b08e1314e10d013287ac8fc8', 'r2_uri': '/private/tmp/f11943e9-4382-4c79-8ce9-c94a70c42717.reads_2.fastq.gz', 'r2_md5': '3e1cc358bfc061249e6f5e7504f4635d'}}, {'sample': '9cbb0dab-d47c-48ef-8c08-a72352156406', 'run_number': 1, 'tags': ['site0'], 'control': 'positive', 'collection_date': '2022-03-08', 'country': 'GBR', 'region': 'Oxfordshire', 'district': nan, 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/9cbb0dab-d47c-48ef-8c08-a72352156406.reads_1.fastq.gz', 'r1_md5': 'dda17843b08e1314e10d013287ac8fc8', 'r2_uri': '/private/tmp/9cbb0dab-d47c-48ef-8c08-a72352156406.reads_2.fastq.gz', 'r2_md5': '3e1cc358bfc061249e6f5e7504f4635d'}}]}}}
```
### Submitting a batch to GPAS

There is now an additional subcommand `submit` that extends the `decontaminate` command (i.e. you do not have to run decontaminate first) and, through ORDS API calls, 
* gets the user's name, organisation and allowed tags in that environment
* posts the `md5sum` of the samples (one per sample i.e. for Illumina the `md5` for `fastq1` is passed) and gets in return the batch number and sample UUID4 (server-side GUIDs)
* gets the PAR
* posts the JSON object containing all the metadata to APEX
* upload the FASTQ files
* puts the finalisation mark (`upload_done.txt`) to trigger `dir_watcher.py`

```
$ gpas-upload --environment dev --token token.json --json submit examples/illumina-fastq-upload.csv
{"decontamination": {"sample": "sample1", "status": "started", "file": "paired-sample-1_1.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "started", "file": "paired-sample-1_2.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "completed", "file": "paired-sample-1_1.fastq.gz", "cleaned": "/private/tmp/sample1.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "completed", "file": "paired-sample-1_2.fastq.gz", "cleaned": "/private/tmp/sample1.reads_2.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "started", "file": "paired-sample-2_1.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "started", "file": "paired-sample-2_2.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "completed", "file": "paired-sample-2_1.fastq.gz", "cleaned": "/private/tmp/sample2.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "completed", "file": "paired-sample-2_2.fastq.gz", "cleaned": "/private/tmp/sample2.reads_2.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "started", "file": "paired-sample-3_1.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "started", "file": "paired-sample-3_2.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "completed", "file": "paired-sample-3_1.fastq.gz", "cleaned": "/private/tmp/sample3.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "completed", "file": "paired-sample-3_2.fastq.gz", "cleaned": "/private/tmp/sample3.reads_2.fastq.gz"}}
{"submission": {"status": "completed", "batch": {"file_name": "E-22c843b9", "uploaded_on": "2022-04-05T15:32:24.562Z+01:00", "run_numbers": [1], "samples": [{"name": "9ca229e8-62e1-2282-d325-1be1b9072371", "run_number": "", "tags": ["ENA_Data", "HPRU"], "control": "negative", "collection_date": "2022-02-01", "country": "USA", "region": "Texas", "district": "1124", "specimen": "SARS-CoV-2", "host": "human", "instrument": {"platform": "Illumina"}, "primer_scheme": "auto", "pe_reads": {"r1_uri": "/private/tmp/9ca229e8-62e1-2282-d325-1be1b9072371.reads_1.fastq.gz", "r1_md5": "4bc0e0396809c258c3df1bf2628b271b", "r2_uri": "/private/tmp/9ca229e8-62e1-2282-d325-1be1b9072371.reads_2.fastq.gz", "r2_md5": "ad948cba3bb22cd54c74ed5474c9772d"}}, {"name": "88b44b48-dd7b-2ea7-bfba-b293b01574e2", "run_number": 1, "tags": ["HPRU"], "control": "", "collection_date": "2022-03-01", "country": "FRA", "region": "Finist\u00e8re", "district": "", "specimen": "SARS-CoV-2", "host": "human", "instrument": {"platform": "Illumina"}, "primer_scheme": "auto", "pe_reads": {"r1_uri": "/private/tmp/88b44b48-dd7b-2ea7-bfba-b293b01574e2.reads_1.fastq.gz", "r1_md5": "6e6814e2847b133b81e6f2a30f18d871", "r2_uri": "/private/tmp/88b44b48-dd7b-2ea7-bfba-b293b01574e2.reads_2.fastq.gz", "r2_md5": "52c201efcb09d0e0fc374d7122ecd29f"}}, {"name": "198af255-f94d-26ce-105f-92e2fca0477a", "run_number": 1, "tags": ["ENA_Data"], "control": "positive", "collection_date": "2022-03-08", "country": "GBR", "region": "Oxfordshire", "district": "", "specimen": "SARS-CoV-2", "host": "human", "instrument": {"platform": "Illumina"}, "primer_scheme": "auto", "pe_reads": {"r1_uri": "/private/tmp/198af255-f94d-26ce-105f-92e2fca0477a.reads_1.fastq.gz", "r1_md5": "98aced37c7459a01cd5b1e44c1bd8023", "r2_uri": "/private/tmp/198af255-f94d-26ce-105f-92e2fca0477a.reads_2.fastq.gz", "r2_md5": "ffe943b47e953b264656286589b464af"}}]}}}
100%|████████████████████████████████████████████████████████████████████████| 3/3 [00:05<00:00,  1.84s/it]
{"status": "completed", "batch": {"file_name": "E-22c843b9", "uploaded_on": "2022-04-05T15:32:24.562Z+01:00", "run_numbers": [1], "samples": [{"name": "9ca229e8-62e1-2282-d325-1be1b9072371", "run_number": "", "tags": ["ENA_Data", "HPRU"], "control": "negative", "collection_date": "2022-02-01", "country": "USA", "region": "Texas", "district": "1124", "specimen": "SARS-CoV-2", "host": "human", "instrument": {"platform": "Illumina"}, "primer_scheme": "auto", "pe_reads": {"r1_uri": "/private/tmp/9ca229e8-62e1-2282-d325-1be1b9072371.reads_1.fastq.gz", "r1_md5": "4bc0e0396809c258c3df1bf2628b271b", "r2_uri": "/private/tmp/9ca229e8-62e1-2282-d325-1be1b9072371.reads_2.fastq.gz", "r2_md5": "ad948cba3bb22cd54c74ed5474c9772d"}}, {"name": "88b44b48-dd7b-2ea7-bfba-b293b01574e2", "run_number": 1, "tags": ["HPRU"], "control": "", "collection_date": "2022-03-01", "country": "FRA", "region": "Finist\u00e8re", "district": "", "specimen": "SARS-CoV-2", "host": "human", "instrument": {"platform": "Illumina"}, "primer_scheme": "auto", "pe_reads": {"r1_uri": "/private/tmp/88b44b48-dd7b-2ea7-bfba-b293b01574e2.reads_1.fastq.gz", "r1_md5": "6e6814e2847b133b81e6f2a30f18d871", "r2_uri": "/private/tmp/88b44b48-dd7b-2ea7-bfba-b293b01574e2.reads_2.fastq.gz", "r2_md5": "52c201efcb09d0e0fc374d7122ecd29f"}}, {"name": "198af255-f94d-26ce-105f-92e2fca0477a", "run_number": 1, "tags": ["ENA_Data"], "control": "positive", "collection_date": "2022-03-08", "country": "GBR", "region": "Oxfordshire", "district": "", "specimen": "SARS-CoV-2", "host": "human", "instrument": {"platform": "Illumina"}, "primer_scheme": "auto", "pe_reads": {"r1_uri": "/private/tmp/198af255-f94d-26ce-105f-92e2fca0477a.reads_1.fastq.gz", "r1_md5": "98aced37c7459a01cd5b1e44c1bd8023", "r2_uri": "/private/tmp/198af255-f94d-26ce-105f-92e2fca0477a.reads_2.fastq.gz", "r2_md5": "ffe943b47e953b264656286589b464af"}}], "bucket_name": "TLFgFoKopAxVfeANCwdEcakPVdMJfGqg", "uploaded_by": "PHILIP.FOWLER@NDM.OX.AC.UK", "organisation": "University of Oxford"}}
--> All samples have been successfully submitted to GPAS for processing
```

### Checking the status of the samples in the batch and downloading the output files

The above process will, by default, have written out the mappings between the local (batch,run,sample) identifiers to the deidentified GPAS equivalents in `samples_names.csv`. To query the status of the samples:

```
$ gpas-upload --environment dev --token token.json --json download examples/sample_names.csv --dry_run
{"state": {"sample": "9ca229e8-62e1-2282-d325-1be1b9072371", "status": "Uploaded"}}
{"state": {"sample": "88b44b48-dd7b-2ea7-bfba-b293b01574e2", "status": "Uploaded"}}
{"state": {"sample": "198af255-f94d-26ce-105f-92e2fca0477a", "status": "Unreleased"}}
```
In the example above, one sample has finished running since it is now in `Unreleased` status, whilst the other two remain in `Uploaded` status. Once samples are in `Unreleased` or `Released` state the output files can be downloaded via

```
$ gpas-upload --environment dev --token token.json --json download examples/sample_names.csv --file_types bam vcf fasta
```

The `json` file cannot be downloaded until the new SARS-CoV-2 NextFlow pipeline is implemented in GPAS.

Note that the downloaded files can be renamed with the local sample name so they match the FASTQ (assuming that was named using the local sample name) via

```
$ gpas-upload --environment dev --token token.json --json download examples/sample_names.csv --file_types bam vcf fasta --rename
```

This also automatically adds the local sample name to the header of the `fasta` file but keeps the GPAS GUID.

Finally, the code will add columns recording the success of the download for each specified file type to the provided mapping CSV. If you provide a name the code will write this out as a CSV file

```
$ gpas-upload --environment dev --token token.json --json download examples/sample_names.csv --file_types bam vcf fasta --output_csv batch_1_status.csv
```
  
## Creating a single file for distribution

This is necessary to package up `gpas-upload` inside the Electron Client. If we follow the regular installation process as above we will get a `pyarrow` warning that I can't suppress. Since `pyarrow` is required by `pandera` but we do not use its functionality, we can suppress its installation which avoids the warning. Hence we do the installation but have to manually specify the packages we need to avoid `pyarrow`.

```
$ git clone git@github.com:GenomePathogenAnalysisService/gpas-uploader.git
$ cd gpas-uploader
$ python3 -m venv env
$ source env/bin/activate
(env) $ pip install pandas pandarallel pycountry pytest requests typing_inspect wrapt pydantic tqdm
(env) $ pip install --no-deps pandera
```

Then follow the same instructions for `samtools` and `read-it-and-keep` before running `PyInstaller` as

```
(env) $ pip install pyinstaller
(env) $ python3 -m PyInstaller distribute.spec
```

The result will be a single binary as below. `ReadItAndKeep` requires the reference genome file as an input; we've been unable to get this working with `--add-data` and `importlib.resources.path` so for the time being you will need to copy this file alongside the binary as below. If you do not then `ReadItAndKeep` will fail and hence the `decontaminate` step will not work.

```
(env) $ ls dist/
gpas-upload
(env) $ cp gpas_uploader/MN908947_no_polyA.fasta dist/
(env) $ cd dist/
(env) $ ls
MN908947_no_polyA.fasta gpas-upload
```
Now we can test it works

```
(env) $ ./gpas-upload --help
usage: gpas-upload [-h] [--parallel] [--json] {validate,decontaminate} ...

GPAS batch upload tool

positional arguments:
  {validate,decontaminate}
    validate            parse and validate an upload CSV
    decontaminate       remove human reads from the FASTQ files specified in the upload CSV file

optional arguments:
  -h, --help            show this help message and exit
  --parallel
  --json                whether to write text or json to STDOUT
  ```
So far so good

```
(env) $ ./gpas-upload --json validate ../examples/illumina-bam-upload.csv
{'validation': {'status': 'completed', 'samples': [{'sample': 'df27b4fc-7115-40cd-a044-b61a50e8f5d4', 'files': ['paired1_1.fastq.gz', 'paired1_2.fastq.gz']}, {'sample': '1bd082d2-4278-46b1-9b62-d9e08972c47d', 'files': ['paired2_1.fastq.gz', 'paired2_2.fastq.gz']}, {'sample': '1c742eb8-421b-47f7-a4eb-46717a341ed9', 'files': ['paired3_1.fastq.gz', 'paired3_2.fastq.gz']}]}}
```


Now a stringent test - decontaminating an upload CSV which specifies BAMs. This will therefore call both `samtools` and `ReadItAndKeep`.
 ```
(env) $ ./gpas-upload --json decontaminate ../examples/illumina-bam-upload.csv
{"decontamination": {"sample": "sample1", "status": "started", "file": "paired1_1.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "started", "file": "paired1_2.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "completed", "file": "paired1_1.fastq.gz", "cleaned": "/private/tmp/399d56fa-dfda-4e62-9bdf-048dd243c422.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample1", "status": "completed", "file": "paired1_2.fastq.gz", "cleaned": "/private/tmp/399d56fa-dfda-4e62-9bdf-048dd243c422.reads_2.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "started", "file": "paired2_1.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "started", "file": "paired2_2.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "completed", "file": "paired2_1.fastq.gz", "cleaned": "/private/tmp/e0055999-d05c-4d47-8819-c0dd3c29f2a7.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample2", "status": "completed", "file": "paired2_2.fastq.gz", "cleaned": "/private/tmp/e0055999-d05c-4d47-8819-c0dd3c29f2a7.reads_2.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "started", "file": "paired3_1.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "started", "file": "paired3_2.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "completed", "file": "paired3_1.fastq.gz", "cleaned": "/private/tmp/dfecc638-c09e-4c6c-971e-1119349bea96.reads_1.fastq.gz"}}
{"decontamination": {"sample": "sample3", "status": "completed", "file": "paired3_2.fastq.gz", "cleaned": "/private/tmp/dfecc638-c09e-4c6c-971e-1119349bea96.reads_2.fastq.gz"}}
{'submission': {'status': 'completed', 'batch': {'file_name': 'B-9X3WMMF', 'uploaded_on': '2022-03-16T08:10:31.937Z+00:00', 'run_numbers': [0], 'samples': [{'sample': '399d56fa-dfda-4e62-9bdf-048dd243c422', 'run_number': 0, 'tags': ['site0', 'repeat'], 'control': 'negative', 'collection_date': '2022-02-01', 'country': 'USA', 'region': 'Texas', 'district': '1124', 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/399d56fa-dfda-4e62-9bdf-048dd243c422.reads_1.fastq.gz', 'r1_md5': '70d2e71ef2a5bfdfb5e1e8615a36fedd', 'r2_uri': '/private/tmp/399d56fa-dfda-4e62-9bdf-048dd243c422.reads_2.fastq.gz', 'r2_md5': '51eec565569360fbe875bfd7b5ee8e68'}}, {'sample': 'e0055999-d05c-4d47-8819-c0dd3c29f2a7', 'run_number': 0, 'tags': ['site0'], 'control': nan, 'collection_date': '2022-03-01', 'country': 'FRA', 'region': 'Finistère', 'district': nan, 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/e0055999-d05c-4d47-8819-c0dd3c29f2a7.reads_1.fastq.gz', 'r1_md5': '70d2e71ef2a5bfdfb5e1e8615a36fedd', 'r2_uri': '/private/tmp/e0055999-d05c-4d47-8819-c0dd3c29f2a7.reads_2.fastq.gz', 'r2_md5': '51eec565569360fbe875bfd7b5ee8e68'}}, {'sample': 'dfecc638-c09e-4c6c-971e-1119349bea96', 'run_number': 0, 'tags': ['site0'], 'control': nan, 'collection_date': '2022-03-09', 'country': 'GBR', 'region': 'Devon', 'district': nan, 'specimen': 'SARS-CoV-2', 'host': 'human', 'instrument': {'platform': 'Illumina'}, 'primer_scheme': 'auto', 'pe_reads': {'r1_uri': '/private/tmp/dfecc638-c09e-4c6c-971e-1119349bea96.reads_1.fastq.gz', 'r1_md5': '70d2e71ef2a5bfdfb5e1e8615a36fedd', 'r2_uri': '/private/tmp/dfecc638-c09e-4c6c-971e-1119349bea96.reads_2.fastq.gz', 'r2_md5': '51eec565569360fbe875bfd7b5ee8e68'}}]}}}
```
And finally a failing case

```
(env) $ ./gpas-upload --json decontaminate ../tests/files/nanopore-fastq-upload-csv-fail-1.csv
{'validation': {'status': 'failure', 'samples': [{'sample': 'f15fe4c5-33ef-4ee6-ba3a-4ff1d234b979', 'error': 'sample4.fastq.gz is too small (< 100 bytes)'}]}}
```

### Automated build via GitHub Actions

In addition, the above `PyInstaller` process can be run by a GitHub Action on both ubuntu-latest and macos-latest OS producing two artefacts that can be downloaded. Since the GitHub Action takes several minutes, it is not run automatically on `push` but instead must be manually triggered at present. To do this go [here](https://github.com/GenomePathogenAnalysisService/gpas-uploader/actions/workflows/distribute.yaml) and click the `Run workflow` button. Once complete, the summary page for the Action will have an Artifacts box from which you can download `gpas-upload-macos-latest` and `gpas-upload-ubuntu-latest` zip files. Unzipped these contain

```
$ ls
MN908947_no_polyA.fasta      samtools
gpas-upload                  readItAndKeep
```
You will need to make all bar the fasta file executable, which on a Mac will require you to use System Preferences to allow these applications to be opened, since they have not been code signed.

## Technical notes

Internally, this library uses the new `gpas_uploader.UploadBatch` class which stores the upload CSV as a `pandas.DataFrame`. Additional columns, e.g. the GPAS batch, run and sample identifiers, are added to this dataframe and much of the functionality is achieved using the `pandas.DataFrame.apply` pattern whereby a bespoke function is applied to each row of the dataframe in turn. This also then enables the use of `pandarallel` which allows `samtools` and `ReadItAndKeep` to be run in parallel on your local computer. Since this amounts to multiple `subprocess.Popen` commands being issued, scaling performance should be good. At present `pandarallel` autodetects the number of CPUs and ignores hyperthreading -- in principle additional speedup is possible by making using of threading. Note that this functionality is automatically disabled for Windows, although as noted in the comments, `pandarallel` can be run using the Windows Linux Subsytem.

The simple `gpas-upload` script has been renamed to plan for the GPAS CLI at which point we anticipate moving to `gpas upload`. 
