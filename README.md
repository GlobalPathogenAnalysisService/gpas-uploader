# gpas-uploader

Sequencing data upload client for GPAS users

The global `--json` flag will translate output to JSON.

## validate metadata

`gpas-uploader --json import submission.xls`

Will return a validated JSON object to stderr:

```
{'submission': {'batch': 'submission.xls',
    'samples': [{'sample': {'name': 'sample1.fq', 'instrument': 'Illumina', ...}
                           {'name': 'sample2.fq', 'instrument': 'Illumina', ...}
                           {'name': 'sample3.fq', 'instrument': 'Illumina', ...}
}]}}
}
```

## find fastq files, prepare submission

This will validate the samplesheet and prepare a submission job by locating files in the CWD.

`gpas-uploader --json import submission.xls --dir . > submission.json`

This will output a submission JSON object:

```
{
}
```

Additionally, program status will be written to stderr.

```
{'error': 'missing file:'}
```

## begin submission

`gpas-uploader --json upload submission.json`

### register upload

`gpas-uploader --json import submission.xls --dir .`

`gpas-uploader` will contact the server to test for duplicate submissions with the original file's hash and receive preauthenticated upload requests.

Valid samples are registered in the GPAS database.  file hashes and file names are added to the Read tables. The status is set to `PENDING`.

The client recieves an upload 'ticket'; an augmented submission JSON object with pre-auth upload links:

```
{'submission': {'batch': 'submission.xls',
    'samples': [{'sample': 'name': 'sample1',
                            'original_shasum': 'acdfcdcaf',
                            'PAR': 'asdf'}]}}
```

### decontaminate reads

Input files undergo decontamination on the client side to keep PII off the wire.

Decontamination is performed incrementally per file. As samples complete they're logged on stderr (optionally as JSON) to allow a wrapping program to report progress.

```
"original_fastq1": "foo_1.fq.gz",
"original_fastq2": "foo_2.fq.gz",
"decontam_fastq1": "decontam_1.fq.gz",
"decontam_fastq2": "decontam_2.fq.gz",
"... one for each files above sha256": "abcdefgh123455",
"read_counts": {
    "total_original": 100000,
    "after_decontam":  99990,
    "...?", ...
}
"success": true,
"error_messages": none,
"deconaminator_version": "1.1.2",
"genomes_used": {"keep": ["foo"], "remove", ["bar"]},
```

Successfully decontaminated samples are queued for upload through HTTP POST.

Hashes of decontaminated files are preserved to allow resumption.

## resuming an upload

`gpas-uploaded --json upload submission.json --resume`

with the `--resume` flag duplicate filename and hashes for a dataset name are not treated as an error. Matching hashes are skipped and mismatched or missing hashes are treated as missing.
