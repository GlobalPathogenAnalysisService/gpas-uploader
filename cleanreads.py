import sys
import subprocess
from error import GpasError
from pathlib import Path

riak = Path("./readItAndKeep").resolve()
ref_genome = "MN908947.fasta"

# test riak installation
if not riak.exists():
    raise GpasError({"decontamination": riak})

with subprocess.Popen(
    [riak, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
) as p_test:
    p_test.wait()
    if p_test.returncode != 0:
        raise GpasError({"decontamination": "read removal tool error"})


class Decontamination:
    process = None
    error = []

    def __init__(self, fq1, fq2=None, outdir=None):
        #        if not outdir:
        #            outdir = Tempfile.dir()
        # test output dir
        outdir = "/tmp"

        if not Path(fq1).exists():
            print("missing", fq1, file=sys.stderr)
            raise GpasError()

        if fq2:
            if not Path(fq2).exists():
                print("missing", fq2, file=sys.stderr)
                raise GpasError()
            # paired run

            self.process = subprocess.Popen(
                [riak, "--ref_fasta", ref_genome, "--reads1", fq1, "--reads2", fq2],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        else:
            # single end
            self.process = subprocess.Popen(
                [riak, "--ref_fasta", ref_genome, "--reads1", fq1],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

    def result(self):
        # wait for decontam process to finish
        out, err = self.process.communicate()
        if self.process.returncode != 0:
            #            raise GpasError({"decontamination": self.process.return_code})
            pass
        return out, err
