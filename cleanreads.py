import sys
import subprocess
import shutil
from error import GpasError
from pathlib import Path
from os.path import isabs

ref_genome = "MN908947_no_polyA.fasta"

# if there is a local riak
# (as will be the case inside the Electron client)
# use that one
if Path("./readItAndKeep").exists():
    riak = Path("./readItAndKeep").resolve()

# or if there is one in the $PATH use that one
elif shutil.which('readItAndKeep') is not None:
    riak = Path(shutil.which('readItAndKeep'))

else:
    raise GpasError({"decontamination": "read removal tool not found"})

with subprocess.Popen(
    [riak, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
) as p_test:
    p_test.wait()
    if p_test.returncode != 0:
        raise GpasError({"decontamination": "read removal tool error"})

class Decontamination:

    process = None
    error = None
    fq1 = None
    fq2 = None

    def __init__(self, fq1, fq2=None, sample="clean_reads", outdir=Path("/tmp")):
        #        if not outdir:
        #            outdir = Tempfile.dir()
        # test output dir

        #        if not isabs(fq1):

        if not Path(fq1).exists():
            print("missing", fq1, file=sys.stderr)
            raise GpasError()

        if fq2:
            if not Path(fq2).exists():
                print("missing", fq2, file=sys.stderr)
                raise GpasError()
            self.fq1 = Path(outdir) / f"{sample}.reads_1.fastq.gz"
            self.fq2 = Path(outdir) / f"{sample}.reads_2.fastq.gz"
            # paired run
            self.process = subprocess.Popen(
                [
                    riak,
                    "--enumerate_names",
                    "--ref_fasta",
                    ref_genome,
                    "--reads1",
                    fq1,
                    "--reads2",
                    fq2,
                    "--outprefix",
                    outdir / sample,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        else:
            # single end
            self.fq1 = Path(outdir) / f"{sample}.reads.fastq.gz"
            self.process = subprocess.Popen(
                [
                    riak,
                    "--enumerate_names",
                    "--ref_fasta",
                    ref_genome,
                    "--reads1",
                    fq1,
                    "--outprefix",
                    outdir / sample,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

    def result(self):
        # wait for decontam process to finish
        out, err = self.process.communicate()
        self.error = err
        # print(out, file=sys.stderr)
        if self.process.returncode == 0:
            if self.fq2:
                return self.fq1, self.fq2
            else:
                return self.fq1
        else:
            return None
