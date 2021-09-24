from json import dumps
import sys


class GpasError(Exception):
    pass


def emsg(error, json=False, file=sys.stdout):
    if json:
        print(dumps({"error": error}), file=file)


def smsg(samples, json=False, file=sys.stdout):
    if json:
        print(dumps({"submission": samples}), file=file)


def dmsg(sample, error, json=False, file=sys.stdout):
    if json:
        print(
            dumps({"decontamination": {"sample": sample, "status": error}}), file=file
        )


def verr(errors, json=False, file=sys.stdout):
    if json:
        print(dumps({"validation": errors}))


def derr(sample, error, json=False, file=sys.stdout):
    if json:
        print(
            dumps(
                {
                    "decontamination": {
                        "sample": sample,
                        "status": "failure",
                        "error": error,
                    }
                }
            )
        )
