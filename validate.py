from openpyxl import load_workbook

class Samplesheet:
    def __init__(self):
        pass

    def to_json(self):
        pass

def samplesheet(fn):
    wb = load_workbook(filename=fn)
