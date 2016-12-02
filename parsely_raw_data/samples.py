import json
import os
import tempfile

import tablib
import xlsxwriter

from .schema import SCHEMA

def gen_json2csv(jsonlines):
    first_columns = ["action", "ts_action", "visitor_site_id", "url", "apikey"]
    headers = [field["key"] for field in SCHEMA
               if field["key"] not in first_columns]
    headers = first_columns + headers
    yield headers
    for jsonline in jsonlines:
        row = []
        for field in headers:
            cell = jsonline.get(field)
            if cell is None:
                cell = ""
            if isinstance(cell, (dict, list)):
                cell = json.dumps(cell)
            row.append(cell)
        yield row

SAMPLE_SIZE_IN_ROWS = 50000

def make_sample_dataset(jsonlines):
    lines = []
    # take first 50k jsonlines
    for i, line in enumerate(jsonlines):
        if i > SAMPLE_SIZE_IN_ROWS:
            break
        jsonline = json.loads(line)
        lines.append(jsonline)
    headers = None
    body = []
    for i, line in enumerate(gen_json2csv(lines)):
        if i == 0:
            headers = line
            continue
        else:
            body.append(line)
    data = tablib.Dataset(*body, headers=headers)
    return data


def make_xlsx(rows):
    tmp_path = tempfile.mktemp()
    workbook = xlsxwriter.Workbook(tmp_path, {'strings_to_numbers': False,
                                              'strings_to_formulas': False,
                                              'strings_to_urls': False})
    worksheet = workbook.add_worksheet()
    worksheet.write_row(0, 0, rows.headers)
    for i, row in enumerate(rows):
        worksheet.write_row(i + 1, 0, row)
    workbook.close()
    contents = open(tmp_path, "rb").read()
    os.remove(tmp_path)
    return contents


if __name__ == "__main__":
    import csv
    print("CSV file self-test...")
    tmp_path = tempfile.mktemp()
    with open(tmp_path, "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        for line in gen_json2csv([{"visitor_site_id": "1",
                                   "apikey": "qz.com",
                                   "ts_action": "2016-11-26 08:30:57",
                                   "action": "pageview",
                                   "extra_data": {"present": True},
                                   "url": "http://qz.com/1234"}]):
            csv_writer.writerow(line)
    for i, line in enumerate(open(tmp_path).readlines()):
        print(line[:80])
    os.remove(tmp_path)
    print("Done.")

    print("Generating CSV and XLSX sample from sample.json.gz...")
    import gzip
    with open("sample.csv", "w") as csv_file, \
         open("sample.xlsx", "wb") as xlsx_file:
        jsonlines = gzip.GzipFile("sample.json.gz")
        data = make_sample_dataset(jsonlines)
        csv_file.write(data.csv)
        xlsx_file.write(make_xlsx(data))
    print("Done.")
