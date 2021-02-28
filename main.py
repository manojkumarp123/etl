"""
ESMA FRDIS DLTINS 2021-01-17

1. request xml
2.
   a. find first
   b. request ZIP
   c. Un zip
3. xml to csv
4. save to blob
"""

import requests
import codecs
import xml.etree.ElementTree as ET
import csv
import os
from zipfile import ZipFile
import sys
import boto3
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
bucket = os.getenv("BUCKET", "audit-stock-market")
upload_filename = "steel_eye_etl.csv"


def query_esma_firds():
    url = """https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=1"""
    response = requests.get(url)
    response.raise_for_status()
    print (response.status_code, url)
    print ("QUERIED")
    
    # Write to File
    filename = "tmp/esma.firds.response.xml"
    with open(filename, "w") as wf:
        data = codecs.decode(response.content, "utf-8")
        wf.write(data)
        print ("WRITTEN")
        return "tmp/esma.firds.response.xml"

def find():
    filename = query_esma_firds()
    tree = ET.parse(filename)
    print ("PARSED")
    root = tree.getroot() 
    for doc_item in root.findall('./result/doc'):
        download_link, is_dltins = "", False
        for str_item in doc_item.findall("./str"):
            if str_item.attrib["name"] == "download_link":
                download_link = str_item.text
                print ("SET")
            if str_item.attrib["name"] == "file_type" and str_item.text == "DLTINS":
                is_dltins = True
                print ("FOUND")
        if is_dltins:
            print (download_link)
            return download_link

def download_zip():
    download_url = find()
    save_path = "tmp/DLTINS_one.zip"
    
    r = requests.get(download_url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)
        print ("SAVED")
        return save_path

def unzip():
    filename = download_zip()
    with ZipFile(filename, "r") as zipfile:
        unzip_dir = "tmp/dltins_one/"
        zipfile.extractall(unzip_dir)
        print ("UNZIPPED")
        return unzip_dir

def xml_to_csv(limit=None):
    """
    Requirement:
        FinInstrmGnlAttrbts.Id
        FinInstrmGnlAttrbts.FullNm
        FinInstrmGnlAttrbts.ClssfctnTp
        FinInstrmGnlAttrbts.CmmdtyDerivInd
        FinInstrmGnlAttrbts.NtnlCcy
        Issr

    Output:
    ╰─$ head output.csv
    FinInstrmGnlAttrbts.Id,FinInstrmGnlAttrbts.FullNm,FinInstrmGnlAttrbts.ClssfctnTp,FinInstrmGnlAttrbts.CmmdtyDerivInd,FinInstrmGnlAttrbts.NtnlCcy,Issr
    DE000A1R07V3,Kreditanst.f.Wiederaufbau     Anl.v.2014 (2021),DBFTFB,false,EUR,549300GDPG70E3MBBU98
    DE000A1R07V3,KFW 1 5/8 01/15/21,DBFTFB,false,EUR,549300GDPG70E3MBBU98
    DE000A1R07V3,Kreditanst.f.Wiederaufbau Anl.v.2014 (2021),DBFTFB,false,EUR,549300GDPG70E3MBBU98
    DE000A1R07V3,Kreditanst.f.Wiederaufbau Anl.v.2014 (2021),DBFTFB,false,EUR,549300GDPG70E3MBBU98
    DE000A1X3J56,IKB Deutsche Industriebank AG Stufenz.MTN-IHS v.2014(2021),DTVUFB,false,EUR,PWEFG14QWWESISQ84C69
    DE000A1X3J56,IKB Deutsche Industriebank AG Stufenz.MTN-IHS v.2014(2021),DTVUFB,false,EUR,PWEFG14QWWESISQ84C69
    DE000A1X3J56,LSFEU  3.700  1/20/21 (URegS),DTVUFB,false,EUR,PWEFG14QWWESISQ84C69
    DE000A1YC5L8,NIESA Float 01/15/21 BOND,DNVTFB,false,EUR,391200ITQQZ7JMHXK080
    DE000A1YC5L8,NIESA Float 01/15/21 BOND,DNVTFB,false,EUR,391200ITQQZ7JMHXK080
    """
    path = unzip()
    filename = os.listdir(path)[0]

    xml_filename = os.path.join(path, filename)
    print ("PARSING... this may take a while")
    tree = ET.parse(xml_filename)
    root = tree.getroot()
    print ("PARSED")

    hdr, pyld = root
    document = pyld[0]
    FinInstrmRptgRefDataDltaRpt = document[0]
    hdr, *FinInstrm = FinInstrmRptgRefDataDltaRpt

    fieldnames = [
        'FinInstrmGnlAttrbts.Id',
        'FinInstrmGnlAttrbts.FullNm',
        'FinInstrmGnlAttrbts.ClssfctnTp',
        'FinInstrmGnlAttrbts.CmmdtyDerivInd',
        'FinInstrmGnlAttrbts.NtnlCcy',
        'Issr'
    ]
    output_filename = 'output.csv'
    with open(output_filename, mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        count = 0
        FinInstrm = FinInstrm[:limit] if limit is not None else FinInstrm
        for fin_instrm in FinInstrm:
            TermntdRcrd = fin_instrm[0]
            FinInstrmGnlAttrbts, Issr = TermntdRcrd[0:2]

            _id = None
            full_name = None
            clssfctn_tp = None
            cmmdty_deriv_ind = None
            ntnl_ccy = None
            issr = Issr.text

            for column in FinInstrmGnlAttrbts:
                if column.tag.endswith("Id"):
                    _id = column.text
                elif column.tag.endswith("FullNm"):
                    full_name = column.text
                elif column.tag.endswith("ClssfctnTp"):
                    clssfctn_tp = column.text
                elif column.tag.endswith("CmmdtyDerivInd"):
                    cmmdty_deriv_ind = column.text
                elif column.tag.endswith("NtnlCcy"):
                    ntnl_ccy = column.text

            writer.writerow({
                'FinInstrmGnlAttrbts.Id': _id,
                'FinInstrmGnlAttrbts.FullNm': full_name,
                'FinInstrmGnlAttrbts.ClssfctnTp': clssfctn_tp,
                'FinInstrmGnlAttrbts.CmmdtyDerivInd': cmmdty_deriv_ind,
                'FinInstrmGnlAttrbts.NtnlCcy': ntnl_ccy,
                'Issr': issr
            })
            count += 1
        print (f"{count} CSV ROWS WRITTEN")
        return output_filename

def get_limit():
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
        print (f"LIMIT {limit}")
        return limit

def upload():
    limit=get_limit()
    filename = xml_to_csv(limit)
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        print ("UPLOADING")
        s3.upload_file(filename, bucket, upload_filename)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

if __name__ == "__main__":
    print ("STARTED")
    upload()
    print ("COMPLETED")