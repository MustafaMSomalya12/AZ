import os
import zipfile
import json
import requests
import urllib
import pandas as pd
from util import eval_unpack

DOWNLOAD_URL = 'https://api.fda.gov/download.json'
COLUMNS_REQ = [
    'effective_time',
    'spl_product_data_elements',
    'drug_interactions',
    'openfda_manufacturer_name',
    'openfda_brand_name',
    'openfda_generic_name',
    'openfda_route'
    ]


class FDA(object):

    def __init__(self, url=DOWNLOAD_URL):
        self.url = url

    def get_drug_label_files(self, nested_keys=[
                                    'results', 'drug', 'label', 'partitions']):
        try:
            response = requests.get(self.url)
            data = response.json()
            for key in nested_keys:
                data = data[key]
            self.file_urls = list(map(lambda x: x['file'], data))
        except Exception as err:
            print(err)

    def download_save_zip_files(
            self,
            files, path_to_save='./fda_data/drug_label/zip_files'):
        try:
            if not os.path.exists(path_to_save):
                os.makedirs(path_to_save)
            self.zip_files = []
            for f in files:
                filename = os.path.join(path_to_save, f.split('/')[-1])
                print('Downloading: {}\nSaving to: {}'.format(f, filename))
                urllib.request.urlretrieve(f, filename)
                self.zip_files.append(filename)
        except Exception as err:
            print(err)

    def unzip_files(
            self, files, path_to_save='./fda_data/drug_label/json_files'):
        try:
            if not os.path.exists(path_to_save):
                os.makedirs(path_to_save)
            self.json_files = []
            for f in files:
                print('Unzipping: {}\nUnzipped to: {}'.format(f, path_to_save))
                with zipfile.ZipFile(f, 'r') as zip_obj:
                    zip_obj.extractall(path_to_save)
                    _ = [self.json_files.append(
                        os.path.join(
                            path_to_save, z)) for z in zip_obj.namelist()]
        except Exception as err:
            print(err)

    def merge_jsons_to_pandas_df(self, files, columns_req=None):
        try:
            json_results = []
            openfda = []
            for f in files:
                with open(f, 'r') as jf:
                    json_dict = json.load(jf)
                json_results += json_dict['results']
                openfda += [r['openfda'] for r in json_dict['results']]
            json_results_df = pd.DataFrame.from_dict(json_results)
            openfda_df = pd.DataFrame.from_dict(openfda)
            openfda_df = openfda_df.add_prefix('openfda_')
            merged_df = pd.concat(
                [json_results_df, openfda_df],
                axis=1, sort=False)
            merged_df.reset_index(drop=True)
            if columns_req is None:
                self.pandas_df = merged_df
            else:
                self.pandas_df = merged_df[columns_req]
        except Exception as err:
            print(err)

    def eval_unpack_df_cols(self, dataframe):
        for col in dataframe:
            dataframe[col] = dataframe[col].apply(eval_unpack)
        self.pandas_df = dataframe

    def save_pandas_df(
                self, pandas_df,
                path_to_save='./fda_data/drug_label/pandas',
                filename='pandas_df.csv'):
        try:
            if not os.path.exists(path_to_save):
                os.makedirs(path_to_save)
            path_file = os.path.join(path_to_save, filename)
            pandas_df.to_csv(path_file, index=False)
            print('Pandas dataframe saved to: {}'.format(path_file))
        except Exception as err:
            print(err)


if __name__ == "__main__":
    fda = FDA()
    fda.get_drug_label_files()
    fda.download_save_zip_files(fda.file_urls)
    fda.unzip_files(fda.zip_files)
    fda.merge_jsons_to_pandas_df(fda.json_files, COLUMNS_REQ)
    fda.eval_unpack_df_cols(fda.pandas_df)
    fda.save_pandas_df(fda.pandas_df)
