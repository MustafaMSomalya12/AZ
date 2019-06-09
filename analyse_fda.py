import pandas as pd
import re

class ANALYSE_FDA(object):
    def __init__(self, filename=None):
        if filename is not None:
            try:
                self.dataframe = pd.read_csv(filename)
            except Exception as err:
                print(err)
    def get_company_df(self, company_name, dataframe=None):
        if dataframe is not None:
            return dataframe[dataframe['openfda_manufacturer_name'].
                                 str.contains(company_name, na=False)].copy()
        else:
            try:
                return self.dataframe[self.dataframe['openfda_manufacturer_name'].
                                 str.contains(company_name, na=False)].copy()
            except Exception as err:
                print(err)
    def get_year(self, effective_time):
        effective_time_str = str(effective_time)
        return int(effective_time_str[0:4])
    def get_ingredients(self, spl_product_data_elements):
        try:
            ingredients = spl_product_data_elements.lower().split()
            ingredients = list(map(lambda x: re.sub(r"[^a-zA-Z0-9]+", '', x), ingredients))
            return list(set(ingredients))
        except:
            return spl_product_data_elements
    def get_ingredients_count(self, ingredients):
        try:
            return len(ingredients)
        except:
            ingredients
    def add_year_ingredients(self, dataframe):
        dataframe['year'] = dataframe['effective_time'].map(self.get_year)
        dataframe['ingredients'] = dataframe['spl_product_data_elements'].map(self.get_ingredients)
        dataframe['ingredients_count'] = dataframe['ingredients'].map(self.get_ingredients_count)
        return dataframe
    def get_avg_yearly_results(self, dataframe):
        results = []
        for yr in dataframe['year'].unique():
            yr_df = dataframe[dataframe['year']==yr]
            drugs = yr_df['openfda_brand_name'].values
            drugs = ','.join(drugs).lower()
            avg = yr_df['ingredients_count'].mean()
            results.append({'year': yr,
                           'drug_names': drugs,
                           'avg_number_of_ingredients': avg})
            results_df = pd.DataFrame.from_dict(results)
        return results_df