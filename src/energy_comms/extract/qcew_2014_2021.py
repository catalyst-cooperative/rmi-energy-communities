# imports

import pandas as pd
import numpy as np

import qcew_python_3x_example as qcew
from qcew_python_3x_example import qcewGetIndustryData as industry

import qcew_area_titles as area

# years for this extraction step

INDUSTRY_YEARS = ['2014','2015','2016','2017','2018','2019','2020','2021']

# qualifying fossil employment naics codes

INDUSTRY_NAICS_CODES = ['2121','211','213','23712','486','4247','22112']


# first chunk of data - from industry extraction provided by BLS sample code

# make extraction function to set up for loop

def industry_extraction(year,naics_code):
    output = industry(year,'a',naics_code)

    return output


# extraction of all NAICS codes data for all geographies

def get_2014_data():
    appended_df = []

    for year in INDUSTRY_YEARS:
        for naics_code in INDUSTRY_NAICS_CODES:
            i = industry_extraction(year,naics_code)
            df = pd.DataFrame(i[1:],columns=i[0])
            appended_df.append(df)

    appended_df = pd.concat(appended_df,axis=0)

    return appended_df

# function to clean and make wide to long


def cleaning_2014_data():
    
    df = get_2014_data()

    area = area.extract()

    # remove quotation marks in rows
    for i, col in enumerate(df.columns):
        df.iloc[:,i] = df.iloc[:,i].str.replace('"','')

    # remove quotation marks in columns
    df.columns = df.columns.str.replace('"','')

    #merge in BLS area codes to get geographic names for each observations

    df_with_areas = df.merge(area,on='area_fips',how='left')

    #melt to make variable identifier to merge other identifiers in later

    final_df = df_with_areas.melt(id_vars=['area_fips','area_title','industry_code'],value_vars=['annual_avg_emplvl'],value_name='num_of_employees',var_name='variable')


    return final_df


# using BLS area codes, make a column indicating what geographic level the observation is in (county, statewide, MSA, etc) and state column
def get_2014_with_bls_geographic_tag():
    # potentially make df a parameter
    data = get_2014_data()

    data['geographic_level'] = np.where(data['area_title'].str.contains('Statewide'),'state',data['area_title'])
    data['geographic_level'] = np.where(data['area_title'].str.contains('Parish|City|Borough|County'),'county',data['geographic_level'])
    data['geographic_level'] = np.where(data['area_title'].str.contains('MSA'),'metropolitan_stat_area',data['geographic_level'])
    data['geographic_level'] = np.where(data['area_title'].str.contains('MicroSA'),'micropolitan_stat_area',data['geographic_level'])
    data['geographic_level'] = np.where(data['area_title'].str.contains('(Combined)'),'aggregated_stat_area',data['geographic_level'])
    data['geographic_level'] = np.where(data['area_title'].str.contains('TOTAL'),'nationwide',data['geographic_level'])
    data['geographic_level'] = np.where(data['area_title'].str.contains('Unknown'),'undefined',data['geographic_level'])
    data
        
    # make state column for filtering based on regex/np where parsing
    data[['area','state']] = data['area_title'].str.split(",",1,expand=True)
    #spl_word = ['--Statewide']
    data['state'] = np.where(data['state'].isnull(),data['area_title'].str.split('-- Statewide').str.get(0),data['state'])
    data['state'] = np.where(data['state'].str.contains('MSA'),data['area_title'].str.split(',').str.get(1),data['state'])



    return data

# make dataframe for total employment numbers

#industry as 10, ownership as 0
