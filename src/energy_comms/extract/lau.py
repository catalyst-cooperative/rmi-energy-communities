import pandas as pd
import numpy as np
import requests
from pathlib import Path
import io
import glob
import os
import requests, zipfile
from io import BytesIO

LAU_URL = f'https://download.bls.gov/pub/time.series/la/'
CACHE_DIR = Path.home() / 'Documents/lau_cache'

lau_ranges = ['10-14','15-19','20-24']
FILE_NAMES = ['la.data.0.CurrentU10-14','la.data.0.CurrentU15-19','la.data.0.CurrentU20-24']

#FILE_NAMES = ['la.area','la.data.0.CurrentU10-14','la.data.0.CurrentU15-19','la.data.0.CurrentU20-24']
def extract_lau():
   if not CACHE_DIR.exists():
        CACHE_DIR.mkdir(parents=True)

   for file in FILE_NAMES:
        downloads = LAU_URL+file
        resp = requests.get(downloads)
    # not sure how to incorporate CACHE_DIR here yet (mix path with str)
        output = open(f'/Users/mcastillo/Documents/lau_cache/{file}.txt','wb')
        output.write(resp.content,)
        output.close()

        df_areas = pd.read_table(CACHE_DIR / 'la.area.txt')
        # only keep columns we want
        df_areas = df_areas[['area_code','area_text']]
        # only keep observations with county and MSA

        df_areas = df_areas.loc[df_areas['area_code'].str.contains('CN') |df_areas['area_code'].str.contains('MT')]
        df_areas.reset_index()

        df_areas['geographic_level'] = np.where(df_areas['area_code'].str.contains('CN'),'county','metropolitan_stat_area')

        df_areas['state_code'] = df_areas['area_code'].str[2:4]

        df_areas['geoid'] = np.where(df_areas['geographic_level']=='county',df_areas['area_code'].str[4:7],df_areas['area_code'].str[4:10])

        df_areas['series_id'] =  'LAU' + df_areas['area_code'].str[0:2] + df_areas['state_code'] + df_areas['geoid']

        df_areas['series_id'] = np.where(df_areas['geographic_level']=='county',df_areas['series_id']+'00000000',df_areas['series_id']+'00000')

        df_areas['series_id'] = df_areas['series_id'] + '03'
    
        return df_areas

 
# let's use an example
def get_lau_data(lau_file):

    df_areas = extract_lau()

    df_bls = pd.read_table(CACHE_DIR / lau_file)
  
    df_bls.columns = df_bls.columns.str.replace(' ','')

    df_bls['series_id'] = df_bls['series_id'].str.replace(' ','')

    df = df_bls.merge(df_areas,on='series_id',how='left')

    df = df.query("geographic_level== 'metropolitan_stat_area' | geographic_level == 'county'")
    
    # M13 is annual average
    df = df.query("period=='M13'")

    return df 
 

def get_data():
    
    years = ['2010','2015','2020']

    my_dict = {}

    for year in years:

        year_file_combination = {'2010':'la.data.0.CurrentU10-14.txt','2015':'la.data.0.CurrentU15-19.txt','2020':'la.data.0.CurrentU20-24.txt'}

        
        #for file in files:
        my_dict['df_' + year] = get_lau_data(year_file_combination[year])
    
    df = pd.concat(my_dict.values())
    
    df = df.rename(columns={'value':'local_area_unemployment'})

    lau_df = df[['area_text','geographic_level','state_code','geoid','year','local_area_unemployment']]

    lau_df

    return lau_df



