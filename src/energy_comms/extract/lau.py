import pandas as pd
import numpy as np
import requests

LAU_URL = f'https://download.bls.gov/pub/time.series/la/'

FILE_NAMES = ['la.area','la.data.0.CurrentU10-14','la.data.0.CurrentU15-19','la.data.0.CurrentU20-24']

def extract_lau():
    for file in FILE_NAMES:
        downloads = LAU_URL+file
        resp = requests.get(downloads)

        output = open(file+'.txt','wb')
        output.write(resp.content,)
        output.close()

def get_lau_areas():

    df_areas = pd.read_table('la.area.txt')
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
    df_bls = pd.read_table(lau_file)

    df_areas = get_lau_areas()

    df_bls.columns = df_bls.columns.str.replace(' ','')

    df_bls['series_id'] = df_bls['series_id'].str.replace(' ','')

    df = df_bls.merge(df_areas,on='series_id',how='left')

    df = df.query("geographic_level== 'metropolitan_stat_area' | geographic_level == 'county'")
    
    # M13 is annual average
    df = df.query("period=='M13'")

    return df 

