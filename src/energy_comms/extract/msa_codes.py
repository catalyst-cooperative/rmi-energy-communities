import io
import pathlib
import pandas as pd
import numpy as np
import requests

#not sure why
#import energy_comms

#from link: https://www.bls.gov/oes/current/msa_def.htm

SOURCE_URL = 'https://www.bls.gov/oes/2021/may/area_definitions_m2021.xlsx'

def extract():
    # download area titles and convert to pandas dataframe

    d = pd.read_excel(SOURCE_URL)
    
    geoid_cols = ['FIPS code','County code','Township code']

    for col in geoid_cols:
        d[col] = d[col].astype(str)
    # do zfill on all geoid columns

    d['FIPS code'] = d['FIPS code'].str.zfill(2)
    d['County code'] = d['County code'].str.zfill(3)
    d['Township code'] = d['Township code'].str.zfill(3)

    # make geoid column with logic

    d['geoid'] = np.where(d['May 2021 MSA name'].str.contains('nonmetropolitan'),d['FIPS code'] + d['County code'],d['May 2021 MSA code '])

    return d

