# extract area titles for all geographic identifiers in BLS QCEW data set, then make into pandas dataframe


import io
import pathlib
import pandas as pd
import requests

#not sure why
#import energy_comms

#from link: https://www.bls.gov/cew/classifications/areas/qcew-area-titles.htm

SOURCE_URL = 'https://www.bls.gov/cew/classifications/areas/area-titles-xlsx.xlsx'

def extract():
    # download area titles and convert to pandas dataframe

    d = pd.read_excel(SOURCE_URL)
    
    # make sure fips code is always 5 digit

    d['area_fips'] = d['area_fips'].str.zfill(5)


    return d
