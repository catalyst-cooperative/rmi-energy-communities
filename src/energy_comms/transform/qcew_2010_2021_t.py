import pandas as pd
import numpy as np
import energy_comms

from energy_comms.extract.qcew_2014_2021 import get_2014_industry_with_bls_geographic_tag as geo_tag

from energy_comms.extract.msa_codes import extract as extract_msa
    #import national unemployment data
from energy_comms.extract.cps import extract_API as cps_extract
from energy_comms.transform.cps import transform as cps_transform

#import local unemployment data
from energy_comms.extract.lau import get_data as lau_extract


#from qcew_2010_2013 import *


FOSSIL_NAICS_CODES = ['2121','211','213','23712','486','4247','22112']


def transform(df):

    #create total employment

    total_employment = df.query("industry_code == '10' & own_code == 0")
    total_employment = geo_tag(total_employment)
    total_employment = total_employment.rename(columns={'annual_avg_emplvl':'total_employees'})
    total_employment_df = total_employment[['area_fips','area_title','geographic_level','year','total_employees']]

   # create fossil employment
    fossil_employment = df.loc[df['industry_code'].isin(FOSSIL_NAICS_CODES)]
    fossil_employment = geo_tag(fossil_employment)
    total_fossil_employment = fossil_employment.groupby(['area_fips','area_title','geographic_level','year']).agg({'annual_avg_emplvl':'sum'}).reset_index()
    total_fossil_employment = total_fossil_employment.rename(columns={'annual_avg_emplvl':'fossil_employees'})
    total_fossil_employment_df = total_fossil_employment[['area_fips','area_title','geographic_level','year','fossil_employees']]

    #combine for total and fossil 2010-2013
    combined = total_employment_df.merge(total_fossil_employment_df,on=['area_fips','area_title','geographic_level','year'],how='left')
    
    # fix dtypes
    combined['year'] = combined['year'].astype(str)

    #geoid processing

    # clean area fips column first
    combined['area_fips'] = combined['area_fips'].astype(str)
    #make 5 digits for 4 digit occurences
    combined['area_fips'] = combined['area_fips'].apply(lambda x:str(x).zfill(5))
    #make separate column for MSA logic
    combined['geoid'] = combined['area_fips']
    # take out C in MSA to add extra 0 
    combined['geoid'] = combined['geoid'].str.replace('C','')
    # for MSAs, make geoid to match census crosswalk
    combined['geoid'] = np.where(combined['geographic_level']=='metropolitan_stat_area',combined['geoid'] + '0',combined['geoid'])
    # only keep geo levels we need (e.g. remove state and country totals)
    final_employment = combined.query("geographic_level=='county' or geographic_level=='metropolitan_stat_area'")

    # do msa to county cross walk
    msa = extract_msa()

    msa['geoid'] = msa['geoid'].astype(str)
    employment = final_employment.merge(msa,on='geoid',how='inner')
    employment['fips_county'] = np.where(employment['geographic_level']=='county',employment['geoid'],employment['FIPS code'] + employment['County code'])

    #create eligibility criteria # 1) % fossil employees
    employment['percent_fossil_employment'] = employment['fossil_employees'] / employment['total_employees'] * 100
    employment['meets_fossil_threshold'] = np.where(employment['percent_fossil_employment'] > .17,1,0)
 
    #create eligibility criteria # 2) unemployment rate higher than previous years


    national_unemployment = cps_transform(cps_extract())

    local_unemployment = lau_extract()

    #get final unemployment data

    unemployment = local_unemployment.merge(national_unemployment,on='year',how='left')

    # make column a float for comparison
    unemployment['local_area_unemployment'] = pd.to_numeric(unemployment['local_area_unemployment'],errors='coerce')

    #fix geoid for merge with employment

    unemployment['geoid'] = np.where(unemployment['geographic_level']=='county',unemployment['state_code'] + unemployment['geoid'],unemployment['geoid'].str[0:5])

    employment_both = employment.merge(unemployment,on=['geoid','geographic_level','year'],how='inner')
    employment_both['meets_unemployment_criteria'] = np.where(employment_both['local_area_unemployment'] > employment_both['national_unemployment_rate_prev_year'],1,0)

    #identify which areas meet the unemployment criteria

    eligible_employment_areas = employment_both.query("meets_fossil_threshold==1 & meets_unemployment_criteria==1")

    # clean for export
    eligible_employment_areas = eligible_employment_areas.rename(columns={'May 2021 MSA name':'msa_name'})
    eligible_employment_areas = eligible_employment_areas[['area_title','geographic_level','State','msa_name','geoid','percent_fossil_employees','meets_fossil_threshold','local_area_unemployment','national_unemployment_rate_prev_year','meets_unemployment_criteria','fips_county']]

    #narrow exmport for patio

    patio_employment = eligible_employment_areas[['area_title','fips_county']]

    # columns in patio
    patio_employment['qualifying_area'] = 'msa_or_county'

    patio_employment['criteria'] = 'fossil_employment'

    return patio_employment
