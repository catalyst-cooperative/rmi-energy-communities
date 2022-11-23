#imports

import requests
import json
import prettytable
import pandas as pd



def transform(x):
    # convert API pretty table to pandas dataframe
  
    tbl_as_csv = x.get_csv_string().replace('\r','')

    txt = open('national_unemployment.csv','w')

    n = txt.write(tbl_as_csv)
    txt.close()

    cps = pd.read_csv('national_unemployment.csv')

    annual_average = cps.groupby('year').agg({'value':'mean'}).reset_index()

    # round to match official sig figures on BLS website

    annual_average = annual_average.round(1)

    # manually input missing years data from: 
    #https://www.bls.gov/cps/tables.htm#empstat (GETTING API FIXED)
    annual_average = annual_average.append({'year':2019,'value':3.7},ignore_index=True)
    annual_average = annual_average.append({'year':2020,'value':8.1},ignore_index=True)
    #annual_average = annual_average.append({'year':2021,'value':5.3},ignore_index=True)

    # make year column an int - looks weird and will probs be a dtype issue
    annual_average['year'] = annual_average['year'].astype(int)

    #rename value column for merge
    annual_average = annual_average.rename(columns={'value':'national_unemployment_rate_prev_year'})

    #ADD 1 to each year since criteria is for PREVIOUS YEAR!!!

    annual_average['year'] = annual_average['year'] + 1

    return annual_average