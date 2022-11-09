#import dispatch
import pandas as pd
import qcew_2014_2021
from qcew_2014_2021 import *
from pathlib import Path
import io
import glob
import os


"""Function that downloads zip file through an online URL"""

old_years = ['2010','2011','2012','2013']


def download_qcew_zip_files(yr):

    # importing necessary modules
    import requests, zipfile
    from io import BytesIO
    print('Downloading started')

    #only did one year, didn't work

        #Defining the zip file URL
    url = f'https://data.bls.gov/cew/data/files/{yr}/csv/{yr}_annual_by_area.zip'

        # Split URL to get the file name
    filename = url.split('/')[-1]

        # Downloading the file by sending the request to the URL
    req = requests.get(url)
    print('Downloading Completed')

        # extracting the zip file contents
    zipfile= zipfile.ZipFile(BytesIO(req.content))
    zipfile.extractall('/Users/mariacastillo-coding/Documents/qcew/')

"""Attempt to make directory into a large dataframe"""

def make_year_df(yr):

    mycsvdir_qcew_files = f'/Users/mariacastillo-coding/Documents/qcew/{yr}.annual.by_area'
    csvfiles = glob.glob(os.path.join(mycsvdir_qcew_files, '*.csv'))

    # get all the csv files in that directory (assuming they have the extension .csv)
    csvfiles = glob.glob(os.path.join(mycsvdir_qcew_files, '*.csv'))

    # loop through the files and read them in with pandas
    dataframes = []  # a list to hold all the individual pandas DataFrames
    for csvfile in csvfiles:
            df = pd.read_csv(csvfile)
            dataframes.append(df)

        # concatenate them all together
    df = pd.concat(dataframes, ignore_index=True)

    return df

def extract_2010():
    for yr in old_years:
     download_qcew_zip_files(yr)


    data_2010 = make_year_df(2010)
    data_2011 = make_year_df(2011)
    data_2012 = make_year_df(2012)
    data_2013 = make_year_df(2013)

    data_dfs = [data_2010,data_2011,data_2012,data_2013]

    df = pd.concat(data_dfs,ignore_index=True)


    return df