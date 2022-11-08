import pandas as pd
import numpy as np
import energy_comms

from qcew_2010_2013 import *



def transform(df,employment_type):

    #employment type - fossil or total

    if employment_type == 'total':
           agg_df = df.query("industry_code == '10' & own_code == 0")

    

 