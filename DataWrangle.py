"""
Author:     Alan Danque
Date:       20200907
Project:    Task: Dashboard
Step:       Data Wrangling
Purpose:    Wrangle the two datasets to be used for my exploratory data analysis.
"""

import numpy as np
import pandas as pd
from pandasql import sqldf
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from numpy.core.defchararray import find
import datetime

import warnings
import matplotlib.cbook
warnings.filterwarnings("ignore",category=matplotlib.cbook.mplDeprecation)


def reverse(x):
    # Returns string in reverse
    return x[::-1]

def locationcontain(val):
    # Returns location of comma
    if "," in val:  #val.str.contains(","):
        valout = val.index(',')
    else:
        valout = 0
    return valout

def retlastchars(valin, valoc):
    # Gets the string upto an index and evaluates the Country value
    if valoc > 0:
        USSTATES = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
                    'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
                    'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
                    'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico',
                    'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania',
                    'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont',
                    'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
        valoc = valoc - 1
        valout = valin[-valoc:]
        if valout in USSTATES:
            valout = "USA"
    else:
        valout = valin
    return valout

#Step 1:  Load data into the dataframes to unify
afpydf1 = pd.read_excel('.\\datasets\\Accidents and fatalities per year.xlsx', sheet_name='Accident list 2015')
afpydf2 = pd.read_excel('.\\datasets\\Accidents and fatalities per year.xlsx', sheet_name='Accident list 2016')
afpydf3 = pd.read_excel('.\\datasets\\Accidents and fatalities per year.xlsx', sheet_name='Accident list 2017')
afpydf4 = pd.read_excel('.\\datasets\\Accidents and fatalities per year.xlsx', sheet_name='Accident list 2018')
afpydf5 = pd.read_excel('.\\datasets\\Accidents and fatalities per year.xlsx', sheet_name='Accident list 2019')
afpydf6 = pd.read_excel('.\\datasets\\Accidents and fatalities per year.xlsx', sheet_name='Accident list 2020')

pysqldf = lambda t: sqldf(t, globals())
t = """
SELECT 
      a.Date, a.Type, a.Operator, a.Fatalities, a."Flight type", a.Phase, a.Location, a.Country, "" as Category
            FROM afpydf1 a where a.Date <> "NaT"          
union
SELECT 
      a.Date, a.Type, a.Operator, a.Fatalities, a."Flight type", a.Phase, a.Location, a.Country, "" as Category
            FROM afpydf2 a where a.Date <> "NaT"            
union
SELECT 
      a.Date, a.Type, a.Operator, a.Fatalities, a."Flight type", a.Phase, a.Location, a.Country, "" as Category
            FROM afpydf3 a where a.Date <> "NaT"
union
SELECT 
      a.Date, a.Type, a.Operator, a.Fatalities, a."Flight type", a.Phase, a.Location, a.Country, a."Category 
(unconfirmed)"
            FROM afpydf4 a where a.Date <> "NaT"
union
SELECT 
      a.Date, a.Type, a.Operator, a.Fatalities, a."Flight type", a.Phase, a.Location, a.Country, a."Category 
(unconfirmed)"
            FROM afpydf5 a where a.Date <> "NaT"
union
SELECT 
      a.Date, a.Type, a.Operator, a.Fatalities, a."Flight type", a.Phase, a.Location, a.Country, a."Category 
(unconfirmed)"
            FROM afpydf6 a where a.Date <> "NaT"
                                               
        ;"""


dfs1 = pysqldf(t)
print(dfs1)
dfs1_sorted = dfs1.sort_values('Date',ascending=True)
dfs1_sorted.to_csv('.\\datasets\\2015-2020-DataSet.csv')

#Step 2:  Load data into the dataframe
# Supplemental DataSet to combine
# https://data.world/hhaveliw/airplane-crashes-1908-2009
# https://www.kaggle.com/saurograndi/airplane-crashes-since-1908
adf = pd.read_csv(".\\datasets\\Airplane_Crashes_and_Fatalities_Since_1908.csv")

pysqldf = lambda t: sqldf(t, globals())
t = """
SELECT 
      a.Date, a.Type, a.Operator, a.Fatalities, a.Route, a.Location, a.Location as Country, Summary
            FROM adf a where a.Date <> "NaT"    
   ;"""
dfs1 = pysqldf(t)

# Reverse the string order of the location to get country using the following comman search
dfs1['rtval'] = dfs1['Country'].astype(str).apply(lambda x: x[::-1])
# Get the location of the first comma to identify country
dfs1['rtva2'] = dfs1['rtval'].apply(locationcontain)
dfs1['Country'] = dfs1.apply(lambda x: retlastchars(x.Country, x.rtva2), axis=1)
del dfs1['rtval']
dfs2_sorted = dfs1.sort_values('Date',ascending=True)
dfs1.to_csv('.\\datasets\\Since1908.csv')

# Unify both datasets
pysqldf = lambda t: sqldf(t, globals())
t = """
SELECT 
      a.Date as Date, a.Type, a.Operator, a.Fatalities, a."Flight type", a.Phase, a.Location, a.Country, Category
            FROM dfs1_sorted a where a.Date <> "NaT"          
union
SELECT 
      a.Date as Date, a.Type, a.Operator, a.Fatalities, a.Route as "Flight type", "" as Phase, a.Location, a.Country, Summary as Category
            FROM dfs2_sorted a where a.Date <> "NaT"     
   ;"""
dfs1 = pysqldf(t)
dfs1['Date']= pd.to_datetime(dfs1['Date'])
dfs1['Year']= dfs1['Date'].dt.strftime('%Y')
dfs1['Month']= dfs1['Date'].dt.month
dfs1['DayOfWeek']= dfs1['Date'].dt.dayofweek
# (Monday =0, Tuesday=1, Wednesday=2,Thursday =3,  Friday=4 ,  Saturday =5, Sunday =6)
dfs1_sorted = dfs1.sort_values('Date',ascending=True)
dfs1_sorted.to_csv('.\\datasets\\UnifiedDataSet.csv')