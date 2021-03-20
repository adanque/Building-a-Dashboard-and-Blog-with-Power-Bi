"""
Author:     Alan Danque
Date:       20201003
Project:    Task: Blog Post
Step:       Exploratory Data Analysis
Purpose:    Wrangle the two datasets to be used for my exploratory data analysis.
"""

#from textblob import TextBlob
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandasql import sqldf
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from numpy.core.defchararray import find

import warnings
import matplotlib.cbook
warnings.filterwarnings("ignore",category=matplotlib.cbook.mplDeprecation)

print("Generate Pandas Profile for dfs8")
from pandas_profiling import ProfileReport

adf = pd.read_csv(".\\datasets\\Commercial_Aviation_Departures.csv")
bprof = ProfileReport(adf)
bprof.to_file(output_file='EDA_Commercial_Aviation_Departures_PandasProfileReport_output.html')




