import pandas as pd

#clean departments csv
def clean_departments():
    data= pd.read_csv("departments.csv",header=None,skiprows=0)
    clean = data[0].str.split(",", n=1, expand=False)
    clean.to_csv('departments.csv',index=False)
