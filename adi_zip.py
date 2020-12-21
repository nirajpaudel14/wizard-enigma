# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 20:40:58 2020

@author: u6029558
"""
import pandas as pd
import glob 

path1 = r'D:\Users\u6029558\Desktop\hcup\datasets\State Census Block Group to Zip Conversions'
geo_files = glob.glob(path1 + "/*.csv")

all_geodf = []

for filename in geo_files:
    df = pd.read_csv(filename, index_col = None, header = 0, encoding= "cp1252", skiprows = [1], dtype = {'state' : str, 'zcta5' : str, 'county' : str, 'tract' : str, 'bg' :str})
    all_geodf.append(df)

geoframe = pd.concat(all_geodf, axis = 0, ignore_index = True)
geoframe['tract'] = (geoframe['tract'].replace('\.', '', regex = True))
geoframe['geoid10'] = geoframe['county'] + geoframe['tract'] + geoframe['bg']


path2 = r'D:\Users\u6029558\Desktop\hcup\datasets\State ADI Data'
adi_files = glob.glob(path2 + "/*.txt")

all_adidf = []

for filename in adi_files:
    df = pd.read_csv(filename, index_col = None, header = 0, dtype = {'GEOID10': str})
    all_adidf.append(df)
    
adiframe =pd.concat(all_adidf, axis = 0, ignore_index = True)


path3 = r'D:\Users\u6029558\Desktop\hcup\zip_only.csv'
zipdf = pd.read_csv(path3, dtype = str)

adi_geo = pd.merge(left = geoframe, right =adiframe, left_on = 'geoid10', right_on = 'GEOID10')

af_zip = pd.merge(left =zipdf, right = adi_geo, left_on = 'zip', right_on = 'zcta5')


df_final = af_zip[['zip','stab', 'GEOID10','zcta5','afact','ADI_STATERNK', 'ADI_NATRANK']].drop_duplicates()

#a = df_final.loc[df_final['ADI_STATERNK'] == 'P']

df_final = df_final[df_final['ADI_STATERNK'].isin(['U', 'P', 'NA']) == False]
df_final = df_final[df_final['ADI_NATRANK'].isin(['U', 'P', 'NA']) == False]

df_final[['ADI_STATERNK', 'ADI_NATRANK']] = df_final[['ADI_STATERNK', 'ADI_NATRANK']].apply(pd.to_numeric)

df_final.dtypes

df_final['wei_adistate'] = df_final['afact']*df_final['ADI_STATERNK']
df_final['wei_adinat'] = df_final['afact']*df_final['ADI_NATRANK']
df_final1 = df_final[['zcta5', 'stab', 'wei_adistate', 'wei_adinat']]

df_final1 = df_final1.groupby(['zcta5', 'stab']).sum().reset_index()

a = df_final1.loc[df_final1['zcta5'] == '86514']

## for count by zip
##b = df_final1.zcta5.value_counts()

n= len(pd.unique(df_final['zcta5']))
ls = list(df_final1['zcta5'].unique())


diff = zipdf[zipdf['zip'].isin(ls) == False]

df_final1.to_csv('test.csv', index =False)







