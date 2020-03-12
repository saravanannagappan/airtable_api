# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 21:23:46 2020

@author: nagappas
"""

from airtable import Airtable 
import pandas as pd
from datetime import datetime
import os
import json

os.chdir('C:/Data Science/airtable/')

#Paste the API key here 
API_Key = ''


input_file = pd.read_excel('Data_Format.xlsx')
data = pd.DataFrame()

for index, row in input_file.iterrows():
    base_key = row['Application_ID']
    table_name = row['Table_Name']
    legacy = row['Legacy']
    application_name = row['Application_name']
    view_name = row['View_Name']
    column_names = json.loads(row['Column_Names'])    
    print(application_name)
    
    columnvalues = list(column_names.values())
    
    airtable_data = Airtable(base_key, table_name, api_key=API_Key)
    rows = []
    for page in airtable_data.get_iter(view=view_name, formula="FIND('Published', {" + column_names['Status']  + "})=1", fields=columnvalues) :
    
        for record in page:        
            row = {}
            row['Application_Name']=application_name
            row['Legacy']=legacy
            row['Table_Name']=table_name
            row['View_Name']=view_name
            for key in list(column_names):
                if column_names[key] in record['fields']:    
                    row[key] = record['fields'][column_names[key]]
                    
            if column_names['Publish Date'] in record['fields']:
                    dt = datetime.strptime(record['fields'][column_names['Publish Date']], '%Y-%m-%d')
                    row['Publish Month'] = dt.strftime("%B")
            
                
            rows.append(row)
                    
    data = data.append(pd.DataFrame(rows), ignore_index=True, sort=False)
    
data.to_csv('Data_sample.csv', index = False)
print("Process Completed")
