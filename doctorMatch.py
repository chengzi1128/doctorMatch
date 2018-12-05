
import pandas as pd
import json
from pandas.io.json import json_normalize 

#########Read in json file########

url = './data_files/source_data.json'
df = pd.read_json(url, lines = True)
df_doctor=df['doctor'].apply(pd.Series)
#print(df_doctor.head(5))
df_practices = df['practices'].apply(pd.Series)
#print(df_practices.head(5))

######flatten the doctor column######

flat = pd.concat([df_doctor,df['practices']],axis=1)
#print(flat.head(3))
####concat with practice####
objs = [flat,pd.DataFrame(df['practices'].tolist())]
Drop = pd.concat(objs,axis=1).drop('practices',axis=1)
#print(Drop.head(3))
melt=pd.melt(Drop,value_name='practices',value_vars=[0,1,2], id_vars=['first_name','last_name','npi']).drop('variable',axis=1)
#print(melt.head(3))
#print(melt.loc[result['first_name'] == 'Quinton'])

######flatten the nested practices######

df_practices = melt['practices'].apply(pd.Series)
df1 = melt[['first_name','last_name','npi']]
Table = pd.concat([df1,df_practices],axis =1)
#print(Table.head(5))

######### Read in csv file######

Match_File = './data_files/match_file.csv'
match_data = pd.read_csv(Match_File)
#print(match_data.head(10))
# get the number of missing data points per column
print('Number of missing values in each column:')
missing_values_count = match_data.isnull().sum()


######### Process & Output #######

# look at the # of missing points in the first ten columns
print(missing_values_count[0:10])
# Number of documents scanned
print('Number of documents scanned:'+str(match_data['first_name'].count()))

# Number of Records matched by npi
merged_npi = pd.merge(df_doctor, match_data, on='npi')
S = 'Number of doctors matched by npi: '+str(merged_npi.shape[0])
print(S)

Table_cap = Table.apply(lambda x: x.astype(str).str.lower())
match_nonull = match_data.dropna(subset=['street','street_2','city','state','zip'])
match_data_cap = match_nonull.apply(lambda x: x.astype(str).str.lower())
#print(Table_cap.head(5))

merged_df = pd.merge(match_data_cap,Table_cap,how='inner',on = ['first_name', 'last_name','street','street_2','city','state','zip'])
print('Number of doctors matched by name and address: '+str(merged_df.shape[0]))
#merged_df.to_csv("matched_name_address.csv", index=False)

merged_practice = pd.merge(match_data_cap,Table_cap,how='inner',on = ['street','street_2','city','state','zip'], indicator=True)
print ('Number of practices matched: '+str(merged_practice.shape[0]))

left = match_data[(~match_data.npi.isin(merged_npi.npi))&(~match_data.street.isin(merged_practice.street))]
print ('Number of documents was not matched: '+str(left.shape[0]))
