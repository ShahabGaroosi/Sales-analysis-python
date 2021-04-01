#Import libraries
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

#Read data
filePath = "C:/Users/shaha/Downloads/KPMG_DA_Case_Y21/03 DA Case/Raw data/"
dtypes={'USERID': 'Int64','ITEM':'Int64','QTY':'Int64','DISCOUNT':'Int64','SHIPDAYS':'Int64','RATING':'Int64','TRACKNO':'Int64','PURCHASEPRICE':float,'SALEPRICE':float}
CustomerData = pd.read_csv(filePath+'CustomerData.txt', sep="|", na_values=' ', parse_dates=['DOB'], dtype=dtypes)
ItemData = pd.read_csv(filePath+'ItemData.txt', sep="|", na_values=' ', dtype=dtypes)
TransactionsData = pd.read_csv(filePath+'TransactionsData.txt', sep="|", na_values='', dtype=dtypes, parse_dates=['DELIVERYDATE'])
TransactionsData.TIMESTAMP=pd.to_timedelta(TransactionsData.TIMESTAMP)

#Overview the dataframes
CustomerData
ItemData
TransactionsData

#Get the number of missing values
CustomerData.isnull().sum()
ItemData.isnull().sum()
TransactionsData.isnull().sum()

#Fill missing values
CustomerData.fillna(' ', inplace=True)

#Get and sort all unique field values
for DB in [CustomerData,ItemData,TransactionsData]:
    for i in DB:
        if DB[i].dtypes=='O':
            sorted(DB[i].dropna().unique())

#Check for duplicate elements
CustomerData.duplicated(subset='USERID').any()
ItemData.duplicated(subset=['ITEM','SUPLID']).any()
TransactionsData.duplicated(subset=['USERID','ITEM','WAREHOUSE','TRACKNO']).any()

#Check if duplicate elements has mismatching field values.
len(ItemData[ItemData.duplicated()])!=len(ItemData[ItemData.duplicated(subset=['ITEM','SUPLID'])])

#Drop duplicate elements
ItemData.drop_duplicates(subset=['ITEM','SUPLID'],inplace=True)

#Connect dataframes
df=TransactionsData.merge(ItemData, on='ITEM', how='left').merge(CustomerData, on='USERID', how='left').drop_duplicates(subset=['USERID','ITEM','WAREHOUSE','TRACKNO'])

#Check number of completed transactions and check concerning missing values
TransactionsData[TransactionsData['PURCHASE']=='YES'].groupby(['USERID'])['PURCHASE'].count().describe()
TransactionsData[TransactionsData.PURCHASE!='NO'].fillna({'PURCHASE':'YES'}).groupby(['USERID']).PURCHASE.count().describe()
TransactionsData[TransactionsData.USERID.isnull()&(TransactionsData.PURCHASE!='NO')].fillna({'USERID':0,'PURCHASE':'YES'}).PURCHASE.count()

#Get the most popular item category and check concerning missing values54. df[df.PURCHASE=='YES'].groupby(['CATEGORY']).QTY.sum().sort_values(ascending=False)
df[['PURCHASE','QTY','CATEGORY']][df.PURCHASE!='NO'].isna().sum()
df[df.CATEGORY.isnull()][['PURCHASE','QTY']]
df[df.QTY.isnull()&(df.PURCHASE!='NO')].fillna({'PURCHASE':-1,'QTY':-1}).groupby(['CATEGORY','PURCHASE']).QTY.count().sort_values(ascending=False)
df[df.PURCHASE=='YES'].groupby(['CATEGORY']).QTY.describe().round(2)

#Get total transaction values and check concerning missing values
((df.SALEPRICE*(1-df.DISCOUNT/100)-df.PURCHASEPRICE)*df.QTY)[df.PURCHASE=='YES'].dropna().sum()
((df.SALEPRICE*(1-df.DISCOUNT/100)-df.PURCHASEPRICE)*df.QTY)[df.PURCHASE=='YES'].isnull().sum()/((df.SALEPRICE*(1-df.DISCOUNT/100)-df.PURCHASEPRICE)*df.QTY)[df.PURCHASE=='YES'].dropna().count()
df[['PURCHASE','QTY','DISCOUNT','PURCHASEPRICE','SALEPRICE']][df.PURCHASEPRICE.isnull()]
((df.SALEPRICE*(1-df.DISCOUNT.fillna(100)/100)-df.PURCHASEPRICE)*df.QTY.fillna(5)*(df.PURCHASE.apply(lambda x: 1 if x!='NO' else 0))).apply(lambda x: (x if x<0 else 0)).sum()

#Add profit column to dataframe and rescale profit and quantities column
Profit=(df.SALEPRICE*(1-df.DISCOUNT/100)-df.PURCHASEPRICE)*df.QTY*(df.PURCHASE.apply(lambda x: 1 if x=='YES' else (0 if x=='NO' else float('nan'))))
Profit=Profit/Profit.dropna().sum()
df.QTY=df.QTY/df.QTY.dropna().sum()
df=pd.concat([df,Profit.rename("PROFIT")],axis=1)[df.PURCHASE!='NO']

#Plot for interesting data pattern and anomalies
df[df.DELIVERYDATE.notna()].groupby(pd.Grouper(freq='M',key="DELIVERYDATE"))[['QTY','PROFIT']].sum().plot(ylabel='Percentage (%)', kind='bar')
df[df.TIMESTAMP.notna()].groupby(pd.Grouper(freq='H',key="TIMESTAMP"))[['QTY','PROFIT']].sum().plot(ylabel='Number of Transaction', kind='bar')
df[df.DOB.notna()].groupby(pd.Grouper(freq='5Y',key="DOB"))[['QTY','PROFIT']].sum().plot(xlabel='DOB', ylabel='Percentage (%)', kind='bar')
df.groupby(by=['PPC_ADD'])[['QTY','PROFIT']].sum().sort_values(by=['PROFIT'],ascending=False).plot(xlabel='PPC_ADD',ylabel='Percentage (%)', kind='bar')
df.groupby(by=['WEBBROWSER']).PURCHASE.count().sort_values(ascending=False).plot(xlabel='WEBBROWSER',ylabel='Number of Transaction', kind='bar')
df.groupby(by=['CATEGORY']).sum()[['QTY','PROFIT']].sort_values(by=['PROFIT'],ascending=False).plot(xlabel='CATEGORY',ylabel='Percentage (%)', kind='bar')
df.groupby(by=['EDUCATION']).sum()[['QTY','PROFIT']].sort_values(by=['PROFIT'],ascending=False).plot(xlabel='EDUCATION',ylabel='Percentage (%)', kind='bar')
CustomerData.groupby(by=['HOBBY']).USERID.count()
df.groupby(by=['HOBBY']).sum()[['QTY','PROFIT']].sort_values(by=['PROFIT'],ascending=False).plot(xlabel='HOBBY',ylabel='Percentage (%)', kind='bar')
plt.show()
