import happybase
import pandas as pd
from datetime import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import warnings
warnings.filterwarnings("ignore")




VM_address = 'localhost'
connection = happybase.Connection(VM_address)
twitter = connection.table('twitter')

col_names = [b'Hashtags:AAPL_tweet_count',
b'Hashtags:GOOG_tweet_count',
b'Hashtags:MSFT_tweet_count',
b'Hashtags:TSLA_tweet_count',
b'Id:Time']

col_names_no_time = [b'Hashtags:AAPL_tweet_count',
b'Hashtags:GOOG_tweet_count',
b'Hashtags:MSFT_tweet_count',
b'Hashtags:TSLA_tweet_count']

df = pd.DataFrame(columns = col_names)
for key,data in twitter.scan():
    print()
    df= df.append(data,ignore_index = True)
    
for col in col_names_no_time:
    df[col]=df[col].apply(lambda x: np.nan if x==b'None' else x  ).astype(float)     
    
df[b'Id:Time']=df[b'Id:Time'].apply(lambda x: str(x)[2:-1]  )
df[b'Id:Time']  = pd.to_datetime(df[b'Id:Time'])

df = df.rename(columns={
b'Hashtags:AAPL_tweet_count': "AAPL_tweet_count", 
b'Hashtags:GOOG_tweet_count': "GOOG_tweet_count",
b'Hashtags:MSFT_tweet_count': "MSFT_tweet_count",
b'Hashtags:TSLA_tweet_count': "TSLA_tweet_count",
b'Id:Time' : "Time"    
})



twitter = df


VM_address = 'localhost'
connection = happybase.Connection(VM_address)
alphavantage = connection.table('alphavantage')


col_names = [
 b'GOOG:close',
 b'GOOG:high',
 b'GOOG:low',
 b'GOOG:open',
 b'GOOG:volume',
 b'MSFT:close', 
 b'MSFT:high',
 b'MSFT:low' ,
 b'MSFT:open' ,
 b'MSFT:volume', 
 b'TSLA:close' ,
 b'TSLA:high' ,
 b'TSLA:low' ,
 b'TSLA:open' ,
 b'TSLA:volume',
 b'AAPL:close' ,
 b'AAPL:high' ,
 b'AAPL:low',
 b'AAPL:open',
 b'AAPL:volume']

col_names_no_time = [
 b'GOOG:close',
 b'GOOG:high',
 b'GOOG:low',
 b'GOOG:open',
 b'GOOG:volume',   
 b'MSFT:close', 
 b'MSFT:high',
 b'MSFT:low' ,
 b'MSFT:open' ,
 b'MSFT:volume', 
 b'TSLA:close' ,
 b'TSLA:high' ,
 b'TSLA:low' ,
 b'TSLA:open' ,
 b'TSLA:volume',
 b'AAPL:close' ,
 b'AAPL:high' ,
 b'AAPL:low',
 b'AAPL:open',
 b'AAPL:volume']


df = pd.DataFrame(columns = col_names)
for key,data in alphavantage.scan():
    print()
    df= df.append(data,ignore_index = True)

for col in col_names_no_time:
    df[col]=df[col].apply(lambda x: np.nan if x==b'None' else x  ).astype(float)     
    
df[b'Id:Time']=df[b'Id:Time'].apply(lambda x: str(x)[2:-1]  )
df[b'Id:Time']  = pd.to_datetime(df[b'Id:Time'])    

df = df.rename(columns={
b'AAPL:close': "AAPL_close", 
b'AAPL:high': "AAPL_high",
b'AAPL:low': "AAPL_low",
b'AAPL:open': "AAPL_open",
b'AAPL:volume': "AAPL_volume",
b'GOOG:close': "GOOG_close", 
b'GOOG:high': "GOOG_high",
b'GOOG:low': "GOOG_low",
b'GOOG:open': "GOOG_open",
b'GOOG:volume': "GOOG_volume",
b'MSFT:close': "MSFT_close", 
b'MSFT:high': "MSFT_high",
b'MSFT:low': "MSFT_low",
b'MSFT:open': "MSFT_open",
b'MSFT:volume': "MSFT_volume",
b'TSLA:close': "TSLA_close", 
b'TSLA:high': "TSLA_high",
b'TSLA:low': "TSLA_low",
b'TSLA:open': "TSLA_open",
b'TSLA:volume': "TSLA_volume",
b'Id:Time': "Time"
})

alpha = df




alpha = alpha.sort_values('Time')
twitter = twitter.sort_values('Time')




alpha['year'] = alpha['Time'].apply(lambda x: x.year).astype(str)
alpha['month'] = alpha['Time'].apply(lambda x: x.month).astype(str)
alpha['day'] = alpha['Time'].apply(lambda x: x.day).astype(str)
alpha['hour'] = alpha['Time'].apply(lambda x: x.hour).astype(str)
alpha['date_hour'] = alpha['year'] + "-" + alpha['month'] + "-" + alpha['day'] + " " + alpha['hour'] + ":00"

twitter['year'] = twitter['Time'].apply(lambda x: x.year).astype(str)
twitter['month'] = twitter['Time'].apply(lambda x: x.month).astype(str)
twitter['day'] = twitter['Time'].apply(lambda x: x.day).astype(str)
twitter['hour'] = twitter['Time'].apply(lambda x: x.hour).astype(str)
twitter['date_hour'] = twitter['year'] + "-" + twitter['month'] + "-" + twitter['day'] + " " + twitter['hour'] + ":00"

companies_dict = {'Google': 'GOOG',
                 'Microsoft': 'MSFT',
                 'Tesla': 'TSLA',
                 'Apple': 'AAPL'}

def filter_df(df,date_from= '2023-01-01',date_to= datetime.today()):
    return df[df['Time']>=date_from][df['Time']<=date_to]

def volume_stats_compare(company_name_a='Tesla',
                         company_name_b='Google',
                        date_from = '2023-01-01',
                        date_to = datetime.today()):
    alpha_filter = filter_df(alpha,date_from,date_to)
    stats_a = alpha_filter[companies_dict[company_name_a]+'_volume'].describe()
    stats_b = alpha_filter[companies_dict[company_name_b]+'_volume'].describe()
    
    compare = pd.DataFrame(columns=['Statystyka',company_name_a,company_name_b])
    
    stats = ['mean','std','min','max']
    for stat in stats:
        compare = compare.append({'Statystyka': stat, 
                                  company_name_a: stats_a[stat], 
                                  company_name_b: stats_b[stat]},
                                ignore_index=True)
    compare['Różnica'] = compare[company_name_a] - compare[company_name_b]
    print('Porównanie wolumenów dla firm '+company_name_a+' oraz '+company_name_b+' w zadanym okresie:')
    print()
    print(compare.head())

def plot_companies(include = ['Google','Microsoft','Tesla','Apple'], 
                   date_from = '2023-01-01', 
                   date_to = datetime.today(),
                    close = False):
    alpha_filter = filter_df(alpha,date_from,date_to)
    fig, ax = plt.subplots(figsize=(10,6))
    if close:
        plt.title('Porównanie kursów zamknięcia w interwałach 5min')
        for inc in include:
            plt.plot(alpha_filter['Time'],alpha_filter[companies_dict[inc]+"_close"],label=inc)
    else:
        plt.title('Porównanie kursów otwarcia w interwałach 5min')
        for inc in include:
            plt.plot(alpha_filter['Time'],alpha_filter[companies_dict[inc]+"_open"],label=inc)
    plt.xticks(rotation=45)
    plt.legend()
    plt.show()

def plot_boxplot(company_name = 'Google',
                 date_from = '2023-01-01', 
                date_to = datetime.today(),
                close = False
                ):
    alpha_filter = filter_df(alpha,date_from,date_to)
    fig, ax = plt.subplots(figsize=(14,6))
    if close:
        sns.boxplot(x = alpha_filter['date_hour'], 
                y = alpha_filter[companies_dict[company_name]+'_close'], 
                ax = ax,
               color='skyblue')
        plt.xticks(rotation=45)
        plt.title('Kurs zamknięcia dla '+company_name+' zagregowany do godziny')
        plt.show()
    else:
        sns.boxplot(x = alpha_filter['date_hour'], 
                y = alpha_filter[companies_dict[company_name]+'_open'], 
                ax = ax,
               color='skyblue')
        plt.xticks(rotation=45)
        plt.title('Kurs otwarcia dla '+company_name+' zagregowany do godziny')
        plt.show()

def tweets_stock(company_name = 'Tesla',
                date_from = '2023-01-01',
                date_to = datetime.today(),
                close = False):
    fig, ax = plt.subplots(figsize=(10,6))
    alpha_filter = filter_df(alpha,date_from,date_to)
    twitter_filter = filter_df(twitter,date_from,date_to)
    
    if close:
        ax.plot(alpha_filter['Time'],alpha[companies_dict[company_name]+'_close'],color="red")
        ax.set_xlabel("Time", fontsize = 14)
        ax.set_ylabel(company_name+" close", color="red",fontsize=14)

        ax2=ax.twinx()
        ax2.plot(twitter_filter['Time'], twitter_filter[companies_dict[company_name]+'_tweet_count'],color="blue")
        ax2.set_ylabel("Tweet count",color="blue",fontsize=14)
        plt.title('Liczba tweetów nt. firmy '+company_name+' a jej kursy zamknięcia w interwałach 5min')
        plt.show()
    else:
        ax.plot(alpha_filter['Time'],alpha[companies_dict[company_name]+'_open'],color="red")
        ax.set_xlabel("Time", fontsize = 14)
        ax.set_ylabel(company_name+" open", color="red",fontsize=14)

        ax2=ax.twinx()
        ax2.plot(twitter_filter['Time'], twitter_filter[companies_dict[company_name]+'_tweet_count'],color="blue")
        ax2.set_ylabel("Tweet count",color="blue",fontsize=14)
        plt.title('Liczba tweetów nt. firmy '+company_name+' a jej kursy otwarcia w interwałach 5min')
        plt.show()

def plot_tweets(include = ['Google','Microsoft','Tesla','Apple'],
               date_from = '2023-01-01',
               date_to = datetime.today(),
               trend = False):
    twitter_filter = filter_df(twitter,date_from,date_to).fillna(0)
    fig, ax = plt.subplots(figsize=(10,8))
    for inc in include:
        if trend:
            z = np.polyfit(mdates.date2num(twitter_filter.Time), twitter_filter[companies_dict[inc]+'_tweet_count'], 20)
            p = np.poly1d(z)
            plt.plot(twitter_filter.Time,p(mdates.date2num(twitter_filter.Time)),label=inc)
        else:
            plt.plot(twitter_filter.Time, twitter_filter[companies_dict[inc]+'_tweet_count'],label=inc)
    plt.xticks(rotation=45)
    plt.legend()
    plt.title('Liczba tweetów z hashtagiem danej firmy')
    plt.show()

def tweets_pie(date_from = '2023-01-01',
               date_to = datetime.today()):
    twitter_filter = filter_df(twitter,date_from,date_to)
    
    companies = ['Apple','Google','Tesla','Microsoft']
    
    data = []
    def func(pct, allvalues):
        absolute = int(pct / 100.*np.sum(allvalues))
        return "{:.1f}%\n({:d})".format(pct, absolute)
    for company in companies:
        data.append(twitter_filter[companies_dict[company]+'_tweet_count'].sum())
    
    fig = plt.figure(figsize =(10, 7))
    plt.pie(data, labels = companies,autopct = lambda pct: func(pct, data))
    plt.title('Porównanie liczby tweetów z hashtagiem danej firmy w zadanym okresie')
    plt.show()