from collections import OrderedDict
import pandas as pd

def basic_counts(df):
    # count total number of unique emails and unique emails by pub code
    # check if there are overlap between pub codes
    # check the date range of the data
    
    counts = dict()
    counts['nemails'] = len(df['EMAIL'].unique())
    counts['nemails_by_pub'] = df.groupby('ORD-PUB-CODE').agg({'EMAIL':'nunique'})
    counts['max_date'] = max(df['ORD ENTR DT'])
    counts['min_date'] = min(df['ORD ENTR DT'])   
    
    if counts['nemails_by_pub'].shape[0] == 2:
        counts['noverlap'] = sum(counts['nemails_by_pub']['EMAIL']) - counts['nemails']
    else:
        counts['noverlap'] = 0
    
    return counts


def define_donor_groups(df):    
    
    """Take donation data and dive donors into different groups and save data in an ordered dictionary
    
    example 'df' is:
    sub_don_click_combined_filename = '/home/centos/mojo/subscription_donation_click_combined_data.pkl'
    df = pd.read_pickle(sub_don_click_combined_filename)
    """
    
    
    highvalue_donors = df.loc[df['don_total'] >= 500]
    regular_donors = df.loc[(df['don_total'] > 0) & (df['don_total'] < 500)]
    non_donors = df.loc[df['don_total'] == 0]
    all_donors = df.loc[df['don_total'] > 0]

    donor_groups=OrderedDict()
    donor_groups['regular donors (< $500)']=regular_donors
    donor_groups['highvalue donors (>= $500)']=highvalue_donors
    donor_groups['SDN donors']=highvalue_donors.loc[highvalue_donors['SDN']==1]
    DON_highdonors=highvalue_donors.loc[highvalue_donors['SDN']==0]
    donor_groups['one-time big time donor']=DON_highdonors[DON_highdonors['don_freq']==1]
    donor_groups['multi-time non SDN donors']=DON_highdonors[DON_highdonors['don_freq'] > 1]
    donor_groups['non donors']=non_donors
    donor_groups['all donors']=all_donors
    return donor_groups

def check_subscription_rate(df, groupname):
    
    mag_sub_rate = round((df['subs_freq']>0).mean()*100,2)
    news_sub_rate = round((df['Url']>0).mean()*100,2)
    
    #print('{}% {} are also subscribers '.format(mag_sub_rate, groupname))
    #print('{}% {} clicked oct newsletter '.format(news_sub_rate,groupname))
    
    return (groupname, mag_sub_rate, news_sub_rate)


def check_click_distribution(df, groupname):
    dist = round(df[df['Url'] > 0]['Url'].describe(),2)
    dist = dist.rename(groupname)
    return dist

def check_topics_distribution(df, groupname):
    dist = round(df[df['topic'] > 0]['topic'].describe(),2)
    dist = dist.rename(groupname)
    return dist

def sub_df_colsum(df,cols,name):
    """Function to calculate column sum for selected columns"""
    colsum = df[cols].sum(axis = 0)
    colsum = colsum.rename(name)
    return colsum


def find_most_popular_titles(df, emails, topN = 10):
    """an example of df is click_data from '/home/centos/mojo/newsletter_processed_data.pkl'"""
    temp_data = df.loc[df['Email'].isin(emails)]
    temp_data = temp_data[temp_data['title']!='']


    hoturls = temp_data.groupby('title').agg({'Email': pd.Series.nunique})
    hoturls = hoturls.reset_index()
    hoturls = hoturls.rename(columns = {'Email':'email_count'})
    hoturls['email_pct'] = hoturls['email_count']/len(temp_data['Email'].unique())
    topN_urls = hoturls.sort_values(by='email_count', ascending=False).head(topN)
    
    return topN_urls