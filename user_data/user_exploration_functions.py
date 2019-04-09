from collections import OrderedDict
import pandas as pd
import numpy as np

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

def basic_sum(df):
    # count total amount collected from subscription and donation
    # check the date range of the data
    
    sums = dict()
    sums['sumamt'] = sum(df['AMT PAID'])
    sums['sumamt_by_pub'] = df.groupby('ORD-PUB-CODE').agg({'AMT PAID':'sum'})
    sums['max_date'] = max(df['ORD ENTR DT'])
    sums['min_date'] = min(df['ORD ENTR DT'])   
   
    return sums


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



## register on plot.ly to get user name and api_key
# https://plot.ly/python/choropleth-maps/
#import plotly 
#plotly.tools.set_credentials_file(username='xxx', api_key='xxxx')
import plotly.plotly as py
import pandas as pd

def plot_statemap( map_title,
                   bar_title,
                   states,
                   values
                 ):
    scl = [[0.0, 'rgb(242,240,247)'],[0.2, 'rgb(218,218,235)'],[0.4, 'rgb(188,189,220)'],\
                [0.6, 'rgb(158,154,200)'],[0.8, 'rgb(117,107,177)'],[1.0, 'rgb(84,39,143)']]
    data = [ dict(
            type='choropleth',
            colorscale = scl,
            autocolorscale = False,
            locations = states,
            z = values.astype(float),
            locationmode = 'USA-states',
            #text =df['PCT'],
            marker = dict(
                line = dict (
                    color = 'rgb(255,255,255)',
                    width = 2
                ) ),
            colorbar = dict(
                title = bar_title)
            ) ]

    layout = dict(
            title = map_title,
            geo = dict(
                scope='usa',
                projection=dict( type='albers usa' ),
                showlakes = True,
                lakecolor = 'rgb(255, 255, 255)'),
                 )

    fig = dict( data=data, layout=layout )
    return fig

# example of plotting state map
# fig = plot_statemap( map_title = 'Donation Amount by State (2016-2018)',
#                     bar_title = 'Donation $',
#                     states = df['STATE'],
#                     values = df['AMT PAID']
#                    )
# py.iplot( fig, filename='d3-cloropleth-map' )

# add moving avgs to a data frame
def add_moving_avgs(dat,colname, N=5):

    mylist = dat[colname]
    cumsum, moving_aves = [0], []

    for i, x in enumerate(mylist, 1):
        cumsum.append(cumsum[i-1] + x)
        if i>=N:
            moving_ave = (cumsum[i] - cumsum[i-N])/N
            #can do stuff with moving_ave here
            moving_aves.append(moving_ave)
            
    
    temp = dat.copy()
    temp['moving_aves'] = [0]*int((N-1)/2) + moving_aves + [0]*int((N-1)/2)
    temp = temp[temp['moving_aves'] != 0]
    return(temp)


def cal_nclicks_by_modeling_topics(topic_filename, raw_click_data, prefix, topNtopics):
    # give a file with all the topics generated from NMF or LDA with their scores
    # choose top 3 topics for each post
    # count the number topics each user read
    
    # load topics and choose top 3 topics for each post
    features = pd.read_csv(topic_filename, encoding = "ISO-8859-1")
    features = features.add_prefix(prefix)
    topics = features.columns[1:]
    
    top_values = features.apply(lambda x: x[1:].sort_values(ascending=False).head(n=3), axis= 1).fillna(0).astype('bool').astype('int64')
    top_values['post_name'] = features[prefix + 'post_name']
    
        # count # clicks from each NMC topic by users
    raw_click_data_topics  = raw_click_data.merge(top_values, how = 'inner', left_on= 'title', right_on ='post_name').fillna(0)
    nclicks_raw_click_data_topics =raw_click_data_topics.groupby('Email')[topics].sum(axis=1)
    nclicks_raw_click_data_topics  = nclicks_raw_click_data_topics.reset_index()
    return nclicks_raw_click_data_topics


def choose_top_values(x, thresholds, topN = 3):

    y = x.sort_values(ascending=False).head(n=topN)
    top_topics = [i for i in y.index if y[i] > thresholds[i]]
    if len(top_topics) ==0:
        top_topics = y.index[0]

    z = x.copy()
    for i in z.index:
        if i in top_topics:
            z[i] = 1
        else:
            z[i] = 0
    return(z)

def cal_nclicks_by_modeling_topics_percentile(topic_filename, raw_click_data, prefix, topNtopics):
    # give a file with all the topics generated from NMF or LDA with their scores
    # choose top topics for each post: top topics that have scores higher than 95th percentile. up to topNtopics
    # count the number topics each user read
    
    # load topics and choose top 3 topics for each post
    features = pd.read_csv(topic_filename, encoding = "ISO-8859-1")
    features = features.add_prefix(prefix)
    topics = features.columns[1:]
    
    thresholds = features[topics].apply(lambda x: np.percentile(x, 95), axis = 0)
    top_values = features.apply(lambda x:choose_top_values(x[1:], thresholds = thresholds, topN = topNtopics),axis = 1)
    top_values['post_name'] = features[prefix + 'post_name']
    
        # count # clicks from each NMC topic by users
    raw_click_data_topics  = raw_click_data.merge(top_values, how = 'inner', left_on= 'title', right_on ='post_name').fillna(0)
    nclicks_raw_click_data_topics =raw_click_data_topics.groupby('Email')[topics].sum(axis=1)
    nclicks_raw_click_data_topics  = nclicks_raw_click_data_topics.reset_index()
    return nclicks_raw_click_data_topics
