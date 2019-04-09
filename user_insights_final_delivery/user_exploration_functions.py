""" Collection of functions used in user_exploration_grouped_insights_1.ipynb """

from collections import OrderedDict
import pandas as pd
import numpy as np

##################################################
# click by topic functions used in section 2.2 ###
##################################################
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

def cal_nclicks_by_modeling_topics(topic_filename, raw_click_data, prefix, topNtopics):
    # give a file with all the topics generated from NMF or LDA with their scores
    # choose top 3 topics for each post
    # count the number topics each user read
    
    # load topics and choose top 3 topics for each post
    features = pd.read_csv(topic_filename, encoding = "ISO-8859-1")
    features = features.add_prefix(prefix)
    topics = features.columns[1:]
    
    top_values = features.apply(lambda x: x[1:].sort_values(ascending=False).head(n=topNtopics), axis= 1).fillna(0).astype('bool').astype('int64')
    top_values['post_name'] = features[prefix + 'post_name']
    
        # count # clicks from each NMC topic by users
    raw_click_data_topics  = raw_click_data.merge(top_values, how = 'inner', left_on= 'title', right_on ='post_name').fillna(0)
    nclicks_raw_click_data_topics =raw_click_data_topics.groupby('Email')[topics].sum(axis=1)
    nclicks_raw_click_data_topics  = nclicks_raw_click_data_topics.reset_index()
    return nclicks_raw_click_data_topics

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

###########################################
# summary functions used in section 3.1 ###
###########################################

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



###########################################
# Map functions used in section 3.2 ###
###########################################
from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
print (__version__) # requires version >= 1.9.0

#Always run this the command before at the start of notebook
init_notebook_mode(connected=True)
import plotly.graph_objs as go

def plot_statemap( map_title,
                   bar_title,
                   states,
                   values
                 ):
    # create US heatmap based on provided values
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
# iplot( fig, filename='d3-cloropleth-map' )

#############################################################
# Donation distribution functions used in section 3.6 #######
#############################################################
## function to calculate cumulative donation and donors
def don_email_cumpct(dat):
    # dat must contain column 'don_total' for donation amount 
    # and column 'Email'
    dat_cnts = dat.groupby('don_total').agg({'Email': 'count'})
    dat_cnts = dat_cnts.reset_index()
    dat_cnts['Email_cumpct'] = 100*(dat_cnts['Email']/dat_cnts['Email'].sum()).cumsum()
    dat_cnts['don_total_cum'] = dat_cnts['don_total']*dat_cnts['Email']
    dat_cnts['don_total_cumpct'] = 100*(dat_cnts['don_total_cum'].cumsum())/(dat_cnts['don_total_cum'].sum())
    return dat_cnts


######################################################
# moving average functions used in section 3.9 #######
######################################################
def add_moving_avgs(dat,colname, N=5):
    # calculate a succession of averages derived from successive segments
    # the default number segments is 5
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


######################################################
# solid line plot function used in section 3.9 #######
######################################################
def make_solid_plot(x, y, df,
                    xlab, ylab, title, legends):
    
    # the function to make solid time series 
    # then add dashed 5 month average moving trend line
    import matplotlib.pyplot as plt
    from textwrap import wrap

    plt.rcParams['figure.figsize'] = [18,5]

    plt.plot(x, y, data=df, marker='', markerfacecolor='green', 
             markersize=12, color='green', linewidth=4, linestyle = 'solid')

    # calculate trend with 5 month moving avgs
    df_moving_avgs = add_moving_avgs(dat = df, colname = y, N=5)
    
    plt.plot(x, 'moving_aves', data = df_moving_avgs,  marker='', color='orange', linewidth= 3, linestyle = 'dashed')
    plt.title("\n".join(wrap(title,50)),fontweight='bold', color = 'purple', fontsize = 'large')
    plt.xlabel(xlab,fontweight='bold', color = 'darkblue', fontsize = 'large')
    plt.ylabel(ylab,fontweight='bold', color = 'darkblue', fontsize = 'large')


    plt.axvline(x=datetime.datetime(2016,12,1), linestyle = '-',color='grey', linewidth=2)
    plt.axvline(x=datetime.datetime(2017,12,1), linestyle = '-',color='grey', linewidth=2)
    plt.axvline(x=datetime.datetime(2018,12,1), linestyle = '-',color='grey', linewidth=2)
    plt.legend(labels = legends)
    
######################################################
# scatter line plot function used in section 3.10 #####
######################################################
def make_click_data_scatter_plot(x_colname, y_colname, df,
                                 xlab, ylab, title, legends):
    
    # the function to make solid time series 
    # then add dashed 5 month average moving trend line
    import matplotlib.pyplot as plt
    from textwrap import wrap
    
    plt.rcParams['figure.figsize'] = [18,5]

    plt.plot_date( x= df[x_colname], 
                   y =df[y_colname],
                   color='green')

    # calculate trend with 5 month moving avgs
    df_moving_avgs= add_moving_avgs(dat = df, colname = y_colname,N=5)
    plt.plot( x_colname, 'moving_aves', data = df_moving_avgs,  marker='', color='orange', linewidth= 3, linestyle = 'solid')


    plt.title("\n".join(wrap(title,50)),fontweight='bold', color = 'purple', fontsize = 'large')
    plt.xlabel(xlab,fontweight='bold', color = 'darkblue', fontsize = 'large')
    plt.ylabel(ylab,fontweight='bold', color = 'darkblue', fontsize = 'large')

    plt.axvline(x=datetime.datetime(2017,12,1), linestyle = '-',color='grey', linewidth=2)
    plt.axvline(x=datetime.datetime(2018,12,1), linestyle = '-',color='grey', linewidth=2)
    plt.legend(labels = legends)



######################################################
# month edit functions used in section 3.11 #####
######################################################
import datetime
import calendar

def add_months(sourcedate,months):
    # add months to a given date
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day,calendar.monthrange(year,month)[1])
    return datetime.date(year,month,day)

def diff_month(d1, d2):
    # calculate the number of months between two dates
    return (d1.year - d2.year) * 12 + d1.month - d2.month

######################################################
# box plot function used in section 4.1 and more #####
######################################################

def plot_group_boxplot(x_colname, y_colname, 
                       df,
                       xlab, ylab,
                       xticklab, yticklab,
                       title,
                       orient="v"):
    import seaborn as sns
    import pandas as pd
    import numpy as np
    import matplotlib.pylab as plt
    import datetime as dt
    sns.set()
    
    g = sns.catplot(x=x_colname, y=y_colname,
                    data=df, palette="muted",
                    showfliers=False,
                    kind='box',height=6, aspect=1.5, orient = orient)
    g.set_ylabels(ylab,fontsize='x-large',fontweight = 'bold')
    g.set_xlabels(xlab, fontsize='x-large',fontweight = 'bold')
    
    if orient == 'v':
        if max([len(s) for s in xticklab]) > 30:
            rotation_degree = 20
        else:
            rotation_degree = 0
    else:
        rotation_degree = 0
        
    if len(xticklab) != 0:
        # use default values if tick labels are not given
        g.set_xticklabels(xticklab, 
                       rotation=rotation_degree,fontweight = 'bold',fontsize = 'large', color = '#34495e')
    else:
        g.set_xticklabels(rotation=rotation_degree,fontweight = 'bold',fontsize = 'large', color = '#34495e')
        
    if len(yticklab) != 0:
        g.set_yticklabels(yticklab, 
                           rotation=0,fontweight = 'bold',fontsize = 'large', color = '#34495e')
    else:
        # use default values if tick labels are not given
        g.set_yticklabels(rotation=0,fontweight = 'bold',fontsize = 'large', color = '#34495e')
    plt.title(title, fontsize = 'x-large',fontweight = 'bold')

    if orient == 'v':
        print("Median values:")
        print(df.groupby([x_colname])[y_colname].median())
    else:
        print("Median values:")
        print(df.groupby([y_colname])[x_colname].median())
        
###########################################################
# topic heatmap function used in section 4.1 and more #####
###########################################################

def plot_topic_heatmap(df, xlab, ylab, xticklab, yticklab, title, fig_width = 14, fig_height = 8):
    import matplotlib.pylab as plt
    import seaborn as sns; sns.set()
    sns.set(rc={'figure.figsize':(fig_width,fig_height)})
    g = sns.heatmap(df, 
                 annot=True,
                 annot_kws = {'size':20},
                 cmap="PiYG",
                 center = 0,
                 linewidths=.5)
    g.set_ylabel(ylab,fontsize = 'x-large',fontweight = 'bold')
    g.set_xlabel(xlab, fontsize = 'x-large',fontweight = 'bold')
    
    if max([len(s) for s in xticklab]) > 25 :
        rotation_degree = 20
    else:
        rotation_degree = 0
    
    g.set_xticklabels(xticklab, 
                       rotation=rotation_degree,fontweight = 'bold',fontsize = 'large', color = '#34495e')
    g.set_yticklabels(yticklab, 
                       rotation=0,fontweight = 'bold',fontsize = 'large', color = '#34495e')
    plt.title(title, fontsize = 'x-large',fontweight = 'bold')

################################################################
# function to find top clicked posts in section 4.1 and more #####
################################################################
def find_most_popular_titles(df, emails, topN = 10):
    """an example of df is click_data from 'newsletter_processed_data.pkl'"""
    temp_data = df.loc[df['Email'].isin(emails)]
    temp_data = temp_data[temp_data['title']!='']


    hoturls = temp_data.groupby('title').agg({'Email': pd.Series.nunique})
    hoturls = hoturls.reset_index()
    hoturls = hoturls.rename(columns = {'Email':'email_count'})
    hoturls['email_pct'] = hoturls['email_count']/len(temp_data['Email'].unique())
    topN_urls = hoturls.sort_values(by='email_count', ascending=False).head(topN)
    
    return topN_urls    
   
###################################################################
# function to define interested groups in section 4.3 and more #####
################################################################ ###   
def define_interested_groups(df):    
    
    """Take donation data and dive donors into different groups and save data in an ordered dictionary
    
    example 'df' is:
    sub_don_click_combined_filename = 'subscription_donation_click_combined_data.pkl'
    df = pd.read_pickle(sub_don_click_combined_filename)
    """
    
    
    highvalue_donors = df.loc[df['don_total'] >= 500]
    regular_donors = df.loc[(df['don_total'] > 0) & (df['don_total'] < 500)]
    non_donors = df.loc[df['don_total'] == 0]
    
    sub_print_only = df.loc[(df['subs_freq'] >0) & (df['Url'] == 0)]
    sub_online_only = df.loc[(df['subs_freq'] == 0) & (df['Url'] > 0)]
    sub_both = df.loc[(df['subs_freq'] >0) & (df['Url']>0)]
    sub_all_online = df.loc[(df['Url']>0)]
    
    interested_groups=OrderedDict()
    interested_groups['regular donors (< $500)']=regular_donors
    interested_groups['highvalue donors (>= $500)']=highvalue_donors
    interested_groups['non donors']=non_donors
    interested_groups['print subscriber only'] = sub_print_only
    interested_groups['online reader only'] = sub_online_only 
    interested_groups['both print subscriber and online reader'] = sub_both
    interested_groups['all online readers'] = sub_all_online
    return interested_groups

################################################################
# Various util functions to used in section 4 ##################
################################################################ 

def check_subscription_rate(df, groupname):
    
    mag_sub_rate = round((df['subs_freq']>0).mean()*100,2)
    news_sub_rate = round((df['Url']>0).mean()*100,2)
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




