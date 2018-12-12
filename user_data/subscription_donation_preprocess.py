"""The pipeline to preprocess subscription and donation data.

Aggregate on email level to calculate the frequency, recency and monetery of
donations and subscriptions
"""

import pandas as pd
import datetime

# to show the whole content in the columns
pd.set_option('display.max_colwidth', -1)

subsciption_filename = '/home/centos/mojo/data/subscriptions.xlsx'
donation_filename = '/home/centos/mojo/data/donations_combined.xlsx'


def calculate_recency(dates):
    """Calculate the recency."""
    return (datetime.date.today() - dates).astype('timedelta64[D]').astype(int).min()

def calculate_trans_range(dates):
    """Calculate the date range between the first and last transaction."""
    return (dates.max() - dates.min()).days


def aggregate_sub_don(df,
                      groupby_col='EMAIL',
                      aggs_methods={'ORD ENTR DT': ['count',
                                                    calculate_recency,
                                                    calculate_trans_range],
                                    'ORD REMT': ['sum']},
                      new_colnames={'ORD ENTR DT_count': 'subs_freq',
                                    'ORD ENTR DT_calculate_recency': 'subs_recency',
                                    'ORD ENTR DT_calculate_trans_range': 'subs_range',
                                    'ORD REMT_sum': 'subs_total',
                                    'Email': 'email'}):
    """
    Aggregate subscriptions/donations.

    The process works on email level to
    calculate the frequency, recency and monetery

    """
    new_df = df.groupby(groupby_col).agg(aggs_methods)
    new_df.columns = ["_".join(x) for x in new_df.columns.ravel()]
    new_df = new_df.reset_index()

    new_df.rename(columns=new_colnames, inplace=True)
    return new_df

def reshape_data_to_wide( df, 
                          row, 
                          col, 
                          element,
                          cal,
                         ):
    temp = df.groupby([row,col]).agg({element: pd.Series.nunique})
    temp = temp.reset_index()
    temp_wide = temp.pivot_table(values=element, 
                   index= row,
                   columns=col,
                   aggfunc = cal,
                   fill_value = 0,
                   margins = True)
    
    return temp_wide

def combine_dat_sets(dat1,
                     dat2,
                     by_dat1,
                     by_dat2,
                     join_method = 'outer'):
    """Outer join two data sets."""
    combined = dat1.merge(dat2, left_on=by_dat1, right_on=by_dat2, how=join_method)
    combined.fillna(0, inplace=True)
    return combined


def sub_don_process(subsciption_filename,
                    donation_filename):
    """Main function to preprocess subscription and donation."""
    subscription = pd.read_excel(subsciption_filename)
    donation = pd.read_excel(donation_filename)

    # aggregate subscription data
    subs = aggregate_sub_don(subscription,
                             groupby_col='EMAIL',
                             aggs_methods={'ORD ENTR DT': ['count',
                                                           calculate_recency,
                                                           calculate_trans_range],
                                           'ORD REMT': ['sum']},
                             new_colnames={'ORD ENTR DT_count': 'subs_freq',
                                           'ORD ENTR DT_calculate_recency': 'subs_recency',
                                           'ORD ENTR DT_calculate_trans_range': 'subs_range',
                                           'ORD REMT_sum': 'subs_total',
                                           'EMAIL': 'Email'})
    # aggregate donation data
    don = aggregate_sub_don(donation,
                            groupby_col='EMAIL',
                            aggs_methods={'ORD ENTR DT': ['count',
                                                          calculate_recency,
                                                          calculate_trans_range],
                                          'ORD REMT': ['sum']},
                            new_colnames={'ORD ENTR DT_count': 'don_freq',
                                          'ORD ENTR DT_calculate_recency': 'don_recency',
                                          'ORD ENTR DT_calculate_trans_range': 'don_range',
                                          'ORD REMT_sum': 'don_total',
                                          'EMAIL': 'Email'})
    # combine subscribers and donars
    combined = combine_dat_sets(subs, don, 'Email', 'Email')
    
    # convert ORD PUB CODE to wide forward since some emails have two pub codes
    # combine donation and subscription pub code
    don_uniques = donation[['EMAIL', 'ORD-PUB-CODE']].drop_duplicates()
    don_uniques['VALUE'] = 1
    don_pub = don_uniques.pivot(index= 'EMAIL',columns = 'ORD-PUB-CODE', values = 'VALUE')
    don_pub = don_pub.reset_index()
    
    sub_uniques = subscription[['EMAIL', 'ORD-PUB-CODE']].drop_duplicates()
    sub_uniques['VALUE'] = 1
    sub_pub = sub_uniques.pivot(index= 'EMAIL',columns = 'ORD-PUB-CODE', values = 'VALUE')
    sub_pub = sub_pub.reset_index()
    
    don_sub_pub_combo = combine_dat_sets(sub_pub, don_pub, 'EMAIL', 'EMAIL')
    don_sub_pub_combo.rename(columns = {'EMAIL': 'Email'}, inplace=True)
    
    final_combo = combine_dat_sets(combined, don_sub_pub_combo, 'Email', 'Email')
    
    return subscription, donation, final_combo

if __name__ == "__main__":
    # run aggregation and return raw subscription data, raw donation data
    # and combined processed data
    subscription, donation, combined = sub_don_process(subsciption_filename,
                                                       donation_filename)
