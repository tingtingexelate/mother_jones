"""The pipeline to preprocess subscription and donation data.

Aggregate on email level to calculate the frequency, recency and monetery of
donations and subscriptions
"""

import pandas as pd
import datetime

# to show the whole content in the columns
pd.set_option('display.max_colwidth', -1)

def calculate_recency(dates):
    """Calculate the recency."""
    dates = pd.to_datetime(dates)
    return (datetime.date.today() - dates).astype('timedelta64[D]').astype(int).min()

def calculate_trans_range(dates):
    """Calculate the date range between the first and last transaction."""
    dates = pd.to_datetime(dates)
    return (dates.max() - dates.min()).days


def aggregate_sub_don(df,
                      groupby_col,
                      aggs_methods,
                      new_colnames):
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


def sub_don_process(subscription_filenames,
                    donation_filenames,
                    column_names = {'email':'EMAIL',
                                    'amount':'ORD REMT',
                                    'date': 'ORD ENTR DT',
                                    'pubcode':'ORD-PUB-CODE'}                  
                   
                   ):
    """Main function to preprocess subscription and donation."""

    if isinstance(subscription_filenames, list):
        # if inputs are list of file names, read the files as data frame
        subscription_df = []
        for f in subscription_filenames:
            if '.csv' in f:
                dat = pd.read_csv(f,encoding = "ISO-8859-1")
            if '.xlsx' in f:
                dat = pd.read_excel(f)
            subscription_df.append(dat)
        subscription = pd.concat(subscription_df)

        donation_df = []
        for f in donation_filenames:
            if '.csv' in f:
                dat = pd.read_csv(f,encoding = "ISO-8859-1")
            if '.xlsx' in f:
                dat = pd.read_excel(f)
            donation_df.append(dat)
        donation = pd.concat(donation_df)

    if isinstance(subscription_filenames, pd.DataFrame):       
        # if inputs are data frame, use them as they are
        subscription = subscription_filenames
        donation = donation_filenames
        
    # aggregate subscription data
    subs = aggregate_sub_don(subscription,
                             groupby_col=column_names['email'],
                             aggs_methods={column_names['date']: ['count',
                                                           calculate_recency,
                                                           calculate_trans_range],
                                           column_names['amount']: ['sum']},
                             new_colnames={column_names['date']+'_count': 'subs_freq',
                                           column_names['date']+'_calculate_recency': 'subs_recency',
                                           column_names['date']+'_calculate_trans_range': 'subs_range',
                                           column_names['amount'] + '_sum': 'subs_total',
                                           column_names['email']: 'Email'})
    # aggregate donation data
    don = aggregate_sub_don(donation,
                            groupby_col=column_names['email'],
                            aggs_methods={column_names['date']: ['count',
                                                          calculate_recency,
                                                          calculate_trans_range],
                                          column_names['amount']: ['sum']},
                            new_colnames={column_names['date']+'_count': 'don_freq',
                                          column_names['date']+'_calculate_recency': 'don_recency',
                                          column_names['date']+'_calculate_trans_range': 'don_range',
                                          column_names['amount']+'_sum': 'don_total',
                                          column_names['email']: 'Email'})
    # combine subscribers and donars
    combined = combine_dat_sets(subs, don, 'Email', 'Email')
    
    # convert ORD PUB CODE to wide forward since some emails have two pub codes
    # combine donation and subscription pub code
          
    don_uniques = donation[[column_names['email'], column_names['pubcode']]].drop_duplicates()
    don_uniques['VALUE'] = 1
    don_pub = don_uniques.pivot(index= column_names['email'],columns = column_names['pubcode'], values = 'VALUE')
    don_pub = don_pub.reset_index()
    
    sub_uniques = subscription[[column_names['email'], column_names['pubcode']]].drop_duplicates()
    sub_uniques['VALUE'] = 1
    sub_pub = sub_uniques.pivot(index= column_names['email'],columns = column_names['pubcode'], values = 'VALUE')
    sub_pub = sub_pub.reset_index()
    
    don_sub_pub_combo = combine_dat_sets(sub_pub, don_pub, column_names['email'], column_names['email'])
    don_sub_pub_combo.rename(columns = {column_names['email']: 'Email'}, inplace=True)
    
    final_combo = combine_dat_sets(combined, don_sub_pub_combo, 'Email', 'Email')
    final_combo['process_date'] = datetime.date.today()
    return subscription, donation, final_combo

if __name__ == "__main__":
    # run aggregation and return raw subscription data, raw donation data
    # and combined processed data
    subscription, donation, combined = sub_don_process(subsciption_filenames,
                                                       donation_filenames)
