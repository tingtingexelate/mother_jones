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


def aggregate_sub_don(df,
                      groupby_col='EMAIL',
                      aggs_methods={'ORD ENTR DT': ['count',
                                                    calculate_recency],
                                    'ORD REMT': ['sum']},
                      new_colnames={'ORD ENTR DT_count': 'subs_freq',
                                    'ORD ENTR DT_calculate_recency': 'subs_recency',
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


def combine_sub_don(sub,
                    don,
                    by_sub='email',
                    by_don='email'):
    """Combine subscription and donation."""
    combined = sub.merge(don, left_on=by_sub, right_on=by_don, how='outer')
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
                                                           calculate_recency],
                                           'ORD REMT': ['sum']},
                             new_colnames={'ORD ENTR DT_count': 'subs_freq',
                                           'ORD ENTR DT_calculate_recency': 'subs_recency',
                                           'ORD REMT_sum': 'subs_total',
                                           'EMAIL': 'Email'})

    # aggregate donation data
    don = aggregate_sub_don(donation,
                            groupby_col='EMAIL',
                            aggs_methods={'ORD ENTR DT': ['count',
                                                          calculate_recency],
                                          'ORD REMT': ['sum']},
                            new_colnames={'ORD ENTR DT_count': 'don_freq',
                                          'ORD ENTR DT_calculate_recency': 'don_recency',
                                          'ORD REMT_sum': 'don_total',
                                          'EMAIL': 'Email'})

    # combine subscribers and donars
    combined = combine_sub_don(subs, don, by_sub='Email', by_don='Email')

    return subscription, donation, combined


if __name__ == "__main__":
    # run aggregation and return raw subscription data, raw donation data
    # and combined processed data
    subscription, donation, combined = sub_don_process(subsciption_filename,
                                                       donation_filename)
