import pandas as pd
from MojoNewsletterClicksParser import MojoNewsletterClicksParser

# to show the whole content in the columns
pd.set_option('display.max_colwidth', -1)

# define the folder that contains the newsletter csv files
datafolder = '/home/centos/mojo/data/click_data_v2/'
datafiles = [os.path.join(datafolder, f) for f in os.listdir(datafolder)]

# read all the csv files into one data frame
clean_df = []
for f in datafiles:
    dat = pd.read_csv(f)
    clean_df.append(dat)

all_dat = pd.concat(clean_df)

# change column names to match the names used in the module MojoNewsletterClicksParser
all_dat.rename(columns={'url': 'Url',
                        'email hashed': 'Email'},
               inplace=True)


# create an instance of the class MojoNewsletterClicksParser
nl = MojoNewsletterClicksParser(url_df=all_dat)

# Parse Url and add parsed info to the original data frame
# set by_date and by_ua to be False
# since the data don't contain date and ua info
full_url_df = nl.extend_url_df(nl.url_df,
                               by_date=False,
                               by_url=True,
                               by_domain_type=True,
                               by_ua=False)
full_url_df.to_pickle('full_url_df_v2.pkl')

# subset data frame to records from standard mojo links only
# further parse url parts to get topic, title
nl.cleaned_mojo_standard = nl.mojo_standard_parser(full_url_df,
                                                   selected_cols=[
                                                        'Email',
                                                        'Url',
                                                        'domain',
                                                        'domain_type',
                                                        'topic',
                                                        'title'])
# subset data frame to records from other sources like facebook, twitter, etc
# further parse url parts to get topic, title
nl.cleaned_other = nl.others_parser(full_url_df,
                                    selected_cols=['Email',
                                                   'Url',
                                                   'domain',
                                                   'domain_type',
                                                   'topic',
                                                   'title'])

# subset data frame to records from non standard mojo links only;
# like li.motherjones, secure.motherjones, etc
nl.cleaned_mojo_nonstandard = nl.mojo_nonstandard_parser(full_url_df,
                                                         selected_cols=[
                                                                'Email',
                                                                'domain',
                                                                'domain_type'])
combined_clean_data = pd.concat([nl.cleaned_mojo_standard, nl.cleaned_other])
combined_clean_data.to_pickle('newsletter_processed_data_v2.pkl')

combined_clean_data.head()
