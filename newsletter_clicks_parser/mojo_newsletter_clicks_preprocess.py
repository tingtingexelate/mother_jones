#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import urllib.parse
from ua_parser import user_agent_parser
from MojoNewsletterClicksParser import MojoNewsletterClicksParser

# to show the whole content in the columns
pd.set_option('display.max_colwidth', -1)

# define the folder that contains the newsletter csv files
datafolder = '/home/centos/mojo/data/'
datafiles = [os.path.join(datafolder, f) for f in os.listdir(datafolder)]

# read all the csv files into one data frame
clean_df = []
for f in datafiles:
    dat = pd.read_csv(f)
    clean_df.append(dat)

all_dat = pd.concat(clean_df)
# create an instance of the class MojoNewsletterClicksParser
nl = MojoNewsletterClicksParser(url_df=all_dat)

# Parse Url, Browser and Recorded On and add parsed info
# to the original data frame
full_url_df = nl.extend_url_df(nl.url_df)
full_url_df.to_pickle('full_url_df.pkl')
# full_url_df = pd.read_pickle('full_url_df.pkl')

# subset data frame to records from standard mojo links only
# further parse url parts to get topic, title, utm_campaign,
# utm_medium, utm_source
nl.cleaned_mojo_standard = nl.mojo_standard_parser(full_url_df)

# subset data frame to records from other sources like facebook, twitter, etc
# further parse url parts to get topic, title,
# utm_campaign, utm_medium, utm_source
nl.cleaned_other = nl.others_parser(full_url_df)

# subset data frame to records from non standard mojo links only;
# like li.motherjones, secure.motherjones, etc
nl.cleaned_mojo_nonstandard = nl.mojo_nonstandard_parser(full_url_df)

# combine processed data and save as a pickle
combined_clean_data = pd.concat([nl.cleaned_mojo_standard, nl.cleaned_other])
combined_clean_data.to_pickle('newsletter_processed_data.pkl')
