import os
import pandas as pd
import urllib.parse
from ua_parser import user_agent_parser

from MojoNewsletterClicksParser import MojoNewsletterClicksParser

# to display the whole column
pd.set_option('display.max_colwidth', -1)

# folder of the newsletter click csv files
datafolder = '/home/centos/mojo/data/'
datafiles = [os.path.join(datafolder, f) for f in os.listdir(datafolder)]

# read all the csv into one data frame
clean_df = []
for f in datafiles:
      dat = pd.read_csv(f)
      clean_df.append(dat)

all_dat = pd.concat(clean_df)

nl = MojoNewsletterClicksParser(url_df=all_dat)
full_url_df = nl.extend_url_df(nl.url_df)
nl.cleaned_mojo_standard=nl.mojo_standard_parser(full_url_df)
nl.cleaned_other=nl.others_parser(full_url_df)

cleaned_dat = pd.concat([nl.cleaned_mojo_standard,nl.cleaned_other])
cleaned_dat.to_csv('mojo_newsletter_cleaned_clicks.csv')
