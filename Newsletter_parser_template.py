import os
import pandas as pd
#python3 -m pip install pandas
import urllib.parse
from ua_parser import user_agent_parser
import requests
from NewsletterParser import NewsletterParser

pd.set_option('display.max_colwidth', -1)

datafolder = '/home/centos/mojo/data/'

datafiles = [os.path.join(datafolder, f) for f in os.listdir(datafolder)]

clean_df = []
for f in datafiles:
      dat = pd.read_csv(f)

      nl = (url_df=dat)
      nl.full_url_df = nl.extend_url_df(nl.url_df)
      nl.cleaned_mojo_standard=nl.mojo_standard_parser(nl.full_url_df)
      nl.cleaned_other=nl.others_parser(full_url_df)

      clean_df.append(nl.cleaned_mojo_standard)
      clean_df.append(nl.cleaned_other)

pd.concat(clean_df).to_csv('clean_data.csv')
