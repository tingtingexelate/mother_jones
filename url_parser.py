import os
import pandas as pd
#python3 -m pip install pandas
from urllib.parse import urlparse

pd.set_option('display.max_colwidth', -1)

datafolder = '/home/centos/mojo/data/'

datafiles= [os.path.join(datafolder,f) for f in os.listdir(datafolder)]

dat = pd.read_csv(datafiles[0])

