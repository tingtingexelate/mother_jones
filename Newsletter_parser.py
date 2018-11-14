"""class NewsletterParser contains functions to process the newsletter data.

It separates newsletter by source and parse urls and user agent data
"""
import urllib.parse
from ua_parser import user_agent_parser
import requests


class NewsletterParser:
    """Class to clean up newsletter data."""

    def __init__(self, url_df):
        """Initiate the class."""
        self.url_df = url_df

    def useragent_parser(self, ua_string):
        """Parse user agent string to extract OS and browser."""
        # .title() capitalize the first letter of each word
        parsed_ua = user_agent_parser.Parse(str(ua_string).title())
        OS = parsed_ua['os']['family']
        browser = parsed_ua['user_agent']['family']
        return OS, browser

    def define_domain_types(self, domain):
        """Categorize domains."""
        if domain == '':
            return 'bad_record'
        # EXAMPLE: url ='Url'

        elif domain == 'www.motherjones.com':
            return 'mojo_standard'
        # EXAMPLE: 'https://www.motherjones.com/politics/2018/10/dana-rohrabacher-julian-assange-russian-hack/?utm_source=mj-newsletters&utm_medium=email&utm_campaign=the-russian-connection-2018-10-18'

        elif domain == 'li.motherjones.com':
            return 'mojo_internal'
        #EXAMPLE: http://li.motherjones.com/click?s=68884&sz=116x15&li=econundrums&e=44210695692a@gmail.com&p=73257121537547990

        elif domain == 'secure.motherjones.com':
            return 'mojo_secure'
        #EXAMPLE: https://secure.motherjones.com/fnx/?action=SUBSCRIPTION&pub_code=MJM&term_pub=MJM&list_source=SEGMN

        elif domain == 'www.motherjones.com><img src=':
            return 'mojo_image'
        #EXAMPLE: "https://www.motherjones.com><img src=" "https://www.motherjones.com><img src=" "https://www.motherjones.com><img src="

        else:
            return 'others'
        #EXAMPLE: https://www.washingtonpost.com/politics/putin-is-probably-involved-in-assassinations-and-poisonings-but-its-not-in-our-country-trump-says/2018/10/14/d745e21c-cff2-11e8-83d6-291fcead2ab1_story.html?utm_term=.3fa094502208

    def url_parse_and_extend(self, df, col_url='Url'):
        """Add the parts of parsed url to the data frame."""
        df['protocol'], df['domain'], df['path'], df['query'], df['fragment'] = zip(*df[col_url].map(urllib.parse.urlsplit))
        return df

    def domain_type_extend(self, df, col_domain='domain'):
        """Add categorized domain type to the data frame."""
        df['domain_type'] = df[col_domain].map(self.define_domain_types)
        return df

    def ua_parse_and_extend(self, df, col_url='Browser'):
        """Add the parts of parsed url to the data frame."""
        df['os'], df['browser'] = zip(*df[col_url].map(self.useragent_parser))
        return df

    def extend_url_df(self, df, by_url=True, by_domain_type=True, by_ua=True):
        """Extended data frame with url parts, OS, browser and domain type."""
        if by_ua:
            df = self.ua_parse_and_extend(df)
        if by_url:
            df = self.url_parse_and_extend(df)
        if by_domain_type:
            df = self.domain_type_extend(df)
        return df

    def subset_urls_by_domain(self, df,
                              domain_type_wl=[],
                              domain_bl=[],
                              col_domain_type='domain_type',
                              col_domain='domain'):
        """Subset domain by types.

        Take a data frame that has domain in the given column
        Subset the data frame based on given domain type whitelist
        and domain blacklist
        """
        if domain_type_wl:
            subset_temp = df.loc[df[col_domain_type].isin(domain_type_wl)]
        else:
            subset_temp = df

        if domain_bl:
            subset = subset_temp.loc[~subset_temp[col_domain].isin(domain_bl)]
        else:
            subset = subset_temp

        print("selected {newcount} records out of {oldcount} records".format(
                                            oldcount=str(df.shape[0]),
                                            newcount=str(subset.shape[0])))
        return subset

    def path_parser(self, path, domain_type):
        """Parse path to get topic and title."""
        if domain_type == 'mojo_standard':
            if path == '':
                topic = ''
                title = ''
            else:
                path_parts = list(filter(None, path.split('/')))
                # remove empty strings

                topic = path_parts[0]
                title = path_parts[3]
            return topic, title

    def query_parser(self, query, domain_type):
        """Parse query to get utm."""
        if domain_type == 'mojo_standard':

            if query == '':
                return '', '', ''

            else:
                mojo_utms = urllib.parse.parse_qs(query)
                utm_campaign = mojo_utms['utm_campaign'][0].split('-')[0]
                utm_medium = mojo_utms['utm_medium'][0]
                utm_source = mojo_utms['utm_source'][0]
            return utm_campaign, utm_medium, utm_source

    def mojo_standard_parser(self, df,
                             selected_cols=['Email', 'Url', 'os', 'browser',
                                            'domain', 'domain_type', 'topic',
                                            'title', 'utm_campaign',
                                            'utm_medium', 'utm_source']):
        """Parse mojo standard path and query to get key info."""
        mojo_standard = self.subset_urls_by_domain(df,
                                                   domain_type_wl=['mojo_standard'])
        mojo_standard['topic'], mojo_standard['title'] = zip(*mojo_standard['path'].map(lambda x: self.path_parser(x,domain_type='mojo_standard')))
        mojo_standard['utm_campaign'], mojo_standard['utm_medium'], mojo_standard['utm_source'] = zip(*mojo_standard['query'].map(lambda x: self.query_parser(x, domain_type='mojo_standard')))

        return mojo_standard[selected_cols]

    def get_url_content(self, link):
        """Get content from the link."""
        f = requests.get(link)
        return f.text
