"""class NewsletterParser contains functions to process the newsletter data.

It separates newsletter by source and parse urls and user agent data
"""
import urllib.parse
from ua_parser import user_agent_parser
import requests


class MojoNewsletterClicksParser:
    """Class to clean up newsletter clicks data."""

    def __init__(self, url_df):
        """Initiate the class with clicks data."""
        self.url_df = url_df

    def define_domain_types(self, domain):
        """Categorize domains."""
        if domain == '':
            return 'bad_record'
        # EXAMPLE: url ='Url'

        elif domain == 'www.motherjones.com':
            return 'mojo_standard'
        # EXAMPLE:
        # 'https://www.motherjones.com/politics/2018/10/dana-rohrabacher-julian-assange-russian-hack/?utm_source=mj-newsletters&utm_medium=email&utm_campaign=the-russian-connection-2018-10-18'

        elif domain == 'li.motherjones.com':
            return 'mojo_internal'
        # EXAMPLE:
        # http://li.motherjones.com/click?s=68884&sz=116x15&li=econundrums&e=44210695692a@gmail.com&p=73257121537547990

        elif domain == 'secure.motherjones.com':
            return 'mojo_secure'
        # EXAMPLE:
        # https://secure.motherjones.com/fnx/?action=SUBSCRIPTION&pub_code=MJM&term_pub=MJM&list_source=SEGMN

        elif domain == 'www.motherjones.com><img src=':
            return 'mojo_image'
        # EXAMPLE: "https://www.motherjones.com><img src="
        # "https://www.motherjones.com><img src="
        # "https://www.motherjones.com><img src="

        else:
            return 'others'
        # EXAMPLE:
        # https://www.washingtonpost.com/politics/putin-is-probably-involved-in-assassinations-and-poisonings-but-its-not-in-our-country-trump-says/2018/10/14/d745e21c-cff2-11e8-83d6-291fcead2ab1_story.html?utm_term=.3fa094502208

    def record_date_parser(self, record_time):
        """Parse click timestamp to get the click date"""
        try:
            return(pd.to_datetime(record_time).date())
        except ValueError:
            # for bad record where timestamp is 'Record on', return ''
            return('')

    def useragent_parser(self, ua_string):
        """Parse user agent string to extract OS and browser."""
        # .title() capitalize the first letter of each word
        parsed_ua = user_agent_parser.Parse(str(ua_string).title())
        OS = parsed_ua['os']['family']
        browser = parsed_ua['user_agent']['family']
        return OS, browser

    def record_date_and_extend(self, df, col_url='Recorded On'):
        """Add the click date to the data frame."""
        df['record_date'] = df[col_url].map(self.record_date_parser)
        print("""Add record date to the click data""")
        return df

    def url_parse_and_extend(self, df, col_url='Url'):
        """Add the parts of parsed url to the data frame."""
        df['protocol'], df['domain'], df['path'], df['query'], df['fragment'] = zip(
            *df[col_url].map(urllib.parse.urlsplit))
        print("""Add protocol, domain, path, query and fragment
                  to the click data""")
        return df

    def domain_type_extend(self, df, col_domain='domain'):
        """Add categorized domain type to the data frame."""
        df['domain_type'] = df[col_domain].map(self.define_domain_types)
        print("""Add domain_type
                  to the click data""")
        return df

    def ua_parse_and_extend(self, df, col_url='Browser'):
        """Add the parts of parsed url to the data frame."""
        df['os'], df['browser'] = zip(*df[col_url].map(self.useragent_parser))
        print("""Add os, browser to the click data""")
        return df

    def extend_url_df(
            self,
            df,
            by_date=True,
            by_url=True,
            by_domain_type=True,
            by_ua=True):
        """Extended data frame with url parts, OS, browser and domain type."""
        if by_date:
            df = self.record_date_and_extend(df)
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
            temp = df.loc[df[col_domain_type].isin(domain_type_wl)].copy()
        else:
            temp = df.copy()

        if domain_bl:
            subset = temp.loc[~temp[col_domain].isin(domain_bl)].copy()
        else:
            subset = temp.copy()

        print(
            """subset by domain type {wl}: selected {newcount} records out of {oldcount} records""".format(
                wl=','.join(domain_type_wl), oldcount=str(
                    df.shape[0]), newcount=str(
                    subset.shape[0])))
        return subset

    def path_parser(self, path, domain_type):
        """Parse path to get topic and title."""
        if domain_type == 'mojo_standard':
            if path in ['', '/']:
                topic = ''
                title = ''
            else:
                # mojo standard path examples:
                # 1. /environment/2018/09/whale-videos-triple-breach-wow/
                #2. /about/interact-engage/free-email-newsletter
                # use the first part as topic and last part as title

                path_parts = list(filter(None, path.split('/')))
                # remove empty strings
                topic = path_parts[0]
                title = path_parts[-1]
            return topic, title
        if domain_type == 'others':
            if path in ['', '/']:
                topic = ''
                title = ''
            else:
                path_parts = list(filter(None, path.split('/')))
                # the url part that contains '-' looks like titles
                lookalike_title = [p for p in path_parts if '-' in p]
                if lookalike_title:
                    title = lookalike_title[0]
                    topic = ''
                else:
                    topic = ''
                    title = ''
            return topic, title

    def query_parser(self, query, domain_type):
        """Parse query to get utms or maybe topics/title when it's redirect."""
        if domain_type == 'mojo_standard':

            if query == '':
                return '', '', ''

            else:
                mojo_utms = urllib.parse.parse_qs(query)
                if 'utm_campaign' in mojo_utms.keys():
                    utm_campaign = mojo_utms['utm_campaign'][0][:-11]
                else:
                    utm_campaign = ''

                if 'utm_medium' in mojo_utms.keys():
                    utm_medium = mojo_utms['utm_medium'][0]
                else:
                    utm_medium = ''

                if 'utm_source' in mojo_utms.keys():
                    utm_source = mojo_utms['utm_source'][0]
                else:
                    utm_source = ''
            return utm_campaign, utm_medium, utm_source

        if domain_type == 'others':

            if query == '':
                topic = ''
                title = ''
                utm_campaign = ''
                utm_medium = ''
                utm_source = ''
                return topic, title, utm_campaign, utm_medium, utm_source

            else:
                others_utms = urllib.parse.parse_qs(query)
                if 'u' in others_utms.keys():

                    url_parts = urllib.parse.urlsplit(others_utms['u'][0])

                    if url_parts.netloc == 'www.motherjones.com':
                        topic, title = self.path_parser(
                            url_parts.path,
                            domain_type='mojo_standard')
                        utm_campaign, utm_medium, utm_source = self.query_parser(
                            url_parts.query, domain_type='mojo_standard')
                    else:
                        topic = ''
                        title = ''
                        utm_campaign = ''
                        utm_medium = ''
                        utm_source = ''

                else:
                    topic = ''
                    title = ''
                    utm_campaign = ''
                    utm_medium = ''
                    utm_source = ''

                if 'url' in others_utms.keys():

                    url_parts = urllib.parse.urlsplit(others_utms['url'][0])

                    if url_parts.netloc == 'www.motherjones.com':

                        topic, title = self.path_parser(
                            url_parts.path,
                            domain_type='mojo_standard')

                        utm_campaign, utm_medium, utm_source = self.query_parser(
                            url_parts.query, domain_type='mojo_standard')
                    else:
                        topic = ''
                        title = ''
                        utm_campaign = ''
                        utm_medium = ''
                        utm_source = ''

                else:
                    topic = ''
                    title = ''
                    utm_campaign = ''
                    utm_medium = ''
                    utm_source = ''

                if 'utm_campaign' in others_utms.keys():
                    utm_campaign = others_utms['utm_campaign'][0][:-11]

                if 'utm_medium' in others_utms.keys():
                    utm_medium = others_utms['utm_medium'][0]

                if 'utm_source' in others_utms.keys():
                    utm_source = others_utms['utm_source'][0]

                return topic, title, utm_campaign, utm_medium, utm_source

    def mojo_standard_parser(
        self,
        df,
        selected_cols=[
            'Email',
            'record_date',
            'Url',
            'os',
            'browser',
            'domain',
            'domain_type',
            'topic',
            'title',
            'utm_campaign',
            'utm_medium',
            'utm_source']):
        """Parse mojo standard path and query to get key info."""
        mojo_standard = self.subset_urls_by_domain(
            df, domain_type_wl=['mojo_standard'])

        mojo_standard['topic'], mojo_standard['title'] = zip(
            *mojo_standard['path'].map(lambda x: self.path_parser(x, domain_type='mojo_standard')))
        print("""Add topic, title to the click data""")

        mojo_standard['utm_campaign'], mojo_standard['utm_medium'], mojo_standard['utm_source'] = zip(
            *mojo_standard['query'].map(lambda x: self.query_parser(x, domain_type='mojo_standard')))
        print("""Add utm_campaign, utm_medium, utm_source to the click data""")

        return mojo_standard[selected_cols]

    def others_parser(
        self,
        df,
        selected_cols=[
            'Email',
            'record_date',
            'Url',
            'os',
            'browser',
            'domain',
            'domain_type',
            'topic',
            'title',
            'utm_campaign',
            'utm_medium',
            'utm_source']):
        """Parse mojo standard path and query to get key info."""
        others = self.subset_urls_by_domain(df,
                                            domain_type_wl=['others'])

        # first try to get topic and title from query for the re-direct clicks' queries
        # try to get topic and title from the non-standard clicks' path
        # assume the path parts that contains '-' are titles
        path_info = others['path'].map(
            lambda x: nl.path_parser(
                x, domain_type='others'))
        query_info = others['query'].map(
            lambda x: nl.query_parser(
                x, domain_type='others'))

        paths_query_combo = []
        for x, y in zip(path_info + ('', '', ''), query_info):
            paths_query_combo.append(["{}{}".format(x_, y_)
                                      for x_, y_ in zip(x, y)])

        others['topic'], others['title'], others['utm_campaign'], others['utm_medium'], others['utm_source'] = zip(
            *paths_query_combo)

        print(
            """Add topic, title, utm_campaign, utm_medium, utm_source to the click data""")
        return others[selected_cols]

    def mojo_nonstandard_parser(
        self,
        df,
        domain_type=[
            'mojo_internal',
            'mojo_image',
            'mojo_secure'],
        selected_cols=[
            'Email',
            'record_date',
            'Url',
            'os',
            'browser',
            'domain',
            'domain_type']):
        """Parse mojo standard path and query to get key info."""
        mojo_nonstandard = self.subset_urls_by_domain(
            df, domain_type_wl=domain_type)

        return mojo_nonstandard[selected_cols]

    def get_url_content(self, link):
        """Get content from the link."""
        f = requests.get(link)
        return f.text
