import os
import logging
import sys

import requests
import twitter

LOGGER = logging.getLogger(__name__)
__folder__ = os.path.dirname(os.path.realpath(__file__))


class LocalFileStorage:
    def __init__(self, base='data'):
        self.base_path = os.path.join(__folder__, base)
        self.create_local_data_folder()

    def create_local_data_folder(self):
        try:
            os.mkdir(self.base_path)
        except FileExistsError:
            pass

    def save(self, name, binary):
        path = os.path.join(self.base_path, name)
        with open(path, 'wb') as fileobj:
            fileobj.write(binary)


def download_public_nongeo_datasets(storage: LocalFileStorage):
    urls = ['https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h',
            'https://data.colorado.gov/Business/Business-Entity-Transaction-History/casm-dbbj',
            'https://data.colorado.gov/Business/Recently-Approved-Liquor-Licenses-in-Colorado/htyp-tqzh',
            'https://data.colorado.gov/Health/Restaurant-Inspections-in-Tri-County-Colorado-2018/cx7q-izrb',
            'https://data.colorado.gov/Business/Liquor-Licenses-in-Colorado/ier5-5ms2',
            'https://data.colorado.gov/Business/Liquor-Licenses-in-Colorado/ier5-5ms2',
            'https://data.colorado.gov/Energy/Alternative-Fuels-and-Electric-Vehicle-Charging-St/team-3ugz'
            'https://data.colorado.gov/Housing/Rents-by-Age-of-Building-in-Colorado-2016/hwir-8ay9',
            'https://data.colorado.gov/Energy/Alternative-Fuels-and-Electric-Vehicle-Charging-St/team-3ugz',
            'https://data.colorado.gov/Business/Licensed-Marijuana-Businesses-in-Colorado/sqs8-2un5',
            'https://data.colorado.gov/Business/Liquor-Permits-for-Special-Events-in-Colorado/d6t8-xish',
            'https://data.colorado.gov/Recreation/Points-of-Interest-in-Denver/y6w6-igw6',
            'https://data.colorado.gov/Economic-Growth/Food-Stores-in-Denver/hysf-mrke',
            'https://data.colorado.gov/Health/Body-Art-Licenses-in-Denver/n9gj-cjub',
            'https://data.colorado.gov/Business/Map-of-Recently-Expired-and-Surrendered-Liquor-Lic/x4sv-war6',
            'https://data.colorado.gov/Business/BusinessEntitiesColorado/jsn6-eu37',
            'https://data.colorado.gov/Business/filter-january-2018/hpd2-7ip9',
            'https://data.colorado.gov/Business/BusinessEntitiesColorado/jsn6-eu37',
            'https://data.colorado.gov/Business/business_entities_geo/i2yx-jknt',
            'https://data.colorado.gov/Business/GM-Entity-list/xxss-grk2',
            'https://data.colorado.gov/Business/All-Restaurants-in-Colorado/3x7k-rbx4',
            'https://data.colorado.gov/Business/All-Restaurants-in-Colorado/3x7k-rbx4']

    for url in urls:
        *_, dataset_name, dataset_id = os.path.normpath(url).split('/')
        download_url = 'https://data.colorado.gov/api/views/{}/rows.csv?accessType=DOWNLOAD'
        download_url = download_url.format(dataset_id)
        LOGGER.info('Downloading {}'.format(download_url))
        r = requests.get(download_url, allow_redirects=True)
        r.raise_for_status()
        name = '{}.csv'.format(dataset_name)
        storage.save(name, r.content)


def download_public_geo_datasets(storage: LocalFileStorage):
    urls = ['https://data.colorado.gov/Transportation/Road-Traffic-Counts-in-Colorado-2014/dx5q-y5je',
            'https://data.colorado.gov/Demographics/Census-Zip-Codes-in-Colorado-2016/rwak-e74e',
            'https://data.colorado.gov/Transportation/Construction-Project-Line-Segments-for-Funded-Road/dnyf-3m59']

    for url in urls:
        *_, dataset_name, dataset_id = os.path.normpath(url).split('/')
        download_url = 'https://data.colorado.gov/api/geospatial/{}?method=export&format=GeoJSON'
        download_url = download_url.format(dataset_id)
        LOGGER.info('Downloading {}'.format(download_url))
        r = requests.get(download_url, allow_redirects=True)
        r.raise_for_status()
        name = '{}.geojson'.format(dataset_name)
        storage.save(name, r.content)


def initialize_logging():
    logging.root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    logging.root.addHandler(handler)


def main():
    initialize_logging()

    storage = LocalFileStorage()
    download_public_nongeo_datasets(storage)
    download_public_geo_datasets(storage)

    '''
    api = twitter.Api(consumer_key=os.environ['TWITTER_CONSUMER_KEY'],
                      consumer_secret=os.environ['TWITTER_CONSUMER_SECRET'],
                      access_token_key=os.environ['TWITTER_TOKEN_KEY'],
                      access_token_secret=os.environ['TWITTER_TOKEN_SECRET'])
    results = api.GetSearch(raw_query="q=bicycle")
    print(results)
    print(len(results))
    '''


if __name__ == "__main__":
    main()
