from vi.cache import Cache
from vi.eve.api import api, HEADERS as API_HEADERS
from vi.settings import Settings

import requests

cache = Cache(default_expires=60*60*3, key_prefix='jumpbridge')
settings = Settings('jumpbridge')

settings.register_settings({
    'url': "https://s3.amazonaws.com/vintel-resources/{region_lower}_jb.txt"
})

HEADERS = {
    "User-Agent": API_HEADERS['User-Agent'],
}

class Jumpbridge(object):
    regionNames = None
    regionIDs = None
    systemNames = None
    systemIDs = None
    url = None

    @classmethod
    def get_region_info(cls, regionName):
        if cls.regionIDs is None:
            cls.regionNames = api.regions()
            cls.regionIDs = {value.lower(): key for key, value in cls.regionNames.items()}
            cls.systemNames = api.solarsystems()
            cls.systemIDs = {value.lower(): key for key, value in cls.systemNames.items()}
        region_lower = regionName.lower()
        region_id = cls.regionIDs.get(region_lower)
        if region_id is None:
            return {}
        return {
            'id': region_id,
            'region_lower': region_lower,
            'region': cls.regionNames[region_id]
        }

    @classmethod
    def load(cls, region):
        if cls.url is None:
            cls.url = settings['url']
        info = cls.get_region_info(region)
        cached = cache.get(cls.url, section=info['id'])
        if cached:
            return cached
        data = []
        try:
            url = url.format(**region_info)
            response = requests.get(url, headers=HEADERS)
            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if line.startswith('#') or line.startswith('\\') or line.startswith(';'):
                    continue
                parts = line.strip().split()
                if len(parts) != 3:
                    continue
                fromsys, connection, tosys = parts
                if fromsys.lower() in cls.systemIDs and tosys.lower() in cls.systemIDs:
                    data.append((fromsys, connection, tosys))
            cache.set(cls.url, data, section=info['id'])
        except:
            pass
        return data

    @classmethod
    def validate(cls, url):
        region_info = cls.get_region_info("Providence")
        url = url.format(**region_info)
        try:
            response = requests.get(url, headers=HEADERS)
            return response.status_code in (200, 301, 302)
        except:
            return False

    @classmethod
    def set_url(cls, url):
        settings['url'] = cls.url = url
