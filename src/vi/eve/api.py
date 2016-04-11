import logging
import requests
import time
import xml.etree.ElementTree as ET

from datetime import datetime

try:
    from functools import lru_cache
except ImportError:
    try:
        from functools32 import lru_cache
    except ImportError:
        logging.warning("Unable to load lru_cache, for best performance, run this with python3, or install the functools32 package")
        lru_cache = lambda x: x
from vi.cache import Cache, DEFAULT_EXPIRES
from vi.version import VERSION

DEFAULT_CACHE = object()

BASE_URLS = {
    "api": "https://api.eveonline.com/",
    "public-crest": "https://public-crest.eveonline.com/",
    "authed-crest": "https://crest-tq.eveonline.com/",
    "image": "https://image.eveonline.com/",
}

HEADERS = {
    "User-Agent": "Vintel/{}".format(VERSION),
    "Accept": "application/vnd.ccp.eve.Api-v3+json",
}

cache = Cache(key_prefix='api', default_expires=60*60*6)

def parse_datetime(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


class EveApi(object):
    def urljoin(cls, base, path):
        """
        Joins two url parts together

        Args:
            base: Either the base url, or one of the BASE_URLS aliases.
            path: The url part to join to it.

        Returns:
            A string containing the joined url.
        """
        return requests.compat.urljoin(BASE_URLS.get(base, base), path)

    def apiRequest(self, path, params = None, cacheResponse = False, **kwargs):
        if params is None:
            params = {}
        if cacheResponse:
            key, section, expires = cacheResponse
            cached = cache.get(key, section=section)
            if cached:
                return ET.fromstring(cached)
        url = self.urljoin("api", path)
        logging.debug("Making an API request to %s", url)
        headers = HEADERS
        if 'headers' in kwargs:
            headers = kwargs['headers']
            headers.update(HEADERS)
        response = requests.get(url, params, headers=headers, **kwargs)
        content = response.content
        tree = ET.fromstring(content)
        if cacheResponse:
            if expires == 'smart':
                expires = self.extractApiCacheTime(tree)
            cache.set(key, content, section=section, expires=expires or DEFAULT_EXPIRES)
        return tree

    def eveTime(self):
        return datetime.utcnow()

    def eveTimeEpoch(self):
        return time.mktime(self.eveTime().timetuple())

    def namesToId(self, names, useResultingCase=False):  # TODO: Cache check
        """
        Queries the EVE API for the CharacterID of one or more character names

        Args:
            names: a list of character names to query
            useResultingCase: if set to False (default), use names as passed, if True,
                              use the names as returned from the EVE API

        Returns:
            A dict containing the results, for each name found.
                The key of the dictionary is the name as passed to the function, unless
                useResultingCase is set to True, in which case the name is as returned
                by the EVE API. The value is, obviously, their character ID.
                Items not found by the EVE API will not be in the dict.
        """
        if not isinstance(names, list):
            raise ValueError("names should be a list")
        if len(names) == 0:
            return {}
        logging.debug("Requesting an ID lookup for %d name%s", len(names), '' if len(names) == 1 else '')
        nameslower = {name.lower(): name for name in names}
        cached = cache.get_many(names, section='namesToId')
        names = [x for x in names if x not in cached]
        if not names:  # Everything was cached
            return cached
        ret_normal = {}
        ret_lower = {}
        try:
            response = self.apiRequest("eve/CharacterID.xml.aspx", {'names': ','.join(names)})
            for item in response.findall("./result/rowset/row"):
                data = item.attrib
                try:
                    characterID = int(data['characterID'])
                    if not characterID:
                        continue
                except:
                    continue
                ret_normal[nameslower[data['name'].lower()]] = characterID
                ret_lower[data['name']] = characterID
        except Exception as ex:
            logging.error("Error during namesToIds call: %s", ex)
        logging.debug("%d ID%s returned", len(ret_normal), '' if len(ret_normal) == 1 else '')
        cache.set_many(ret_lower, section='namesToId')
        if useResultingCase:
            ret_lower.update(cached)
            return ret_lower
        else:
            ret_normal.update(cached)
            return ret_normal

    @lru_cache(maxsize=128)
    def nameToId(self, name):
        """
        A shorthand alias for namesToId([name]).get(name)

        This function is cached with lru_cache to ensure that single, repeated nameToId calls don't
            needlessly call more 'expensive' cache checks.
        """
        return self.namesToId([name]).get(name)

    def characterExists(self, name):
        """
        A shorthand alias for bool(nameToId(name))
        """
        return bool(self.nameToId(name))

    def idsToName(self, characterIDs):  # TODO: Cache check
        """
        Queries the EVE API for the name of the characters from their IDs.

        Args:
            characterIDs: A list of character IDs to query, IDs may be passed
                either as string, or as int.

        Returns:
            A dict containing the results, for each character ID found.
                The key is the character ID (as integer, regardless of how it was passed),
                the value, obviously, is the character name as found.
                Items not found by the EVE API will not be in the dict.
        """
        if not isinstance(characterIDs, list):
            raise ValueError("characterIDs is not a list")
        if len(characterIDs) == 0:
            return {}
        characterIDs = list(map(str, characterIDs))
        cached = cache.get_many(characterIDs, section='idsToName')
        characterIDs = [x for x in characterIDs if x not in cached]
        if not characterIDs:
            return cached
        ret = {}
        logging.debug("Requesting a name lookup for %d id%s", len(characterIDs), '' if len(characterIDs) == 1 else '')
        try:
            response = self.apiRequest("eve/CharacterName.xml.aspx", {'ids': ','.join(characterIDs)})
            for item in response.findall("./result/rowset/row"):
                data = item.attrib
                try:
                    characterID = int(data['characterID'])
                except:
                    continue
                ret[characterID] = data['name']
        except:
            pass
        cache.set_many(ret, section='idsToName')
        logging.debug("%d Name%s returned", len(ret), '' if len(ret) == 1 else '')
        ret.update(cached)
        return ret

    @lru_cache(maxsize=128)
    def idToName(self, characterID):
        """
        return idsToName([characterID]).get(characterID)

        This function is cached with lru_cache to ensure that single, repeated idToName calls don't
            needlessly call more 'expensive' cache checks.
        """
        return self.idsToName([characterID]).get(characterID)

    def avatarFromID(self, characterID, size=32):  # TODO: Caching
        """
        Retrieves the current character avatar for the specified characterID

        Args:
            characterID: The characterID of the character
            size: The size of the avatar to retrieve (default: 32)

        Returns:
            None if no avatar could be retrieved, otherwise the raw avatar data.
                In python3, this returns a bytes array, in python2, a string array (not unicode)
        """
        params = {
            'characterID': characterID,
            'size': size
        }
        cache_key = '{characterID}:{size}'.format(**params)
        cached = cache.get(cache_key, section='avatarFromID')
        if cached:
            return cached
        url = self.urljoin("image", "Character/{characterID}_{size}.jpg".format(params))
        try:
            content = requests.get(url).content
            cache.set(cache_key, content, section='avatarFromID')
            return content
        except Exception as e:
            logging.error("Exception during avatarFromID: %s", e)
            return None

    def avatarFromName(self, characterName, size=32):
        """
        A shorthand alias for avatarFromId(nameToId(characterName), size)
        """
        characterID = self.nameToId(characterName)
        if not characterID:
            return None
        return self.avatarFromID(characterID, size)

    def characterInformation(self, characterID):
        response = self.apiRequest("eve/CharacterInfo.xml.aspx", {'characterID': characterID},
                                   cacheResponse = (characterID, 'characterInformation', 'smart'))

        character = {}
        for row in response.findall('./result/*'):
            if row.tag == 'rowset':
                continue
            value = row.text
            if row.tag.endswith("Date"):
                value = parse_datetime(value)
            if row.tag.endswith("ID"):
                value = int(value)
            character[row.tag] = value
        corporations = []
        for row in response.findall('./result/rowset[@name="employmentHistory"]/*'):
            data = row.attrib
            corporations.append({
                'corporationID': int(data['corporationID']),
                'corporationName': data['corporationName'],
                'startDate': parse_datetime(data['startDate'])
                })
        character['employmentHistory'] = corporations
        return character

    def extractApiCacheTime(self, tree):
        currentTime, cachedUntil = None, None
        for child in tree.getchildren():
            if child.tag not in ['currentTime', 'cachedUntil']:
                continue
            value = parse_datetime(child.text)
            if child.tag == 'currentTime':
                currentTime = value
            elif child.tag == 'cachedUntil':
                cachedUntil = value
        if not all([currentTime, cachedUntil]):
            return DEFAULT_EXPIRES
        return (cachedUntil - currentTime).total_seconds()

    def systemJumps(self):  # TODO: Caching
        """
        Retrieves the current system jumps data

        Returns: A dictionary with the solarsystem id as key, and the amount of jumps as value.
        """
        cached = cache.get('systemJumps')
        if cached:
            return cached
        ret = {}
        try:
            response = self.apiRequest("map/Jumps.xml.aspx")
            for item in response.findall("./result/rowset/row"):
                try:
                    data = item.attrib
                    systemID = int(data['solarSystemID'])
                    jumps = int(data['shipJumps'])
                    ret[systemID] = jumps
                except ValueError:
                    continue
        except:
            pass
        expires = self.extractApiCacheTime(response)
        cache.set('systemJumps', ret, expires=expires)
        return ret

    def systemKills(self):  # TODO: Caching
        """
        Retrieves the current system kill data

        Returns: A dictionary with the solarsystem id as key, and as value a dictionary
                    containing the kills per type.
        """
        cached = cache.get('systemKills')
        if cached:
            return cached
        ret = {}
        try:
            response = self.apiRequest("map/Kills.xml.aspx")
            for item in response.findall("./result/rowset/row"):
                try:
                    data = item.attrib
                    systemID = int(data['solarSystemID'])
                    ret[systemID] = {
                        'ship': int(data['shipKills']),
                        'faction': int(data['factionKills']),
                        'pod': int(data['podKills']),
                    }
                except ValueError:
                    continue
        except:
            pass
        expires = self.extractApiCacheTime(response)
        cache.set('systemKills', ret, expires=expires)
        return ret

    def systemInformation(self):
        jumps = self.systemJumps()
        kills = self.systemKills()
        return {key: dict(value, jumps=jumps.get(key)) for key, value in kills.items()}

api = EveApi()
