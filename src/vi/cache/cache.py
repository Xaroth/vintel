###########################################################################
#  Vintel - Visual Intel Chat Analyzer									  #
#  Copyright (C) 2014-15 Sebastian Meyer (sparrow.242.de+eve@gmail.com )  #
#																		  #
#  This program is free software: you can redistribute it and/or modify	  #
#  it under the terms of the GNU General Public License as published by	  #
#  the Free Software Foundation, either version 3 of the License, or	  #
#  (at your option) any later version.									  #
#																		  #
#  This program is distributed in the hope that it will be useful,		  #
#  but WITHOUT ANY WARRANTY; without even the implied warranty of		  #
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the		  #
#  GNU General Public License for more details.							  #
#																		  #
#																		  #
#  You should have received a copy of the GNU General Public License	  #
#  along with this program.	 If not, see <http://www.gnu.org/licenses/>.  #
###########################################################################

import sqlite3
import threading
import time
from datetime import datetime

import six
if six.PY2:
    def to_blob(x):
        return buffer(str(x))
    def from_blob(x):
        return str(x[0][0])
else:
    def to_blob(x):
        return x
    def from_blob(x):
        return x

import logging

from six.moves import cPickle as pickle
from vi.cache.dbstructure import updateDatabase

NO_DEFAULT = object()
DEFAULT_EXPIRES = object()

class CacheNotInitializedException(Exception):
    pass


class CacheBase(object):
    UPDATES = [
        (1, [
            "CREATE TABLE version (version INT)",
            "INSERT INTO version (version) VALUES (0)",
            """CREATE TABLE cache (
                key VARCHAR PRIMARY KEY,
                blobdata BLOB,
                intdata INT,
                stringdata VARCHAR,
                expires INT NOT NULL
            )""",
        ]),
    ]
    CURRENT_VERSION = 1
    WRITE_LOCK = threading.Lock()

    _initialized = False
    _path = None

    _conn = None

    @property
    def uses_global(self):
        return self._path == self.__class__._path

    @classmethod
    def initialize(cls, path):
        logging.warning("Initializing Cache on path '%s'", path)
        with cls.WRITE_LOCK:
            cls._path = path
            cls.check_version()
            cls._initialized = True

    @classmethod
    def check_version(cls, conn=None):
        close = False
        if conn is None:
            close = True
            conn = cls._get_connection()
        query = "SELECT version FROM version"
        version = 0
        try:
            version = conn.execute(query).fetchall()[0][0]
        except sqlite3.OperationalError as soe:
            if "no such table" not in str(soe):
                raise soe
        except IndexError:
            pass
        queries = []
        for patchlevel, updates in cls.UPDATES:
            if version < patchlevel:
                queries.extend(updates)
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
        conn.commit()
        if version != cls.CURRENT_VERSION:
            cursor.execute("UPDATE version SET version = ?", (cls.CURRENT_VERSION,))
        conn.commit()
        cls.cleanup(conn)
        if close:
            conn.close()

    @classmethod
    def check_initialized(cls):
        if not self.uses_global:
            return
        if not cls._initialized:
            raise CacheNotInitializedException()

    @classmethod
    def utcnow(cls):
        return time.mktime(datetime.utcnow().timetuple())

    @classmethod
    def _get_connection(cls, path=None):
        return sqlite3.connect(path or cls._path)

    @property
    def conn(self):
        if not self._conn:
            if not self._path and not self.__class__._path:
                raise CacheNotInitializedException()
            self._conn = self._get_connection(self._path)
            if not self.uses_global:
                with self.WRITE_LOCK:
                    self.check_version(self._conn)
        return self._conn

    @classmethod
    def cleanup(cls, conn=None):
        close = False
        if conn is None:
            close = True
            conn = cls._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM cache WHERE expires < ?", (cls.utcnow(),))
        conn.commit()
        if close:
            conn.close()

    def close(self):
        if self._conn:
            with self.WRITE_LOCK:  # Lock so we can ensure that all writes are complete, including ours.
                self._conn.close()


class NewCache(CacheBase):
    """
    A new (improved) version of the caching system, allows for easy get/set access to cacheable data.
    """
    def __init__(self, default_expires=DEFAULT_EXPIRES, key_prefix=None, override_path=None):
        """
        Initializes a new cache access point

        Args:
            default_expiry: Instead of requiring an expiry on each set(_many) call a default
                                can be set to be used instead.
            key_prefix: Prefix all cache keys used by get/set/delete(_many) calls with this prefix.
                            Note that the prefix and key used are joined by a colon.
            override_path: Use this path instead of the normal 'global' cache location.
        """
        self._expires=default_expires
        self._key_prefix=key_prefix
        self._path=override_path
        if not self.uses_global:
            self.WRITE_LOCK = threading.Lock()

    def _get_key(self, key, section=None):
        return ':'.join([str(x) for x in [self._key_prefix, section, key] if x is not None])

    def _reverse_key(self, key, section=None):
        return key[len(self._get_key('', section=section)):]

    def _get_expiry(self, expires):
        if expires is not DEFAULT_EXPIRES:
            return expires
        if self._expires is DEFAULT_EXPIRES:
            raise ValueError("No expiry set and no default expires set")
        return self._expires

    def to_python(self, fields):
        blobdata, intdata, stringdata = fields
        for x in [intdata, stringdata]:
            if x is not None:
                return x
        return pickle.loads(from_blob(blobdata))
        
    def from_python(self, obj):
        if isinstance(obj, six.integer_types):
            return None, obj, None
        elif isinstance(obj, six.string_types):
            return None, None, obj
        else:
            return to_blob(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)), None, None

    def get(self, key, default=NO_DEFAULT, section=None, allowExpired=False):
        logging.info("cache.get: %s", self._get_key(key, section))
        query = "SELECT blobdata, intdata, stringdata, expires FROM cache WHERE key = ? {expired}"
        key = self._get_key(key, section)
        now = self.utcnow()
        expired = [now] if allowExpired else []
        query = query.format(expired="AND expired <= ?" if allowExpired else "")
        fields = self.conn.execute(query, [key]+expired).fetchone()
        if fields:
            return self.to_python(fields[0:3])
        return None if default is NO_DEFAULT else default

    def get_many(self, keys, section=None, allowExpired=False):
        logging.info("cache.get_many for %d keys", len(keys))
        now = self.utcnow()
        query = "SELECT key, blobdata, intdata, stringdata, expires FROM cache WHERE key IN ({seq}) {expired}"
        expired = [now] if allowExpired else []
        query = query.format(seq=', '.join(['?']*len(keys)), expired=" AND expired < ?" if allowExpired else "")
        data = {}
        for row in self.conn.execute(query, [self._get_key(key, section) for key in keys] + expired):
            key = self._reverse_key(row[0], section)
            fields = row[1:4]
            data[key] = self.to_python(fields)
        logging.debug("cache.get_many returning %d", len(data))
        return data

    def set(self, key, value, expires=DEFAULT_EXPIRES, section=None):
        logging.info("cache.set: %s", self._get_key(key, section))
        now = self.utcnow()
        expires = self._get_expiry(expires)
        expires = expires if expires > (60*60*24*7) else now + expires
        cursor = self.conn.cursor()
        parameters = (self._get_key(key, section), expires) + self.from_python(value)
        with self.WRITE_LOCK:
            query = "DELETE FROM cache WHERE key = ?"
            cursor.execute(query, (key,))
            query = "INSERT INTO cache (key, expires, blobdata, intdata, stringdata) VALUES (?, ?, ?, ?, ?)"
            cursor.execute(query, parameters)
            self.conn.commit()

    def set_many(self, data, expires=DEFAULT_EXPIRES, section=None):
        logging.info("cache.set_many for %d keys", len(data))
        now = self.utcnow()
        expires = self._get_expiry(expires)
        expires = expires if expires > (60*60*24*7) else now + expires
        cursor = self.conn.cursor()
        parameters = [(self._get_key(key, section), expires) + self.from_python(value) for key, value in data.items()]
        with self.WRITE_LOCK:
            query = "DELETE FROM cache WHERE key IN ({seq})".format(seq=', '.join(['?']*len(data)))
            cursor.execute(query, tuple(data.keys()))
            query = "INSERT INTO cache (key, expires, blobdata, intdata, stringdata) VALUES (?, ?, ?, ?, ?)"
            cursor.executemany(query, parameters)
            self.conn.commit()

    def delete(self, key, section=None):
        self.delete([key], section=section)

    def delete_many(self, keys, section=None):
        cursor = self.conn.cursor()
        with self.WRITE_LOCK:
            query = "DELETE FROM cache WHERE key IN ({seq})".format(seq=', '.join(['?']*len(keys)))
            cursor.execute(query, [self._get_key(key, section) for key in keys])
            self.conn.commit()


class Cache(object):
    # Cache checks PATH_TO_CACHE when init, so you can set this on a
    # central place for all Cache instances.
    PATH_TO_CACHE = None

    # Ok, this is dirty. To make sure we check the database only
    # one time/runtime we will change this classvariable after the
    # check. Following inits of Cache will now, that we allready checked.
    VERSION_CHECKED = False

    # Cache-Instances in various threads: must handle concurrent writings
    SQLITE_WRITE_LOCK = threading.Lock()

    def __init__(self, pathToSQLiteFile="cache.sqlite3"):
        """ pathToSQLiteFile=path to sqlite-file to save the cache. will be ignored if you set Cache.PATH_TO_CACHE before init
        """
        if Cache.PATH_TO_CACHE:
            pathToSQLiteFile = Cache.PATH_TO_CACHE
        self.con = sqlite3.connect(pathToSQLiteFile)
        if not Cache.VERSION_CHECKED:
            with Cache.SQLITE_WRITE_LOCK:
                self.checkVersion()
        Cache.VERSION_CHECKED = True

    def checkVersion(self):
        query = "SELECT version FROM version;"
        version = 0
        try:
            version = self.con.execute(query).fetchall()[0][0]
        except Exception as e:
            if (isinstance(e, sqlite3.OperationalError) and "no such table: version" in str(e)):
                pass
            elif (isinstance(e, IndexError)):
                pass
            else:
                raise e
        updateDatabase(version, self.con)

    def putIntoCache(self, key, value, maxAge=60 * 60 * 24 * 3):
        """ Putting something in the cache maxAge is maximum age in seconds
        """
        with Cache.SQLITE_WRITE_LOCK:
            query = "DELETE FROM cache WHERE key = ?"
            self.con.execute(query, (key,))
            query = "INSERT INTO cache (key, data, modified, maxAge) VALUES (?, ?, ?, ?)"
            self.con.execute(query, (key, value, time.time(), maxAge))
            self.con.commit()

    def getFromCache(self, key, outdated=False):
        """ Getting a value from cache
            key = the key for the value
            outdated = returns the value also if it is outdated
        """
        query = "SELECT key, data, modified, maxage FROM cache WHERE key = ?"
        founds = self.con.execute(query, (key,)).fetchall()
        if len(founds) == 0:
            return None
        elif founds[0][2] + founds[0][3] < time.time() and not outdated:
            return None
        else:
            return founds[0][1]

    def putPlayerName(self, name, status):
        """ Putting a playername into the cache
        """
        with Cache.SQLITE_WRITE_LOCK:
            query = "DELETE FROM playernames WHERE charname = ?"
            self.con.execute(query, (name,))
            query = "INSERT INTO playernames (charname, status, modified) VALUES (?, ?, ?)"
            self.con.execute(query, (name, status, time.time()))
            self.con.commit()

    def getPlayerName(self, name):
        """ Getting back infos about playername from Cache. Returns None if the name was not found, else it returns the status
        """
        selectquery = "SELECT charname, status FROM playernames WHERE charname = ?"
        founds = self.con.execute(selectquery, (name,)).fetchall()
        if len(founds) == 0:
            return None
        else:
            return founds[0][1]

    def putAvatar(self, name, data):
        """ Put the picture of an player into the cache
        """
        with Cache.SQLITE_WRITE_LOCK:
            # data is a blob, so we have to change it to buffer
            data = to_blob(data)
            query = "DELETE FROM avatars WHERE charname = ?"
            self.con.execute(query, (name,))
            query = "INSERT INTO avatars (charname, data, modified) VALUES (?, ?, ?)"
            self.con.execute(query, (name, data, time.time()))
            self.con.commit()

    def getAvatar(self, name):
        """ Getting the avatars_pictures data from the Cache. Returns None if there is no entry in the cache
        """
        selectQuery = "SELECT data FROM avatars WHERE charname = ?"
        founds = self.con.execute(selectQuery, (name,)).fetchall()
        if len(founds) == 0:
            return None
        else:
            # dats is buffer, we convert it back to str
            data = from_blob(founds[0][0])
            return data

    def removeAvatar(self, name):
        """ Removing an avatar from the cache
        """
        with Cache.SQLITE_WRITE_LOCK:
            query = "DELETE FROM avatars WHERE charname = ?"
            self.con.execute(query, (name,))
            self.con.commit()

    def recallAndApplySettings(self, responder, settingsIdentifier):
        settings = self.getFromCache(settingsIdentifier)
        if settings:
            settings = eval(settings)
            for setting in settings:
                obj = responder if not setting[0] else getattr(responder, setting[0])
                # logging.debug("{0} | {1} | {2}".format(str(obj), setting[1], setting[2]))
                try:
                    getattr(obj, setting[1])(setting[2])
                except Exception as e:
                    logging.error(e)


