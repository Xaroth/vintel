from .cache import Cache, NO_DEFAULT
import os
import sys
import six
if six.PY2:
    INT_MAX = sys.maxint
else:
    INT_MAX = sys.maxsize

class Settings(object):
    _cache = None
    def __init__(self, section=None):
        self._defaults = {}
        self.section=section

    @classmethod
    def initialize(cls, directory):
        cls._cache = Cache(default_expires=INT_MAX, override_path=os.path.join(directory, 'settings.sqlite3'))

    @classmethod
    def close(cls):
        cls._cache.close()

    def register_setting(self, key, default_value):
        self._defaults[key] = default_value

    def register_settings(self, settings):
        self._defaults.update(settings)

    def __getitem__(self, key):
        return self._cache.get(key, default=self._defaults.get(key, NO_DEFAULT), section=self.section, allowExpired=True)

    def __setitem__(self, key, value):
        if value is None:
            self._cache.delete(key, section=section)
        else:
            self._cache.set(key, value, section=self.section)

globalsettings = Settings(section='global')
