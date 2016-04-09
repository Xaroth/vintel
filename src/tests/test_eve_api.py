import unittest

class Test_eve_api(unittest.TestCase):
    CHARACTERS = {
        "Xaroth Brook": 931334939,
        "Xanthos": 183452271,
    }
    LOWEST_SOLARSYSTEM_ID = 30000001
    def test_namesToIDs(self):
        """
        Test the namesToId function of the API subsystem using some pre-tested results.
        """
        from vi.eve.api import EveApi, Cache
        Cache.initialize('test.sqlite')
        api = EveApi()
        result = api.namesToId(list(self.CHARACTERS.keys()))
        self.assertEqual(sorted(self.CHARACTERS.values()), sorted(result.values()), "Unable to request the proper values")

    def test_idsToName(self):
        """
        Test the idsToName function of the API subsystem using some pre-tested results.
        """
        from vi.eve.api import EveApi, Cache
        Cache.initialize('test.sqlite')
        api = EveApi()
        result = api.idsToName(list(self.CHARACTERS.values()))
        self.assertEqual(sorted(self.CHARACTERS.keys()), sorted(result.values()), "Unable to request the proper values")

    def test_systemJumps(self):
        """
        Retrieve the system jumps list and check if there is valid data within it.
        """
        from vi.eve.api import EveApi, Cache
        Cache.initialize('test.sqlite')
        api = EveApi()
        jumps = api.systemJumps()
        self.assertIn(self.LOWEST_SOLARSYSTEM_ID, jumps, "Unable to find %s in the jumps list" % self.LOWEST_SOLARSYSTEM_ID)

    def test_systemKills(self):
        """
        Retrieve the system jumps list and check if there is valid data within it.
        """
        from vi.eve.api import EveApi, Cache
        Cache.initialize('test.sqlite')
        api = EveApi()
        kills = api.systemKills()
        self.assertIn(self.LOWEST_SOLARSYSTEM_ID, kills, "Unable to find %s in the kills list" % self.LOWEST_SOLARSYSTEM_ID)

    def test_characterInformation(self):
        from vi.eve.api import EveApi, Cache
        Cache.initialize('test.sqlite')
        api = EveApi()
        for characterName, characterID in self.CHARACTERS.items():
            info = api.characterInformation(characterID)
            self.assertEqual(info['characterName'], characterName, "Charactername '%s' does not match the assumed '%s'" % (
                             info['characterName'], characterName))
            self.assertEqual(info['characterID'], characterID, "Character ID '%d' does not match the asumed '%d'" % (
                             info['characterID'], characterID))


if __name__ == '__main__':
    unittest.main()
