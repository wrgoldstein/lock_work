from unittest import TestCase
import pandas as pd

from lib.squash_air import RuleSquasher

class TestSquashAir(TestCase):
    def test_merge(self):
        obj = RuleSquasher('test/fixtures/FAM-SGT-10_LINEAC.xlsx', 'foofoo')
        obj.load_data()
        print(obj.dataset['Parent Sgmt'])