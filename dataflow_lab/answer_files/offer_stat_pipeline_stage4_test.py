import unittest

from apache_beam import Create
from apache_beam.runners import DirectRunner
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from dataflow_lab.cheat.offer_stat_pipeline_testing import OfferStatTransform


class MyTest(unittest.TestCase):

    def test(self):
        p = TestPipeline(DirectRunner())

        test_user = {'account': {'id': 1}, 'country': 'Germany'}
        test_account_offer = {'account_id': 1, 'account_offer_id': 2, 'offer_id': 3, }
        test_offer = {'offer_id': 3, 'offer_name': 'offer name'}

        users = p | "Create users" >> Create([test_user])
        account_offers = p | "Create account offers" >> Create([test_account_offer])
        offers = p | "Create offers" >> Create([test_offer])

        result = {'users': users, 'account_offers': account_offers, 'offers': offers} | OfferStatTransform()

        assert_that(result, self.assertSimple)

        p.run()

    def assertSimple(self, l):
        if not isinstance(l, list):
            raise Exception
        self.assertEqual(len(l), 1)
        result = l[0]
        self.assertEqual(result, {'account_offer_id': '2', 'offer_name': 'offer name', 'user_country': 'Germany'})
