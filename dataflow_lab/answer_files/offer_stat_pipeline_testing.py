from __future__ import absolute_import

import argparse
import logging
import sys

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class OfferStatPipeline:
    """
    This pipeline is refactored from stage 1 to use a composite transform as the logic of the pipeline which we can test.
    """

    def __init__(self):
        pass

    def run(self, argv=None):
        parser = argparse.ArgumentParser()

        known_args, pipeline_args = parser.parse_known_args(argv)

        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options.view_as(SetupOptions).save_main_session = True
        offer_stat_pipeline_options = pipeline_options.view_as(OfferStatPipelineOptions)

        p = beam.Pipeline(options=pipeline_options)

        users = p | "Read users" >> beam.io.Read(beam.io.BigQuerySource(table=offer_stat_pipeline_options.users_bq_table, flatten_results=False))
        account_offers = p | "Read account offers" >> beam.io.Read(beam.io.BigQuerySource(table=offer_stat_pipeline_options.account_offers_bq_table, flatten_results=False))
        offers = p | "Read offers" >> beam.io.Read(beam.io.BigQuerySource(table=offer_stat_pipeline_options.offers_bq_table, flatten_results=False))

        result = ({'users': users, 'account_offers': account_offers, 'offers': offers} | "Compute stats" >> OfferStatTransform())

        result | 'Writing offers to BQ' >> beam.io.WriteToBigQuery(table=offer_stat_pipeline_options.offer_stat_bq_table,
                                                                   create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                                   write_disposition=BigQueryDisposition.WRITE_APPEND,
                                                                   schema='account_offer_id:STRING,user_country:STRING,offer_name:STRING')

        result = p.run()
        result.wait_until_finish()


class OfferStatTransform(beam.PTransform):

    def expand(self, pcolls):
        users = pcolls['users'] | beam.Map(self.map_user)
        account_offers = pcolls['account_offers'] | beam.Map(self.map_account_offer)
        offers = pcolls['offers'] | beam.Map(self.map_offer)

        return ({'users': users, 'account_offers': account_offers} | beam.CoGroupByKey()) \
               | beam.Map(self.merge_user_country, offers=pvalue.AsDict(offers))

    @staticmethod
    def map_user(user_row):
        return user_row['account']['id'], user_row['country']

    @staticmethod
    def map_account_offer(row):
        return row['account_id'], row

    @staticmethod
    def map_offer(row):
        return row['offer_id'], row['offer_name']

    @staticmethod
    def merge_user_country(grouped_data, offers):
        (account_id, info) = grouped_data
        account_offer = info['account_offers'][0]

        joined_data = {}
        joined_data['account_offer_id'] = str(account_offer['account_offer_id'])

        offer_id = account_offer['offer_id']
        offer_name = offers[offer_id]
        if offer_name is not None:
            joined_data['offer_name'] = offer_name
        else:
            logging.warning("No offer found with offer_id: %s", offer_id)

        user_country = info['users'][0]
        if user_country:
            joined_data['user_country'] = user_country
        else:
            logging.warning("No user found with account_id: %s", account_id)

        logging.info("Country joined: %s", joined_data)
        return joined_data


class LoggerDoFn(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, *args, **kwargs):
        logging.info("Element: %s, Timestamp: %s", str(element), str(timestamp))
        yield element


class OfferStatPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--users_bq_table',
            required=True)
        parser.add_argument(
            '--account_offers_bq_table',
            required=True)
        parser.add_argument(
            '--offers_bq_table',
            required=True)
        parser.add_argument(
            '--offer_stat_bq_table',
            required=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    OfferStatPipeline().run(sys.argv)
