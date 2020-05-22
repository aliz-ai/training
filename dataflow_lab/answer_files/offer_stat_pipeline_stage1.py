from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from common import beam_util


def run(argv=None):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    offer_stat_pipeline_options = pipeline_options.view_as(
        OfferStatPipelineOptions)

    p = beam.Pipeline(options=pipeline_options)

    users = p | "Read users" >> beam.io.Read(beam.io.BigQuerySource(table=offer_stat_pipeline_options.users_bq_table, flatten_results=False)) \
        | beam.Map(lambda user_row: (user_row['account']['id'], user_row['country']))

    account_offers = p | "Read account offers" >> beam.io.Read(beam.io.BigQuerySource(table=offer_stat_pipeline_options.account_offers_bq_table, flatten_results=False)) \
        | beam.Map(lambda row: (row['account_id'], row))

    ({'users': users, 'account_offers': account_offers} | beam.CoGroupByKey()) \
        | beam.ParDo(UserCountryMerger()) \
        | beam.ParDo(beam_util.LoggerDoFn())

    result = p.run()
    result.wait_until_finish()


class UserCountryMerger(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, *args, **kwargs):
        logging.info("Merging: %s, Timestamp: %s, Window: %s",
                     str(element), str(timestamp), str(window))
        (account_id, info) = element

        joined_data = {'account_id': account_id}
        if info['account_offers']:
            account_offer = info['account_offers'][0]
            joined_data['account_offer_id'] = account_offer['account_offer_id']
        else:
            logging.warning(
                "No account_offer found with account_id: %s (%s)", account_id, window)

        if info['users']:
            joined_data['user_country'] = info['users'][0]
        else:
            logging.warning(
                "No user found with account_id: %s (%s)", account_id, window)

        logging.info("Country joined: %s", joined_data)
        yield joined_data


class OfferStatPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--users_bq_table',
            required=True)
        parser.add_argument(
            '--account_offers_bq_table',
            required=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
