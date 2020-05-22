from __future__ import absolute_import

import argparse
import datetime
import logging
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

OFFER_STAT_BQ_SCHEMA = 'offer_id:STRING,count:INTEGER,window_start:TIMESTAMP,window_end:TIMESTAMP,created_at:TIMESTAMP'


def run(argv=None):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    offer_stat_pipeline_options = pipeline_options.view_as(OfferStatPipelineOptions)

    p = beam.Pipeline(options=pipeline_options)

    # TODO

    result = p.run()
    result.wait_until_finish()


class PubsubMessageParser(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, *args, **kwargs):
        logging.info("Parsing message %s", element)
        splits = element.split(' ')
        account_id = splits[0]
        offer_id = splits[1]
        event_timestamp = timestamp
        account_offer_id = uuid.uuid4()
        if len(splits) == 3:
            event_timestamp = event_timestamp - int(splits[2])

        parsed_event = {'account_id': int(account_id), 'offer_id': int(offer_id), 'account_offer_id': account_offer_id}
        logging.info("Parsed element %s with timestamp %s", str(parsed_event), str(event_timestamp))
        yield beam.window.TimestampedValue(parsed_event, event_timestamp)


class ConvertStatToBQRow(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, *args, **kwargs):
        (offer_id, count) = element
        bq_row = {'offer_id': str(offer_id),
                  'count': count,
                  'created_at': datetime.datetime.now().strftime('%s'),
                  'window_start': window.start.micros / 1000000,
                  'window_end': window.end.micros / 1000000}
        logging.info("Convert BQ row: %s", bq_row)
        yield bq_row


class OfferStatPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--account_offers_topic',
            required=True)
        parser.add_argument(
            '--offer_stat_bq_table',
            required=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
