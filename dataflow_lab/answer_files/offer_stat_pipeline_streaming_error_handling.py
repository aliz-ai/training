from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.transforms import window, trigger
from apache_beam import pvalue
from apache_beam.io import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
import uuid
import datetime

from apache_beam.transforms.trigger import AccumulationMode

OFFER_STAT_BQ_SCHEMA = 'offer_id:STRING,count:INTEGER,window_start:TIMESTAMP,window_end:TIMESTAMP,created_at:TIMESTAMP'


def run(argv=None):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    offer_stat_pipeline_options = pipeline_options.view_as(OfferStatPipelineOptions)

    p = beam.Pipeline(options=pipeline_options)

    main, error = p | "Read account offer from PS" >> beam.io.ReadFromPubSub(topic=offer_stat_pipeline_options.account_offers_topic) \
                  | "Parse message" >> beam.ParDo(PubsubMessageParser()).with_outputs('error', main='main')

    error | 'Writing errors to BQ' >> beam.io.WriteToBigQuery(table=offer_stat_pipeline_options.error_bq_table,
                                                              create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                              write_disposition=BigQueryDisposition.WRITE_APPEND,
                                                              schema='step:STRING,data:STRING,exception:STRING')

    main \
    | "Windowing" >> beam.WindowInto(window.FixedWindows(60),
                                     trigger=trigger.AfterWatermark(early=trigger.AfterProcessingTime(20)),
                                     accumulation_mode=AccumulationMode.ACCUMULATING) \
    | "WithKeys" >> beam.Map(lambda account_offer: ((account_offer['offer_id']), account_offer)) \
    | beam.GroupByKey() \
    | 'Count distinct accounts' >> beam.ParDo(DistinctAccountCount()) \
    | 'Map to BQ row' >> beam.ParDo(ConvertStatToBQRow()) \
    | 'Writing offers to BQ' >> beam.io.WriteToBigQuery(table=offer_stat_pipeline_options.offer_stat_bq_table,
                                                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                                                        schema=OFFER_STAT_BQ_SCHEMA)

    result = p.run()
    result.wait_until_finish()


class PubsubMessageParser(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, *args, **kwargs):
        try:
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

        except Exception as e:
            logging.error("Error processing element: %s", str(element))
            data = str(element)
            exc = str(e)
            error = {'step': 'MergeDoFn', 'data': data, 'exception': exc}
            yield pvalue.TaggedOutput('error', error)


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


class DistinctAccountCount(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, *args, **kwargs):
        logging.info("Calculating distinct account ids for: %s", element)
        (offer_id, enriched_offers) = element
        distinct_account_ids = set()
        for enriched_offer in enriched_offers:
            distinct_account_ids.add(enriched_offer['account_id'])

        output = (offer_id, len(distinct_account_ids))
        logging.info("Distinct count result: %s", output)
        yield output


class OfferStatPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--account_offers_topic',
            required=True)
        parser.add_argument(
            '--offer_stat_bq_table',
            required=True)
        parser.add_argument(
            '--error_bq_table',
            required=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
