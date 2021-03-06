from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

HOURLY_EVENTS_COUNT_SCHEMA = 'timestamp:TIMESTAMP,count:INTEGER'


def run(argv=None):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    hourly_events_count_pipeline_options = pipeline_options.view_as(HourlyEventsCountOptions)

    p = beam.Pipeline(options=pipeline_options)

    # TODO

    result = p.run()
    result.wait_until_finish()

class PubsubMessageParser(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, *args, **kwargs):
        yield (timestamp, element)


class HourlyEventsCountOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--hourly_events_count_topic',
            required=True)
        parser.add_argument(
            '--hourly_events_count_bq_table',
            required=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
