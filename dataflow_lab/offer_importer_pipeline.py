from __future__ import absolute_import

import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run(argv=None):
    parser = argparse.ArgumentParser()

    result = p.run()
    result.wait_until_finish()


class LoggerDoFn(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, *args, **kwargs):
        logging.info("Element: %s, Timestamp: %s", str(element), str(timestamp))
        yield element

class OfferImporterPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--source_csv',
            required=False,
            help='GCS path to the source csv containing the offers',
            default='gs://aliz-dataflow-training-materials/ecommerce/offers.csv')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
