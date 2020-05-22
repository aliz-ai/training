from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from common import model


def run(argv=None):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    offer_importer_pipeline_options = pipeline_options.view_as(
        OfferImporterPipelineOptions)

    p = beam.Pipeline(options=pipeline_options)

    p | beam.io.ReadFromText(file_pattern=offer_importer_pipeline_options.source_csv, skip_header_lines=1).with_output_types(str) \
        | beam.Map(lambda line: model.Offer.parseFromCsvLine(line)) \
        | 'log' >> beam.ParDo(LoggerDoFn())

    result = p.run()
    result.wait_until_finish()


class LoggerDoFn(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, *args, **kwargs):
        logging.info("Sum: %s, Timestamp: %s", str(element), str(timestamp))
        yield element


class OfferImporterPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--source_csv',
            required=True,
            help='GCS path to the source csv containing the offers')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
