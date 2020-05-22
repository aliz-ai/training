from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import BigQueryDisposition
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
        | 'Parsing offers' >> beam.Map(lambda line: model.Offer.parseFromCsvLine(line)) \
        | 'Logging' >> beam.ParDo(LoggerDoFn()) \
        | 'Convert offers to BQ row' >> beam.Map(lambda offer: offer.to_bq_row()) \
        | 'Writing offers to BQ' >> beam.io.WriteToBigQuery(table=offer_importer_pipeline_options.target_bq_table,
                                                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                                            schema=model.Offer.get_bq_schema())

    result = p.run()
    result.wait_until_finish()


class LoggerDoFn(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, *args, **kwargs):
        logging.info("Element: %s, Timestamp: %s",
                     str(element), str(timestamp))
        yield element


class OfferImporterPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--source_csv',
            required=True,
            help='GCS path to the source csv containing the offers')
        parser.add_argument(
            '--target_bq_table',
            required=True,
            help='Target BigQuery table, make sure to also specify the dataset.')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
