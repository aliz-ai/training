from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

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

class OfferStatPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        pass


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()