from __future__ import absolute_import

import argparse
import logging

from apache_beam.options.pipeline_options import PipelineOptions

def run(argv=None):
    parser = argparse.ArgumentParser()

class OfferStatPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        pass

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
