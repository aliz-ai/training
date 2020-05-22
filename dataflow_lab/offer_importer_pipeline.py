from __future__ import absolute_import

import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def run(argv=None):
    """Main entry point; defines and runs the pipeline."""

    # step 1a: specify the various configurations that you want your job to have and add them into the parser object so they can be conveniently inputted by the user.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://sephora-training-data/offers.csv',
        help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # step 1b: pass the configurations into the Pipeline object.
    pipeline_options = PipelineOptions(pipeline_args)
    p = beam.Pipeline(options=pipeline_options)

    # step 2: define the steps that will be run by the Pipeline object.

    (p
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
     | "print" >> beam.Map(print))

    # step 3: run the Pipeline
    result = p.run()
    result.wait_until_finish()


class OfferImporterPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            required=False,
            help='GCS path to the source csv containing the offers',
            default='gs://sephora-training-data/offers.csv')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
