import unittest

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline


class CountDistinctAccounts:
    pass


class TestOfferStatStreamingPipeline(unittest.TestCase):

    def test_windows(self):
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = True

        with TestPipeline(options=pipeline_options) as p:
            pass

    def test_early_trigger(self):
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = True

        with TestPipeline(options=pipeline_options) as p:
            pass

    def test_late_event_ignored(self):
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = True

        with TestPipeline(options=pipeline_options) as p:
            pass
