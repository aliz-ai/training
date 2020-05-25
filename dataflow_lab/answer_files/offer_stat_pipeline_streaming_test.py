import apache_beam as beam
import unittest

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode

from cheat.offer_stat_pipeline_streaming import PubsubMessageParser, DistinctAccountCount, ConvertStatToBQRow
from common.test_util import live_offer_stat_event, equal_to_with_timestamp_tolerance, live_offer_stat_record


class CountDistinctAccounts(beam.PTransform):

    def expand(self, pcoll):
        return (pcoll
                | "Parse message" >> beam.ParDo(PubsubMessageParser())
                | "Windowing" >> beam.WindowInto(FixedWindows(60),
                                                 trigger=AfterWatermark(
                                                     early=AfterProcessingTime(delay=20)),
                                                 accumulation_mode=AccumulationMode.ACCUMULATING)
                | "WithKeys" >> beam.Map(lambda account_offer: ((account_offer['offer_id']), account_offer))
                | beam.GroupByKey()
                | 'Count distinct accounts' >> beam.ParDo(DistinctAccountCount())
                | 'Map to BQ row' >> beam.ParDo(ConvertStatToBQRow()))


class TestOfferStatStreamingPipeline(unittest.TestCase):

    def test_windows(self):
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = True

        with TestPipeline(options=pipeline_options) as p:
            event_stream = (
                TestStream()
                .advance_watermark_to(0)
                .add_elements([beam.window.TimestampedValue(live_offer_stat_event(account_id=1, offer_id=10), 0)])
                .add_elements([beam.window.TimestampedValue(live_offer_stat_event(account_id=2, offer_id=20), 1)])
                .add_elements([beam.window.TimestampedValue(live_offer_stat_event(account_id=3, offer_id=10), 90)])
                .advance_processing_time(25)
                .add_elements([beam.window.TimestampedValue(live_offer_stat_event(account_id=4, offer_id=10), 119)])
                .advance_watermark_to(60)
                .add_elements([beam.window.TimestampedValue(live_offer_stat_event(account_id=3, offer_id=10), 10)])
                .advance_watermark_to_infinity()
            )

            actual = p | event_stream | CountDistinctAccounts()

            expected = [
                live_offer_stat_record(offer_id=10, count=1, window_start=0),
                live_offer_stat_record(offer_id=20, count=1, window_start=0),
                live_offer_stat_record(offer_id=10, count=1, window_start=0),
                live_offer_stat_record(offer_id=20, count=1, window_start=0),
                live_offer_stat_record(offer_id=10, count=1, window_start=60),
                live_offer_stat_record(offer_id=10, count=2, window_start=60)
            ]

            assert_that(actual, equal_to_with_timestamp_tolerance(
                expected=expected, timestamp_tolerance_secs=2))

    def test_early_trigger(self):
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = True

        with TestPipeline(options=pipeline_options) as p:
            event_stream = (
                TestStream()
                .advance_watermark_to(10)
                .add_elements([live_offer_stat_event(account_id=1, offer_id=1),
                               live_offer_stat_event(
                    account_id=2, offer_id=1),
                    live_offer_stat_event(
                    account_id=3, offer_id=1),
                    live_offer_stat_event(
                    account_id=1, offer_id=2)
                ])
                .advance_processing_time(20)  # should kick early trigger
                # next events' timestamp will default to 20
                .advance_watermark_to(20)
                .add_elements([live_offer_stat_event(account_id=4, offer_id=1),
                               live_offer_stat_event(
                    account_id=2, offer_id=2)
                ])
            )

            actual = p | event_stream | CountDistinctAccounts()

            expected = [
                # speculative for offer 1
                live_offer_stat_record(offer_id=1, count=3, window_start=0),
                # speculative for offer 2
                live_offer_stat_record(offer_id=2, count=1, window_start=0),
                # window end for offer 1
                live_offer_stat_record(offer_id=1, count=4, window_start=0),
                # window end for offer 2
                live_offer_stat_record(offer_id=2, count=2, window_start=0)
            ]

            assert_that(actual, equal_to_with_timestamp_tolerance(
                expected=expected, timestamp_tolerance_secs=2))

    def test_late_event_ignored(self):
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = True

        with TestPipeline(options=pipeline_options) as p:
            event_stream = (
                TestStream()
                .advance_watermark_to(10)
                .add_elements([live_offer_stat_event(account_id=1, offer_id=1),
                               live_offer_stat_event(
                    account_id=2, offer_id=1),
                    live_offer_stat_event(
                    account_id=3, offer_id=1),
                    live_offer_stat_event(
                    account_id=1, offer_id=2)
                ])
                .advance_processing_time(20)  # should kick early trigger
                # next events' timestamp will default to 20
                .advance_watermark_to(20)
                .add_elements([live_offer_stat_event(account_id=4, offer_id=1),
                               live_offer_stat_event(
                    account_id=2, offer_id=2),
                ])
                .advance_watermark_to(65)
                .add_elements([live_offer_stat_event(account_id=5, offer_id=1, late_secs=30),  # late event, should be ignored
                               # next window event
                               live_offer_stat_event(
                    account_id=1, offer_id=1)
                ])
                # kick early trigger on the second window
                .advance_processing_time(80)
                # next events' timestamp will default to 90
                .advance_watermark_to(90)
                .add_elements([live_offer_stat_event(account_id=2, offer_id=1)  # 2nd window after early event
                               ])

            )

            actual = p | event_stream | CountDistinctAccounts()

            expected = [
                # speculative for window 1 offer 1
                live_offer_stat_record(offer_id=1, count=3, window_start=0),
                # speculative for window 1 offer 2
                live_offer_stat_record(offer_id=2, count=1, window_start=0),
                # window end for window 1 offer 1
                live_offer_stat_record(offer_id=1, count=4, window_start=0),
                # window end for window 1 offer 2
                live_offer_stat_record(offer_id=2, count=2, window_start=0),
                # speculative for window 2 offer 1
                live_offer_stat_record(offer_id=1, count=1, window_start=60),
                # window end for window 2 offer 1
                live_offer_stat_record(offer_id=1, count=2, window_start=60)
            ]

            assert_that(actual, equal_to_with_timestamp_tolerance(
                expected=expected, timestamp_tolerance_secs=2))
