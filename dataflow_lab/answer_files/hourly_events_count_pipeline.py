from __future__ import absolute_import

import argparse
import logging
import apache_beam as beam

import calendar
from datetime import timedelta

from apache_beam.options.pipeline_options import SetupOptions, StandardOptions, PipelineOptions
from apache_beam.io import BigQueryDisposition
from apache_beam.coders import VarIntCoder
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer

HOURLY_EVENTS_COUNT_SCHEMA = 'timestamp:TIMESTAMP,count:INTEGER'


def run(argv=None):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    hourly_events_count_pipeline_options = pipeline_options.view_as(HourlyEventsCountOptions)

    p = beam.Pipeline(options=pipeline_options)

    p | "Read account offer from PS" >> beam.io.ReadFromPubSub(topic=hourly_events_count_pipeline_options.hourly_events_count_topic) \
    | "Parse message" >> beam.ParDo(PubsubMessageParser()) \
    | "Count and schedule" >> beam.ParDo(CountAndSchedule()) \
    | 'Writing results to BQ' >> beam.io.WriteToBigQuery(table=hourly_events_count_pipeline_options.hourly_events_count_bq_table,
                                                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                                                        schema=HOURLY_EVENTS_COUNT_SCHEMA)

    result = p.run()
    result.wait_until_finish()


class CountAndSchedule(beam.DoFn):

    COUNTER = BagStateSpec('counter', VarIntCoder())
    SCHEDULED_TIMESTAMP = BagStateSpec('nextSchedule', VarIntCoder())
    TIMER = TimerSpec('timer', TimeDomain.WATERMARK)

    def process(self,
                element,
                timestamp=beam.DoFn.TimestampParam,
                timer=beam.DoFn.TimerParam(TIMER),
                counter=beam.DoFn.StateParam(COUNTER),
                next_schedule=beam.DoFn.StateParam(SCHEDULED_TIMESTAMP), *args,
                **kwargs):
        current_count, = list(counter.read()) or [0]
        counter.clear()
        counter.add(current_count + 1)

        event_datetime = timestamp.to_utc_datetime()
        current_hour_end = event_datetime.replace(second=0, microsecond=0) + timedelta(minutes=1)

        next_tick = calendar.timegm(current_hour_end.timetuple())
        timer.set(next_tick)

        next_schedule.clear()
        next_schedule.add(next_tick)

    @on_timer(TIMER)
    def timer_ticked(self,
                     timer=beam.DoFn.TimerParam(TIMER),
                     counter=beam.DoFn.StateParam(COUNTER),
                     next_schedule=beam.DoFn.StateParam(SCHEDULED_TIMESTAMP)):
        print("TICKTICK")
        current_count, = counter.read()
        this_tick, = next_schedule.read()

        next_tick = this_tick + 60

        next_schedule.clear()
        next_schedule.add(next_tick)

        counter.clear()
        counter.add(0)

        timer.clear()
        timer.set(next_tick)

        yield {'count': current_count, 'timestamp': this_tick}


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
