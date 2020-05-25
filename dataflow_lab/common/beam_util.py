import logging

import apache_beam as beam
import datetime
import pytz
from apache_beam.utils import timestamp


class WithTimestamp(beam.DoFn):

    def __init__(self, timestamp_extractor_function):
        self.timestamp_extractor_function = timestamp_extractor_function

    def process(self, element, *args, **kwargs):
        create_time = self.timestamp_extractor_function(element)
        yield beam.window.TimestampedValue(element, create_time)


class LoggerDoFn(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, *args, **kwargs):
        logging.info("Element: %s, Timestamp: %s, Window: %s", str(element), str(timestamp), str(window))
        yield element


def parse_beam_timestamp_from_string(string):
    parsed_datetime = datetime.datetime.strptime(string, '%Y-%m-%d %H:%M:%S.%f %Z')
    parsed_datetime = pytz.utc.localize(parsed_datetime)
    return timestamp.Timestamp.from_utc_datetime(parsed_datetime)


def parse_beam_timestamp_from_datetime(string):
    parsed_datetime = datetime.datetime.strptime(string, '%Y-%m-%d %H:%M:%S.%f %Z')
    parsed_datetime = pytz.utc.localize(parsed_datetime)
    return timestamp.Timestamp.from_utc_datetime(parsed_datetime)