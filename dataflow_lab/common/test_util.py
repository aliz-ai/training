import time
from apache_beam.testing.util import BeamAssertException


def live_offer_stat_event(account_id, offer_id, late_secs=0):
    return "%d %d" % (account_id, offer_id) if late_secs == 0 else "%d %d %d" % (account_id, offer_id, late_secs)


def live_offer_stat_record(offer_id, count, window_start):
    return {
        'offer_id': str(offer_id),
        'count': count,
        'window_start': window_start,
        'window_end': window_start + 60,
        'created_at': str(int(time.time()))
    }


def equal_to_with_timestamp_tolerance(expected, timestamp_tolerance_secs=10):
    expected = list(expected)

    def _tuple_without_timestamp(item):
        return (item['offer_id'], item['count'], item['window_start'], item['window_end'])

    def _timestamp_range(timestamp):
        return xrange(timestamp - timestamp_tolerance_secs, timestamp + timestamp_tolerance_secs + 1)

    def _itemEqual(exp, act):
        return _tuple_without_timestamp(exp) == _tuple_without_timestamp(act) and \
               int(act['created_at']) in _timestamp_range(int(exp['created_at']))

    def _equal(actual):
        sorted_expected = sorted(expected)
        sorted_actual = sorted(actual)

        for idx, act in enumerate(sorted_actual):
            exp = sorted_expected[idx]
            if not _itemEqual(exp, act):
                raise BeamAssertException('Failed to assert item: %r == %r in lists %r == %r' % (exp, act, sorted_expected, sorted_actual))

    return _equal
