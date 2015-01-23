__author__ = 'bcarson'

import calendar
import os
import xively

from datetime import datetime, timedelta

FEED_ID = os.environ["XIVELY_FEED_ID"]
XIVELY_API_KEY = os.environ["XIVELY_API_KEY"]

xively_api = xively.XivelyAPIClient(XIVELY_API_KEY)

threshold = 100

# A precondition on this is that the datapoints are filtered by the minimum value threshold.
# Calculated as the area under the curve.
def calculate_watthours(datapoints):
    num_points = len(datapoints)

    trapezoid_total = 0

    for i in range(0, num_points - 2):
        trapezoid_total += (float(datapoints[i].value) + float(datapoints[i + 1].value)) / 2.0

    return trapezoid_total * 5.0/60.0

if __name__ == "__main__":
    start_time = datetime.utcnow() - timedelta(days=1)
    end_time = datetime.utcnow()

    print("Retrieving feed data between %s and %s" % (str(start_time), str(end_time)))

    feed = xively_api.feeds.get(FEED_ID, start=start_time, end=end_time)

    temperature_datastream = feed.datastreams.get("0", start=start_time, end=end_time)
    watts_datastream = feed.datastreams.get("1", start=start_time, end=end_time)
    mystery_datastream = feed.datastreams.get("2", start=start_time, end=end_time)

    """
    print("Temperature: %s" % str(temperature_datastream.current_value))
    for point in temperature_datastream.datapoints:
        # Convert from UTC time to local time
        timestamp = calendar.timegm(point.at.timetuple())
        print("%s: %s" % (str(datetime.fromtimestamp(timestamp)), str(point.value)))

    print("Watts: %s" % str(watts_datastream.current_value))
    for point in watts_datastream.datapoints:
        # Convert from UTC time to local time
        timestamp = calendar.timegm(point.at.timetuple())
        print("%s: %s" % (str(datetime.fromtimestamp(timestamp)), str(point.value)))

    print("Mystery: %s" % str(mystery_datastream.current_value))
    for point in mystery_datastream.datapoints:
        # Convert from UTC time to local time
        timestamp = calendar.timegm(point.at.timetuple())
        print("%s: %s" % (str(datetime.fromtimestamp(timestamp)), str(point.value)))
    """

    watt_hours = calculate_watthours([point for point in watts_datastream.datapoints if float(point.value) > threshold])
    print("Watt hours for %s to %s: %f" % (start_time, end_time, watt_hours))