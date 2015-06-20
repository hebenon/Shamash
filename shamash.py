__author__ = 'bcarson'

import calendar,time
from datetime import datetime, timedelta

import os
import xively

import numpy
from scipy.integrate import simps


FEED_ID = os.environ["XIVELY_FEED_ID"]
XIVELY_API_KEY = os.environ["XIVELY_API_KEY"]

xively_api = xively.XivelyAPIClient(XIVELY_API_KEY)

threshold = 100

# A precondition on this is that the datapoints are filtered by the minimum value threshold.
# Calculated as the area under the curve.
def calculate_area_under_curve(datapoints):
    num_points = len(datapoints)

    start_point = calendar.timegm(datapoints[0].at.timetuple())

    yValues = numpy.array([float(point.value) for point in datapoints])
    xValues = numpy.array([float(calendar.timegm(point.at.timetuple()) - start_point) / 3600.0 for point in datapoints])

    return simps(yValues, xValues, even='avg')

def get_maximum_datapoint(dataseries):
    return reduce(lambda x,y: x if float(x.value) > float(y.value) else y, dataseries )

if __name__ == "__main__":
    UTC_OFFSET_TIMEDELTA = datetime.utcnow() - datetime.now()

    start_time = datetime(2015,5,29,0,0,0) + UTC_OFFSET_TIMEDELTA
    end_time = start_time + timedelta(days=1)

    print("Retrieving feed data between %s and %s" % (str(start_time), str(end_time)))

    feed = xively_api.feeds.get(FEED_ID, start=start_time, end=end_time)

    temperature_datastream = feed.datastreams.get("0", start=start_time, end=end_time)
    watts_datastream = feed.datastreams.get("1", start=start_time, end=end_time)
    consumed_datastream = feed.datastreams.get("2", start=start_time, end=end_time)

    # Filter the data points, as the device is a flow meter (i.e. when not generating power, it is measuring draw).
    filtered_watts_points = [point for point in watts_datastream.datapoints if float(point.value) > threshold]

    # Find the point of maximum power generation.
    max_watts_point = get_maximum_datapoint(filtered_watts_points)
    max_watts_time = datetime.fromtimestamp(time.mktime(max_watts_point.at.timetuple())) - UTC_OFFSET_TIMEDELTA

    # Find the point of the highest temperature.
    max_temperature_point = get_maximum_datapoint(temperature_datastream.datapoints)
    max_temperature_time = datetime.fromtimestamp(time.mktime(max_temperature_point.at.timetuple())) - UTC_OFFSET_TIMEDELTA

    # Process power consumption.
    max_consumption_point = get_maximum_datapoint(consumed_datastream.datapoints)
    max_consumption_time = datetime.fromtimestamp(time.mktime(max_consumption_point.at.timetuple())) - UTC_OFFSET_TIMEDELTA
    total_consumption = sum(float(point.value) for point in consumed_datastream.datapoints)

    watt_hours = calculate_area_under_curve(filtered_watts_points) / 1000

    print("Watt hours for %s to %s: %.2f kWh" % (start_time - UTC_OFFSET_TIMEDELTA, end_time - UTC_OFFSET_TIMEDELTA, watt_hours))
    print("Maximum power generation was %s W at %s" % (max_watts_point.value, max_watts_time))
    print("Maximum temperature was %s degrees at %s" % (max_temperature_point.value, max_temperature_time))
    print("Total power consumption was %.2f (maximum: %.2f at %s)" % (total_consumption, float(max_consumption_point.value), max_consumption_time))


