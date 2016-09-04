__author__ = 'bcarson'

import calendar,time
from datetime import datetime, timedelta

import os,sys
import xively
import requests

import numpy
from scipy.integrate import simps


# Input settings
XIVELY_FEED_ID = os.environ["XIVELY_FEED_ID"]
XIVELY_API_KEY = os.environ["XIVELY_API_KEY"]

xively_api = xively.XivelyAPIClient(XIVELY_API_KEY)

# Data retrieval interval, in seconds
INTERVAL = 300

# Output settings
PVOUTPUT_SYSTEM_ID = os.environ["PVOUTPUT_SYSTEM_ID"]
PVOUTPUT_API_KEY = os.environ["PVOUTPUT_API_KEY"]
PVOUTPUT_UPLOAD_ENDPOINT = "http://pvoutput.org/service/r2/addoutput.jsp"

UTC_OFFSET_TIMEDELTA = datetime.utcnow() - datetime.now()

THRESHOLD = 110

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

def upload_pvoutput_data( date, max_watts, max_watts_time, watt_hours_generated, consumption ):
    headers = { "X-Pvoutput-Apikey" : PVOUTPUT_API_KEY,
                "X-Pvoutput-SystemId": PVOUTPUT_SYSTEM_ID }

    parameters = { "d": date.strftime("%Y%m%d"),
                   "g": str(watt_hours_generated),
                   "pp": str(max_watts),
                   "pt": max_watts_time.strftime("%H:%M"),
                   "c" : str(consumption) }

    for i in range(0,5):
        result = requests.post(PVOUTPUT_UPLOAD_ENDPOINT, data=parameters, headers=headers)
        print("PV Output response: %s" % result.text)
        if result.status_code == requests.codes.ok:
            return True
        else:
            sleep(30)

    return False

def process_day(day):
    start_time = day + UTC_OFFSET_TIMEDELTA
    end_time = start_time + timedelta(days=1)

    print("Retrieving feed data between %s and %s" % (str(start_time), str(end_time)))

    feed = xively_api.feeds.get(XIVELY_FEED_ID, start = start_time, end = end_time)

    temperature_datastream = feed.datastreams.get("0", start = start_time, end = end_time, limit = 1000, interval_type = "discrete", interval = INTERVAL)
    watts_datastream = feed.datastreams.get("1", start = start_time, end = end_time, limit = 1000, interval_type = "discrete", interval = INTERVAL)
    consumed_datastream = feed.datastreams.get("2", start = start_time, end = end_time, limit = 1000, interval_type = "discrete", interval = INTERVAL)

    # Filter the data points, as the device is a flow meter (i.e. when not generating power, it is measuring draw).
    filtered_watts_points = [point for point in watts_datastream.datapoints if float(point.value) > THRESHOLD]

    # Find the point of maximum power generation.
    max_watts_point = get_maximum_datapoint(filtered_watts_points)
    max_watts_time = datetime.fromtimestamp(time.mktime(max_watts_point.at.timetuple())) - UTC_OFFSET_TIMEDELTA

    # Find the point of the highest temperature.
    max_temperature_point = get_maximum_datapoint(temperature_datastream.datapoints)
    max_temperature_time = datetime.fromtimestamp(time.mktime(max_temperature_point.at.timetuple())) - UTC_OFFSET_TIMEDELTA

    # Process power consumption.
    max_consumption_point = get_maximum_datapoint(consumed_datastream.datapoints)
    max_consumption_time = datetime.fromtimestamp(time.mktime(max_consumption_point.at.timetuple())) - UTC_OFFSET_TIMEDELTA
    total_consumption = calculate_area_under_curve(consumed_datastream.datapoints)

    watt_hours = calculate_area_under_curve(filtered_watts_points)

    print("Watt hours for %s to %s: %.2f kWh" % (start_time - UTC_OFFSET_TIMEDELTA, end_time - UTC_OFFSET_TIMEDELTA, watt_hours / 1000))
    print("Maximum power generation was %s W at %s" % (max_watts_point.value, max_watts_time))
    print("Maximum temperature was %s degrees at %s" % (max_temperature_point.value, max_temperature_time))
    print("Total power consumption was %.2f kWh (maximum: %.2f W at %s)" % (total_consumption / 1000, float(max_consumption_point.value), max_consumption_time))

    return upload_pvoutput_data( start_time + timedelta(hours=12) - UTC_OFFSET_TIMEDELTA, int(max_watts_point.value), max_watts_time, int(watt_hours), int(total_consumption))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        now = datetime.now() - timedelta(days=1)
        start_date = datetime(now.year, now.month, now.day)
        end_date = start_date + timedelta(days=1)
    else:
        start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")

    number_of_days = (end_date - start_date).days

    for i in xrange(0, number_of_days):
        process_day(start_date)
        start_date += timedelta(days=1)

