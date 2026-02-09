from collections import defaultdict
from datetime import datetime, timedelta, timezone
import requests

# Specify your local timezone offset from UTC here, e.g., UTC+2 hours
LOCAL_TIMEZONE_OFFSET = timedelta(hours=-5)

def parse_time(timestamp):
    # Parse the timestamp and set it as UTC
    utc_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    # Convert UTC to local time
    local_time = utc_time.astimezone(timezone(LOCAL_TIMEZONE_OFFSET))
    return local_time

API_URL_CENTRO = "https://100.123.88.85/pathtrak/api/"
API_URL_REGIONAL = "https://100.123.88.84/pathtrak/api/"
def modulation_diagnosis(data):
    # Group the data by modType and centerFrequency_Hz
    for entry in data:
        entry["timestamp"] = datetime.strptime(entry["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    data.sort(key=lambda x: x["timestamp"])

    # Variables to track consecutive low QoE
    low_start = None
    low_end = None
    counter = 0

    for entry in data:
        qoe = entry["qoeScore"]
        timestamp = entry["timestamp"]

        if qoe < 60:
            if low_start is None:
                low_start = timestamp
            low_end = timestamp
            counter += 1
        else:
            if counter > 0:
                hours = (low_end - low_start).total_seconds() / 3600
                print(f"Low QoE period: {low_start - timedelta(hours=5)} to {low_end - timedelta(hours=5)} — {hours:.2f} hours")
            # Reset
            low_start = None
            low_end = None
            counter = 0

    if counter > 0:
        hours = (low_end - low_start).total_seconds() / 3600
        print(f"Low QoE period: {low_start - timedelta(hours=5)} to {low_end - timedelta(hours=5)} — {hours:.2f} hours")
def main(region, nodeId):
    try:
        current_datetime = datetime.now()
        datetime_minus_1_day = current_datetime - timedelta(days=0) + timedelta(hours=5)
        datetime_minus_2_day = current_datetime - timedelta(days=1) + timedelta(hours=5)
        datetime_minus_3_day = current_datetime - timedelta(days=2) + timedelta(hours=5)
        prev_1_day = datetime_minus_1_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        prev_2_day = datetime_minus_2_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        prev_3_day = datetime_minus_3_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        if region == 'centro':
            base_url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/metric/history'
        elif region == 'regional':
            base_url = f'{API_URL_REGIONAL}node/{nodeId}/qoe/metric/history'
        qoe_1_response =  requests.get(f"{base_url}?startdatetime={prev_1_day}&sampleResponse=false", verify=False)
        qoe_1_response.raise_for_status()
        qoe_data_1 = qoe_1_response.json()

        qoe_2_response =  requests.get(f"{base_url}?startdatetime={prev_2_day}&sampleResponse=false", verify=False)
        qoe_2_response.raise_for_status()
        qoe_data_2 = qoe_2_response.json()

        qoe_3_response =  requests.get(f"{base_url}?startdatetime={prev_3_day}&sampleResponse=false", verify=False)
        qoe_3_response.raise_for_status()
        qoe_data_3 = qoe_3_response.json()
        
        arr = merge_arrays(qoe_data_1, qoe_data_2, qoe_data_3)
        print(modulation_diagnosis(arr))
    except Exception as e:
        print(f"Error in the main process: {e}")
def merge_arrays(*arrays):
    merged = []
    for array in arrays:
        merged.extend(array)
    return merged
main('centro', 6328193)