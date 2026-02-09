from collections import defaultdict
from datetime import datetime, timedelta, timezone

# Specify your local timezone offset from UTC here, e.g., UTC+2 hours
LOCAL_TIMEZONE_OFFSET = timedelta(hours=-5)

def parse_time(timestamp):
    # Parse the timestamp and set it as UTC
    utc_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    # Convert UTC to local time
    local_time = utc_time.astimezone(timezone(LOCAL_TIMEZONE_OFFSET))
    return local_time


def modulation_diagnosis(data):
    # Group the data by modType and centerFrequency_Hz
    grouped_data_list = defaultdict(list)

    for entry in data['upstreamChannelCapacityHistory']:
        mod_type = entry['modType']
        center_freq = entry['centerFrequency_Hz']
        grouped_data_list[(mod_type, center_freq)].append(entry)

    # Initialize grouped data
    grouped_data = defaultdict(lambda: {
        'max_utilize': 0,
        'start_time': None,
        'end_time': None,
        'total_time': timedelta()
    })

    # Process each group
    for key, entries in grouped_data_list.items():
        # Sort entries by timestamp to ensure chronological order
        entries.sort(key=lambda e: parse_time(e['timestamp']))
        
        previous_entry = None
        for entry in entries:
            if (entry['modType'] != 'qam64'):
                mod_type = entry['modType']
                center_freq = entry['centerFrequency_Hz']
                timestamp = parse_time(entry['timestamp'])
                channel_utilize = entry['channelCapUtilize']

                # Update the start and end times
                if grouped_data[key]['start_time'] is None:
                    grouped_data[key]['start_time'] = timestamp

                # Update maximum utilization
                if channel_utilize > grouped_data[key]['max_utilize']:
                    grouped_data[key]['max_utilize'] = channel_utilize

                # Calculate total time based on the difference between entries
                if previous_entry:
                    time_diff = timestamp - parse_time(previous_entry['timestamp'])
                    time_diff_hours = time_diff.total_seconds() / 3600
                    if time_diff_hours <= 2:
                        grouped_data[key]['end_time'] = timestamp
                        grouped_data[key]['total_time'] += timedelta(hours=time_diff_hours)
                    else:
                        grouped_data[key]['total_time'] += timedelta(hours=1)
                
                if len(entries) == 1:
                    grouped_data[key]['total_time'] += timedelta(hours=1)

                previous_entry = entry

    # Print the formatted table
    affected_elements = []
    all_elements = []
    for (mod_type, center_freq), data in grouped_data.items():

        start_time = data['start_time'].strftime('%Y-%m-%d %H:%M:%S') if data['start_time'] else 'N/A'
        end_time = data['end_time'].strftime('%Y-%m-%d %H:%M:%S') if data['end_time'] else 'N/A'
        total_time_hours = data['total_time'].total_seconds() / 3600  # Convert total time to hours
        center_freq_mhz = center_freq / 1_000_000
        all_elements.append({"frequency": center_freq_mhz, "mod_type": mod_type, "max_utilize": data['max_utilize'], "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
        if total_time_hours >= 5:
            affected_elements.append({"frequency": center_freq_mhz, "mod_type": mod_type, "max_utilize": data['max_utilize'], "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    
    
    return { "affected_elements": affected_elements, "all_elements": all_elements }

def modulation_diagnosis_resume(data):
    # Diccionario para guardar el último valor por canal
    grouped_data = {}

    for entry in data['upstreamChannelCapacityHistory']:
        mod_type = entry['modType']
        center_freq = entry['centerFrequency_Hz']
        channel_utilize = entry['channelCapUtilize']
        timestamp = entry['timestamp']

        key = (center_freq)

        # Sobrescribimos siempre, así queda el último valor de esa frecuencia
        grouped_data[key] = {
            "frequency": center_freq / 1_000_000,
            "mod_type": mod_type,
            "max_utilize": channel_utilize
        }

    # Pasamos a lista de resultados
    results = list(grouped_data.values())
    return results