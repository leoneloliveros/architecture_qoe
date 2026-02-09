from collections import defaultdict
from datetime import datetime, timedelta, timezone
import requests
import time
import concurrent.futures

LOCAL_TIMEZONE_OFFSET = timedelta(hours=-5)

def parse_time(timestamp):
    utc_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    local_time = utc_time.astimezone(timezone(LOCAL_TIMEZONE_OFFSET))
    return local_time

API_URL_CENTRO = "https://100.123.88.85/pathtrak/api/"
API_URL_REGIONAL = "https://100.123.88.84/pathtrak/api/"
def analyze_affectation(data, name):
    affected_macs = [entry for entry in data if entry['total_time_hours'] >= 2.5]
    #average time .
    if not affected_macs:
        # print(f"No hay MACs con afectación mayor a 2.5 horas en {name}.")
        return {
            'mac_count': 0,
            'peaks': []
        }
    # Calculate total hours and count of MACs
    
    total_hours = sum(entry['total_time_hours'] for entry in affected_macs)
    


    mac_count = len(affected_macs)


    date_ranges = []
    for entry in affected_macs:
        date_ranges.append({
            'mac': entry['mac'],
            'start_time': entry['start_time'],
            'end_time': entry['end_time'],
            'total_time_hours': entry['total_time_hours']
        })

    peaks = [entry for entry in date_ranges if entry['total_time_hours'] > 2.5]

    # print(f"Total MACs con afectación mayor a 2.5 horas: {mac_count}")
    # print(f"Tiempo Promedio con afectación mayor a 2.5 horas: {total_hours / mac_count:.2f} horas")
    # print(f"\nRangos de fechas con picos de afectación ({name}):")
    # for peak in date_ranges:
        # print(f"MAC: {peak['mac']}, Inicio: {peak['start_time']}, Fin: {peak['end_time']}, Horas afectadas: {peak['total_time_hours']:.2f}")

    return {
        'mac_count': mac_count,
        'peaks': peaks,
        'average_time': total_hours / mac_count if mac_count > 0 else 0,
        'data': data
    }
def modulation_diagnosis(data):
    grouped_data_list = data
    metrics = {
        'uccwe': 0,
        'ccwe': 0,
        'snr': 0,
        'uccwe_dw': 0,
        'ccwe_dw': 0,
        'snr_dw': 0
    }
    counters_met = {
        'uccwe': 0,
        'ccwe': 0,
        'snr': 0,
        'uccwe_dw': 0,
        'ccwe_dw': 0,
        'snr_dw': 0
    }
    grouped_data = defaultdict(lambda: {
        'start_time': None,
        'end_time': None,
        'total_time': timedelta()
    })
    grouped_data_uccwe = defaultdict(lambda: {
        'start_time': None,
        'end_time': None,
        'total_time': timedelta()
    })
    grouped_data_ccwe = defaultdict(lambda: {
        'start_time': None,
        'end_time': None,
        'total_time': timedelta()
    })
    grouped_data_snr = defaultdict(lambda: {
        'start_time': None,
        'end_time': None,
        'total_time': timedelta()
    })
    grouped_data_uccwe_dw = defaultdict(lambda: {
        'start_time': None,
        'end_time': None,
        'total_time': timedelta()
    })
    grouped_data_ccwe_dw = defaultdict(lambda: {
        'start_time': None,
        'end_time': None,
        'total_time': timedelta()
    })
    grouped_data_snr_dw = defaultdict(lambda: {
        'start_time': None,
        'end_time': None,
        'total_time': timedelta()
    })
    # # print(f"Total entries to process: {len(grouped_data_list)}")
    for key, entries in grouped_data_list.items():
        mac_address = key
        entries.sort(key=lambda e: parse_time(e['timestamp']))
        key = mac_address
        
        previous_entry = None
        previous_entry_uccwe = None
        previous_entry_ccwe = None
        previous_entry_snr = None
        previous_entry_uccwe_dw = None
        previous_entry_ccwe_dw = None
        previous_entry_snr_dw = None

        for entry in entries:
            cumple_condicion = False
            if 'uccwe' in entry:
                if isinstance(entry['uccwe'], (int, float)):
                    metrics['uccwe'] += entry['uccwe']
                    counters_met['uccwe'] += 1
                    if entry['uccwe'] >= 1:
                        cumple_condicion = True
                        timestamp_uccwe = parse_time(entry['timestamp'])

                        if grouped_data_uccwe[key]['start_time'] is None:
                            grouped_data_uccwe[key]['start_time'] = timestamp_uccwe

                        if previous_entry_uccwe:
                            time_diff_uccwe = timestamp_uccwe - parse_time(previous_entry_uccwe['timestamp'])
                            time_diff_hours_uccwe = time_diff_uccwe.total_seconds() / 3600
                            if time_diff_hours_uccwe <= 0.50:
                                grouped_data_uccwe[key]['end_time'] = timestamp_uccwe
                                grouped_data_uccwe[key]['total_time'] += timedelta(hours=time_diff_hours_uccwe)
                            else:
                                grouped_data_uccwe[key]['total_time'] += timedelta(hours=0.25)
                        else:                    
                            grouped_data_uccwe[key]['end_time'] = timestamp_uccwe
                            grouped_data_uccwe[key]['total_time'] += timedelta(hours=0)

                        if len(entries) == 1:
                            grouped_data_uccwe[key]['start_time'] = timestamp_uccwe
                            grouped_data_uccwe[key]['total_time'] += timedelta(hours=0.25)

                        previous_entry_uccwe = entry
                    else:
                        if previous_entry_uccwe:
                            previous_entry_uccwe = None

            if 'ccwe' in entry:
                if isinstance(entry['ccwe'], (int, float)):
                    metrics['ccwe'] += entry['ccwe']
                    counters_met['ccwe'] += 1
                    if entry['ccwe'] >= 10:
                        cumple_condicion = True
                        timestamp_ccwe = parse_time(entry['timestamp'])

                        if grouped_data_ccwe[key]['start_time'] is None:
                            grouped_data_ccwe[key]['start_time'] = timestamp_ccwe

                        if previous_entry_ccwe:
                            time_diff_ccwe = timestamp_ccwe - parse_time(previous_entry_ccwe['timestamp'])
                            time_diff_hours_ccwe = time_diff_ccwe.total_seconds() / 3600
                            if time_diff_hours_ccwe <= 0.50:
                                grouped_data_ccwe[key]['end_time'] = timestamp_ccwe
                                grouped_data_ccwe[key]['total_time'] += timedelta(hours=time_diff_hours_ccwe)
                            else:
                                grouped_data_ccwe[key]['total_time'] += timedelta(hours=0.25)
                        else:                    
                            grouped_data_ccwe[key]['end_time'] = timestamp_ccwe
                            grouped_data_ccwe[key]['total_time'] += timedelta(hours=0)

                        if len(entries) == 1:
                            grouped_data_ccwe[key]['start_time'] = timestamp_ccwe
                            grouped_data_ccwe[key]['total_time'] += timedelta(hours=0.25)

                        previous_entry_ccwe = entry
                    else:
                        if previous_entry_ccwe:
                            previous_entry_ccwe = None

            if 'snr' in entry:
                if isinstance(entry['snr'], (int, float)):
                    metrics['snr'] += entry['snr']
                    counters_met['snr'] += 1
                    if 0 <= entry['snr'] <= 28:
                        cumple_condicion = True
                        timestamp_snr = parse_time(entry['timestamp'])

                        if grouped_data_snr[key]['start_time'] is None:
                            grouped_data_snr[key]['start_time'] = timestamp_snr

                        if previous_entry_snr:
                            time_diff_snr = timestamp_snr - parse_time(previous_entry_snr['timestamp'])
                            time_diff_hours_snr = time_diff_snr.total_seconds() / 3600
                            if time_diff_hours_snr <= 0.50:
                                grouped_data_snr[key]['end_time'] = timestamp_snr
                                grouped_data_snr[key]['total_time'] += timedelta(hours=time_diff_hours_snr)
                            else:
                                grouped_data_snr[key]['total_time'] += timedelta(hours=0.25)
                        else:                    
                            grouped_data_snr[key]['end_time'] = timestamp_snr
                            grouped_data_snr[key]['total_time'] += timedelta(hours=0)

                        if len(entries) == 1:
                            grouped_data_snr[key]['start_time'] = timestamp_snr
                            grouped_data_snr[key]['total_time'] += timedelta(hours=0.25)

                        previous_entry_snr = entry
                    else:
                        if previous_entry_snr:
                            previous_entry_snr = None

            if 'snr_dw' in entry:
                if isinstance(entry['snr_dw'], (int, float)):
                    metrics['snr_dw'] += entry['snr_dw']
                    counters_met['snr_dw'] += 1
                    if entry['snr_dw'] <= 32:
                        
                        cumple_condicion = True
                        timestamp_snr_dw = parse_time(entry['timestamp'])

                        if grouped_data_snr_dw[key]['start_time'] is None:
                            grouped_data_snr_dw[key]['start_time'] = timestamp_snr_dw

                        if previous_entry_snr_dw:
                            time_diff_snr_dw = timestamp_snr_dw - parse_time(previous_entry_snr_dw['timestamp'])
                            time_diff_hours_snr_dw = time_diff_snr_dw.total_seconds() / 3600
                            if time_diff_hours_snr_dw <= 0.50:
                                grouped_data_snr_dw[key]['end_time'] = timestamp_snr_dw
                                grouped_data_snr_dw[key]['total_time'] += timedelta(hours=time_diff_hours_snr_dw)
                            else:
                                grouped_data_snr_dw[key]['total_time'] += timedelta(hours=0.25)
                        else:                    
                            grouped_data_snr_dw[key]['end_time'] = timestamp_snr_dw
                            grouped_data_snr_dw[key]['total_time'] += timedelta(hours=0)

                        if len(entries) == 1:
                            grouped_data_snr_dw[key]['start_time'] = timestamp_snr_dw
                            grouped_data_snr_dw[key]['total_time'] += timedelta(hours=0.25)

                        previous_entry_snr_dw = entry
                    else:
                        if previous_entry_snr_dw:
                            previous_entry_snr_dw = None

            if 'ccwe_dw' in entry:
                if isinstance(entry['ccwe_dw'], (int, float)):
                    metrics['ccwe_dw'] += entry['ccwe_dw']
                    counters_met['ccwe_dw'] += 1
                    if entry['ccwe_dw'] >= 10:
                        cumple_condicion = True
                        timestamp_ccwe_dw = parse_time(entry['timestamp'])

                        if grouped_data_ccwe_dw[key]['start_time'] is None:
                            grouped_data_ccwe_dw[key]['start_time'] = timestamp_ccwe_dw

                        if previous_entry_ccwe_dw:
                            time_diff_ccwe_dw = timestamp_ccwe_dw - parse_time(previous_entry_ccwe_dw['timestamp'])
                            time_diff_hours_ccwe_dw = time_diff_ccwe_dw.total_seconds() / 3600
                            if time_diff_hours_ccwe_dw <= 0.50:
                                grouped_data_ccwe_dw[key]['end_time'] = timestamp_ccwe_dw
                                grouped_data_ccwe_dw[key]['total_time'] += timedelta(hours=time_diff_hours_ccwe_dw)
                            else:
                                grouped_data_ccwe_dw[key]['total_time'] += timedelta(hours=0.25)
                        else:                    
                            grouped_data_ccwe_dw[key]['end_time'] = timestamp_ccwe_dw
                            grouped_data_ccwe_dw[key]['total_time'] += timedelta(hours=0)

                        if len(entries) == 1:
                            grouped_data_ccwe_dw[key]['start_time'] = timestamp_ccwe_dw
                            grouped_data_ccwe_dw[key]['total_time'] += timedelta(hours=0.25)

                        previous_entry_ccwe_dw = entry
                    else:
                        if previous_entry_ccwe_dw:
                            previous_entry_ccwe_dw = None

            if 'uccwe_dw' in entry:
                
                if isinstance(entry['uccwe_dw'], (int, float)):
                    metrics['uccwe_dw'] += entry['uccwe_dw']
                    counters_met['uccwe_dw'] += 1
                    if entry['uccwe_dw'] >= 1:
                        cumple_condicion = True
                        timestamp_uccwe_dw = parse_time(entry['timestamp'])
                        if grouped_data_uccwe_dw[key]['start_time'] is None:
                            grouped_data_uccwe_dw[key]['start_time'] = timestamp_uccwe_dw
                        if previous_entry_uccwe_dw:
                            time_diff_uccwe_dw = timestamp_uccwe_dw - parse_time(previous_entry_uccwe_dw['timestamp'])
                            time_diff_hours_uccwe_dw = time_diff_uccwe_dw.total_seconds() / 3600
                            if time_diff_hours_uccwe_dw <= 0.50:
                                grouped_data_uccwe_dw[key]['end_time'] = timestamp_uccwe_dw
                                grouped_data_uccwe_dw[key]['total_time'] += timedelta(hours=time_diff_hours_uccwe_dw)
                            else:
                                grouped_data_uccwe_dw[key]['total_time'] += timedelta(hours=0.25)
                        else: 
                            grouped_data_uccwe_dw[key]['end_time'] = timestamp_uccwe_dw
                            grouped_data_uccwe_dw[key]['total_time'] += timedelta(hours=0)
                        if len(entries) == 1:
                            grouped_data_uccwe_dw[key]['start_time'] = timestamp_uccwe_dw
                            grouped_data_uccwe_dw[key]['total_time'] += timedelta(hours=0.25)
                        previous_entry_uccwe_dw = entry
                    else:
                        if previous_entry_uccwe_dw:
                            previous_entry_uccwe_dw = None
            
            if cumple_condicion:   
                timestamp = parse_time(entry['timestamp'])

                if grouped_data[key]['start_time'] is None:
                    grouped_data[key]['start_time'] = timestamp

                if previous_entry:
                    time_diff = timestamp - parse_time(previous_entry['timestamp'])
                    time_diff_hours = time_diff.total_seconds() / 3600
                    if time_diff_hours <= 0.50:
                        grouped_data[key]['end_time'] = timestamp
                        grouped_data[key]['total_time'] += timedelta(hours=time_diff_hours)
                    else:
                        grouped_data[key]['total_time'] += timedelta(hours=0.25)
                else:                    
                    grouped_data[key]['end_time'] = timestamp
                    grouped_data[key]['total_time'] += timedelta(hours=0)

                if len(entries) == 1:
                    grouped_data[key]['start_time'] = timestamp
                    grouped_data[key]['total_time'] += timedelta(hours=0.25)

                previous_entry = entry
            else:
                if previous_entry:
                    previous_entry = None
    # Print the formatted table
    affected_elements = []
    affected_elements_uccwe = []
    affected_elements_ccwe = []
    affected_elements_snr = []
    affected_elements_uccwe_dw = []
    affected_elements_ccwe_dw = []
    affected_elements_snr_dw = []
    all_elements = []
    # for (mod_type), data in grouped_data.items():

    #     start_time = data['start_time'].strftime('%Y-%m-%d %H:%M:%S') if data['start_time'] else 'N/A'
    #     end_time = data['end_time'].strftime('%Y-%m-%d %H:%M:%S') if data['end_time'] else 'N/A'
    #     total_time_hours = data['total_time'].total_seconds() / 3600  # Convert total time to hours
    #     all_elements.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    #     if total_time_hours >= 2.5:
    #         affected_elements.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    for (mod_type), data in grouped_data_uccwe.items():

        start_time = data['start_time'].strftime('%Y-%m-%d %H:%M:%S') if data['start_time'] else 'N/A'
        end_time = data['end_time'].strftime('%Y-%m-%d %H:%M:%S') if data['end_time'] else 'N/A'
        total_time_hours = data['total_time'].total_seconds() / 3600  # Convert total time to hours
        all_elements.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
        if total_time_hours >= 2.5:
            affected_elements_uccwe.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    for (mod_type), data in grouped_data_ccwe.items():

        start_time = data['start_time'].strftime('%Y-%m-%d %H:%M:%S') if data['start_time'] else 'N/A'
        end_time = data['end_time'].strftime('%Y-%m-%d %H:%M:%S') if data['end_time'] else 'N/A'
        total_time_hours = data['total_time'].total_seconds() / 3600  # Convert total time to hours
        all_elements.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
        if total_time_hours >= 2.5:
            affected_elements_ccwe.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    for (mod_type), data in grouped_data_snr.items():

        start_time = data['start_time'].strftime('%Y-%m-%d %H:%M:%S') if data['start_time'] else 'N/A'
        end_time = data['end_time'].strftime('%Y-%m-%d %H:%M:%S') if data['end_time'] else 'N/A'
        total_time_hours = data['total_time'].total_seconds() / 3600  # Convert total time to hours
        all_elements.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
        if total_time_hours >= 2.5:
            affected_elements_snr.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    for (mod_type), data in grouped_data_uccwe_dw.items():

        start_time = data['start_time'].strftime('%Y-%m-%d %H:%M:%S') if data['start_time'] else 'N/A'
        end_time = data['end_time'].strftime('%Y-%m-%d %H:%M:%S') if data['end_time'] else 'N/A'
        total_time_hours = data['total_time'].total_seconds() / 3600  # Convert total time to hours
        all_elements.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
        if total_time_hours >= 2.5:
            affected_elements_uccwe_dw.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    for (mod_type), data in grouped_data_ccwe_dw.items():

        start_time = data['start_time'].strftime('%Y-%m-%d %H:%M:%S') if data['start_time'] else 'N/A'
        end_time = data['end_time'].strftime('%Y-%m-%d %H:%M:%S') if data['end_time'] else 'N/A'
        total_time_hours = data['total_time'].total_seconds() / 3600  # Convert total time to hours
        all_elements.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
        if total_time_hours >= 2.5:
            affected_elements_ccwe_dw.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    for (mod_type), data in grouped_data_snr_dw.items():

        start_time = data['start_time'].strftime('%Y-%m-%d %H:%M:%S') if data['start_time'] else 'N/A'
        end_time = data['end_time'].strftime('%Y-%m-%d %H:%M:%S') if data['end_time'] else 'N/A'
        total_time_hours = data['total_time'].total_seconds() / 3600  # Convert total time to hours
        all_elements.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
        if total_time_hours >= 2.5:
            affected_elements_snr_dw.append({"mac": mod_type, "start_time": start_time, "end_time": end_time, "total_time_hours": total_time_hours })
    
    metrics_prom = {
        "uccwe": metrics['uccwe'] / counters_met['uccwe'],
        "ccwe": metrics['ccwe'] / counters_met['ccwe'],
        "snr": metrics['snr'] / counters_met['snr'],
        "uccwe_dw": metrics['uccwe_dw'] / counters_met['uccwe_dw'],
        "ccwe_dw": metrics['ccwe_dw'] / counters_met['ccwe_dw'],
        "snr_dw": metrics['snr_dw'] / counters_met['snr_dw'],
    }
    return { "metrics": metrics_prom, "affected_elements": affected_elements, "all_elements": all_elements, "affected_elements_uccwe": affected_elements_uccwe, "affected_elements_ccwe": affected_elements_ccwe, "affected_elements_snr": affected_elements_snr, "affected_elements_uccwe_dw": affected_elements_uccwe_dw, "affected_elements_ccwe_dw": affected_elements_ccwe_dw, "affected_elements_snr_dw": affected_elements_snr_dw}

def history_KPIS_main(region, nodeId):
    try:
        # print(f"Starting process for region: {region}, nodeId: {nodeId}")
        current_datetime = datetime.now()
        datetime_minus_1_day = current_datetime - timedelta(days=0) + timedelta(hours=5)
        datetime_minus_2_day = current_datetime - timedelta(days=1) + timedelta(hours=5)
        datetime_minus_3_day = current_datetime - timedelta(days=2) + timedelta(hours=5)
        prev_1_day = datetime_minus_1_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        prev_2_day = datetime_minus_2_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        prev_3_day = datetime_minus_3_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems'
            base_url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/metric/history'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems'
            base_url = f'{API_URL_REGIONAL}node/{nodeId}/qoe/metric/history'
        qoe_1_response =  requests.get(f"{base_url}?startdatetime={prev_1_day}&sampleResponse=false", verify=False)
        qoe_1_response.raise_for_status()
        qoe_data_1 = qoe_1_response.json()
        # print(f"Data fetched for {len(qoe_data_1)} entries from the last 24 hours.")

        grouped_data_list = defaultdict(list)

        def fetch(entry):
            response = requests.get(f"{url}?startdatetime={entry['timestamp']}", verify=False)
            response.raise_for_status()
            resp = response.json()
            date_test = entry['timestamp']
            for x in resp:
                new_record = {
                    'uccwe': 'No Data',  
                    'ccwe': 'No Data',  
                    'snr': 'No Data',
                    'tx_up': 'No Data',
                    'rx_dw': 'No Data',
                    'uccwe_dw': 'No Data',
                    'ccwe_dw': 'No Data',
                    'snr_dw': 'No Data',
                    'timestamp': date_test
                }
                new_record['timestamp'] = date_test
                for y in x["usChResponse"]:
                    uccwe_val = y.get('uccwe')
                    if uccwe_val is not None:
                        if new_record['uccwe'] == 'No Data':
                            new_record['uccwe'] = uccwe_val
                        else:
                            new_record['uccwe'] = max(new_record['uccwe'], uccwe_val)

                    ccwe_val = y.get('ccwe')
                    if ccwe_val is not None:
                        if new_record['ccwe'] == 'No Data':
                            new_record['ccwe'] = ccwe_val
                        else:
                            new_record['ccwe'] = max(new_record['ccwe'], ccwe_val)

                    snr_val = y.get('snr')
                    if snr_val is not None:
                        if new_record['snr'] == 'No Data':
                            new_record['snr'] = snr_val
                        else:
                            new_record['snr'] = min(new_record['snr'], snr_val)

                    if '-' in y.get('frequency', ''):
                        tx_val = y.get('txLevel')
                        if tx_val is not None:
                            new_record['tx_up'] = tx_val
                for y in x["docsisDsChResponse"]:
                    uccwe_dw_val = y.get('uccwe')
                    if uccwe_dw_val is not None:
                        if new_record['uccwe_dw'] == 'No Data':
                            new_record['uccwe_dw'] = uccwe_dw_val
                        else:
                            new_record['uccwe_dw'] = max(new_record['uccwe_dw'], uccwe_dw_val)

                    ccwe_dw_val = y.get('ccwe')
                    if ccwe_dw_val is not None:
                        if new_record['ccwe_dw'] == 'No Data':
                            new_record['ccwe_dw'] = ccwe_dw_val
                        else:
                            new_record['ccwe_dw'] = max(new_record['ccwe_dw'], ccwe_dw_val)

                    snr_dw_val = y.get('snr')
                    if snr_dw_val is not None:
                        if new_record['snr_dw'] == 'No Data':
                            new_record['snr_dw'] = snr_dw_val
                        else:
                            new_record['snr_dw'] = min(new_record['snr_dw'], snr_dw_val)

                    if '-' in y.get('frequency', ''):
                        level_val = y.get('level')
                        if level_val is not None:
                            new_record['rx_dw'] = level_val

                grouped_data_list[x['mac']].append(new_record)   
            return dict(grouped_data_list)
        
        grouped_data_list = defaultdict(list)
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(fetch, qoe_data_1))
            # with open("result.json", "a", encoding="utf-8") as file:
            #     file.write(f"{results}")
        # print(f"Total requests made: {len(results)}")
        dat = modulation_diagnosis(results[0])        
        uccwe = analyze_affectation(dat['affected_elements_uccwe'], 'UCCWE')
        ccwe = analyze_affectation(dat['affected_elements_ccwe'], 'CCWE')
        snr = analyze_affectation(dat['affected_elements_snr'], 'SNR')
        uccwe_dw = analyze_affectation(dat['affected_elements_uccwe_dw'], 'UCCWE_DW')
        ccwe_dw = analyze_affectation(dat['affected_elements_ccwe_dw'], 'CCWE_DW')
        snr_dw = analyze_affectation(dat['affected_elements_snr_dw'], 'SNR_DW')
        end_time = time.time()
        # print(f"Tiempo total usando paralelo: {end_time - start_time:.2f} segundos")
        
        return { "metrics": dat['metrics'], "uccwe": uccwe, "ccwe": ccwe, "snr": snr, "uccwe_dw": uccwe_dw, "ccwe_dw": ccwe_dw, "snr_dw": snr_dw }
    except Exception as e:
        print(f"Error in the main process: {e}")
def merge_arrays(*arrays):
    merged = []
    for array in arrays:
        merged.extend(array)
    return merged
# history_KPIS_main('centro', 6403203)