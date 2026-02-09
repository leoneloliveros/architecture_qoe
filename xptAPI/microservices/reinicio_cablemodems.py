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
def clients_list(region, nodeId):
    #date now in this format ?startdatetime=2025-10-15T10:23:00.000Z
    start_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    try:
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems?startdatetime={start_datetime}'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems?startdatetime={start_datetime}'

        def fetch():
            response = requests.get(f"{url}", verify=False)
            response.raise_for_status()
            resp = response.json()

            mac_data = {}

            for x in resp:
                mac = x['mac']
                mac_data[mac] = {
                    'chronic': False,
                    'impacted': False,
                    'stressed': False,
                    'snr': False,
                    'fec': False,
                    'pl': False
                }
                mac_data[mac]['chronic'] = x.get('chronic', False)
                mac_data[mac]['impacted'] = x.get('impacted', False)
                mac_data[mac]['stressed'] = x.get('stressed', False)
                mac_data[mac]['offline'] = x.get('offline', False)
                mac_data[mac]['regStatus'] = x.get('regStatus', 'No Data')
                

                for y in x["usChResponse"]:
                    if '-' in y.get('frequency', ''):
                        snr_val = y.get('snr')
                        if snr_val is not None:
                            mac_data[mac]['snr'] = snr_val

                        fec_val = y.get('ccwe')
                        if fec_val is not None:
                            mac_data[mac]['fec'] = fec_val

                        pl_val = y.get('uccwe')
                        if pl_val is not None:
                            mac_data[mac]['pl'] = pl_val

            return mac_data
        results = fetch()
        
        return results
    except Exception as e:
        print(f"Error in the main process: {e}")

def reinicio_main(region, nodeId):
    try:
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems'
        grouped_data_list = defaultdict(list)
        mac_addresses = set([])
        mac_addresses_fec = []
        mac_addresses_pl = []
        mac_addresses_fec_dw = []
        mac_addresses_pl_dw = []

        def fetch():
            response = requests.get(f"{url}", verify=False)
            response.raise_for_status()
            resp = response.json()

            mac_data = {}

            for x in resp:
                mac = x['mac']
                mac_data[mac] = {
                    'snr': False,
                    'fec': False,
                    'pl': False
                }

                # for y in x['docsisDsChResponse']:
                #     if '-' in y.get('frequency', ''):
                #         fec_val = y.get('ccwe')
                #         if fec_val is not None and fec_val >= 50:
                #             mac_data[mac]['fec_dw'] = True

                #         pl_val = y.get('uccwe')
                #         if pl_val is not None and pl_val >= 50:
                #             mac_data[mac]['pl_dw'] = True

                for y in x["usChResponse"]:
                    if '-' in y.get('frequency', ''):
                        snr_val = y.get('snr')
                        if snr_val is not None and snr_val < 17:
                            mac_data[mac]['snr'] = True

                        fec_val = y.get('ccwe')
                        if fec_val is not None and fec_val >= 50:
                            mac_data[mac]['fec'] = True

                        pl_val = y.get('uccwe')
                        if pl_val is not None and pl_val >= 50:
                            mac_data[mac]['pl'] = True

            # Ahora filtramos solo las MACs que cumplen con TODAS las condiciones
            matching_macs = [
                mac for mac, conditions in mac_data.items()
                if all(conditions.values())
            ]

            return { "mac": matching_macs }

        grouped_data_list = defaultdict(list)
        start_time = time.time()
        results = fetch()
        # print(len(results))
        #remove duplicated macs, get a list only one mac and the multiple values 
        # results = merge_arrays(results["mac_fec"], results["mac_pl"], results["mac_fec_dw"], results["mac_pl_dw"])
        # results = organizar_registros(results)
        # print(results)
        # results = list({v['mac']: v for v in results}.values())
        # print(f"Tiempo total usando paralelo: {end_time - start_time:.2f} segundos")
        
        return results
    except Exception as e:
        print(f"Error in the main process: {e}")
def merge_arrays(*arrays):
    merged = []
    for array in arrays:
        merged.extend(array)
    return merged
def organizar_registros(data):
    resultado = {}
    
    for item in data:
        mac = item['mac']
        if mac not in resultado:
            resultado[mac] = {"mac": mac, "pl_dw": None, "fec_dw": None, "pl": 0, "fec": 0, "snr": 0}
        
        # Si tiene FEC
        if "snr" in item:
            resultado[mac]["snr"] = item["snr"]
        if "fec_dw" in item:
            resultado[mac]["fec_dw"] = item["fec_dw"]
        # Si tiene PL
        if "pl_dw" in item:
            resultado[mac]["pl_dw"] = item["pl_dw"]
        if "fec" in item:
            resultado[mac]["fec"] = item["fec"]
        
        # Si tiene PL
        if "pl" in item:
            resultado[mac]["pl"] = item["pl"]
    
    
    # Pasar de dict a lista
    return list(resultado.values())


def chronic_macs(region, nodeId):
    try:
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems'
        mac_addresses = []

        def fetch():
            response = requests.get(f"{url}", verify=False)
            response.raise_for_status()
            resp = response.json()

            for x in resp:
                mac = x['mac']
                
                chronic = x.get('chronic', False)
                if chronic:
                    mac_addresses.append(mac)

            return mac_addresses

        results = fetch()
        # print(len(results))
        #remove duplicated macs, get a list only one mac and the multiple values 
        # results = merge_arrays(results["mac_fec"], results["mac_pl"], results["mac_fec_dw"], results["mac_pl_dw"])
        # results = organizar_registros(results)
        # print(results)
        # results = list({v['mac']: v for v in results}.values())
        # print(f"Tiempo total usando paralelo: {end_time - start_time:.2f} segundos")
        
        return results
    except Exception as e:
        print(f"Error in the main process: {e}")
def affected_macs_v1(region, nodeId):
    try:
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems'
        mac_addresses = []

        def fetch():
            response = requests.get(f"{url}", verify=False)
            response.raise_for_status()
            resp = response.json()

            for x in resp:
                mac = x['mac']
                
                chronic = x.get('chronic', False)
                impacted = x.get('impacted', False)
                stressed = x.get('stressed', False)
                if impacted or stressed or chronic:
                    mac_addresses.append(mac)

            return mac_addresses

        results = fetch()
        # print(len(results))
        #remove duplicated macs, get a list only one mac and the multiple values 
        # results = merge_arrays(results["mac_fec"], results["mac_pl"], results["mac_fec_dw"], results["mac_pl_dw"])
        # results = organizar_registros(results)
        # print(results)
        # results = list({v['mac']: v for v in results}.values())
        # print(f"Tiempo total usando paralelo: {end_time - start_time:.2f} segundos")
        
        return results
    except Exception as e:
        print(f"Error in the main process: {e}")    
def affected_macs_v2(region, nodeId):
    try:
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems'
        mac_addresses = []

        def fetch():
            response = requests.get(f"{url}", verify=False)
            response.raise_for_status()
            resp = response.json()

            for x in resp:
                mac = x['mac']
                
                chronic = x.get('chronic', False)
                impacted = x.get('impacted', False)
                stressed = x.get('stressed', False)
                if (impacted and chronic) or (stressed and chronic):
                    mac_addresses.append(mac)

            return mac_addresses

        results = fetch()
        # print(len(results))
        #remove duplicated macs, get a list only one mac and the multiple values 
        # results = merge_arrays(results["mac_fec"], results["mac_pl"], results["mac_fec_dw"], results["mac_pl_dw"])
        # results = organizar_registros(results)
        # print(results)
        # results = list({v['mac']: v for v in results}.values())
        # print(f"Tiempo total usando paralelo: {end_time - start_time:.2f} segundos")
        
        return results
    except Exception as e:
        print(f"Error in the main process: {e}")  
def affected_macs(region, nodeId):
    try:
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems'
        mac_addresses = []

        def fetch():
            response = requests.get(f"{url}", verify=False)
            response.raise_for_status()
            resp = response.json()

            for x in resp:
                mac = x['mac']
                
                chronic = x.get('chronic', False)
                impacted = x.get('impacted', False)
                stressed = x.get('stressed', False)
                if impacted or stressed or chronic:
                    mac_addresses.append(mac)

            return mac_addresses

        results = fetch()
        # print(len(results))
        #remove duplicated macs, get a list only one mac and the multiple values 
        # results = merge_arrays(results["mac_fec"], results["mac_pl"], results["mac_fec_dw"], results["mac_pl_dw"])
        # results = organizar_registros(results)
        # print(results)
        # results = list({v['mac']: v for v in results}.values())
        # print(f"Tiempo total usando paralelo: {end_time - start_time:.2f} segundos")
        
        return results
    except Exception as e:
        print(f"Error in the main process: {e}")