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

def reinicio_main(region, nodeId, nodo_name, qoe, afectacion, total_cm, qoe_hist1, qoe_date1, file_name):
    try:
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems'
            url_channels = f'{API_URL_CENTRO}preeq/node/{nodeId}/channels'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems'
            url_channels = f'{API_URL_REGIONAL}preeq/node/{nodeId}/channels'
        def fetch():
            response = requests.get(f"{url}", verify=False)
            channels_response = requests.get(f"{url_channels}", verify=False)
            # print(f"{channels_response.json()}")
            response.raise_for_status()
            resp = response.json()
            
            # Obtener los datos de los canales
            channels = channels_response.json()  # Los canales vienen en MHz
            
            # Convertimos las frecuencias a MHz para hacer la comparación
            channel_frequencies_mhz = {channel['frequencyMHz'] for channel in channels}
            
            avg_snr = defaultdict(int)  # Promedio de SNR por frecuencia
            count_snr = defaultdict(int)  # Contador de SNR por frecuencia
            cablemodems_by_frequency = defaultdict(int)  # Total de cablemodems por frecuencia
            affected_cablemodems_by_frequency = defaultdict(int)  # Total de cablemodems por frecuencia
            affected_count = 0  # Cantidad de cablemodems afectados (snr < 28)
            
            mac_addresses = set()
            mac_addresses_snr = []
            
            # Iteramos sobre la respuesta de 'usChResponse'
            for x in resp:
                for y in x["usChResponse"]:
                    if '-' not in y.get('frequency', ''):
                        # Convertir la frecuencia de Hz a MHz
                        frequency_mhz = int(y.get('frequency', 0)) / 1000000  # Convertir a MHz
                        
                        # Verificar si la frecuencia está en los canales disponibles
                        if frequency_mhz in channel_frequencies_mhz:
                            snr_val = y.get('snr')
                            
                            if snr_val is not None:
                                avg_snr[frequency_mhz] += snr_val
                                count_snr[frequency_mhz] += 1
                                
                                # Contar los cablemodems por frecuencia
                                cablemodems_by_frequency[frequency_mhz] += 1
                                
                                # Contar los cablemodems afectados (snr < 28)
                                if snr_val < 28:
                                    affected_count += 1
                                    affected_cablemodems_by_frequency[frequency_mhz] += 1
                                    
                                # Almacenar mac y snr para reportes
                                mac_addresses.add(x['mac'])
                                mac_addresses_snr.append({
                                    "mac": x['mac'],
                                    "snr": snr_val,
                                    "freq": frequency_mhz
                                })
                                
                                #Add request to capacity
            
            # Calcular el promedio de SNR por frecuencia
            avg_snr_by_frequency = {
                freq: avg_snr[freq] / count_snr[freq] if count_snr[freq] > 0 else 0
                for freq in avg_snr
            }
            
            # Promedio total del nodo
            total_snr = sum(avg_snr_by_frequency.values())
            total_frequencies = len(avg_snr_by_frequency)
            avg_snr_total_node = total_snr / total_frequencies if total_frequencies > 0 else 0
            
            # Devolver los resultados
            return {
                "mac": mac_addresses,
                "data_snr": mac_addresses_snr,
                "avg_snr_by_frequency": avg_snr_by_frequency,
                "avg_snr_total_node": avg_snr_total_node,  # Promedio total de SNR del nodo
                "affected_count": affected_count,  # Cantidad de cablemodems afectados
                "cablemodems_by_frequency": cablemodems_by_frequency  # Total de cablemodems por frecuencia
                , "affected_cablemodems_by_frequency": affected_cablemodems_by_frequency  # Total de cablemodems afectados por frecuencia
            }
        grouped_data_list = defaultdict(list)
        start_time = time.time()
        results = fetch()
        # print(len(results))
        #remove duplicated macs, get a list only one mac and the multiple values 
        # results = merge_arrays(results["mac_fec"], results["mac_pl"], results["mac_fec_dw"], results["mac_pl_dw"])
        # results = organizar_registros(results)
        # print(results)
        body = write_to_csv(results, nodo_name, qoe, afectacion, total_cm, qoe_hist1, qoe_date1, file_name) 
        # results = list({v['mac']: v for v in results}.values())
        # print(f"Tiempo total usando paralelo: {end_time - start_time:.2f} segundos")
        
        return body
    except Exception as e:
        print(f"Error in the main process: {e}")

def freq_main(region, nodeId, nodo_name, qoe, afectacion, total_cm, qoe_hist1, qoe_date1, file_name):
    try:
        if region == 'centro':
            url =  f'{API_URL_CENTRO}node/{nodeId}/qoe/modems'
            url_channels = f'{API_URL_CENTRO}preeq/node/{nodeId}/channels'
        elif region == 'regional':
            url =  f'{API_URL_REGIONAL}node/{nodeId}/qoe/modems'
            url_channels = f'{API_URL_REGIONAL}preeq/node/{nodeId}/channels'
        def fetch():
            response = requests.get(f"{url}", verify=False)
            channels_response = requests.get(f"{url_channels}", verify=False)
            # print(f"{channels_response.json()}")
            response.raise_for_status()
            resp = response.json()
            
            # Obtener los datos de los canales
            channels = channels_response.json()  # Los canales vienen en MHz
            if (len(channels) > 4):
              type = "split n"
            else:
              type = "split k"
            # Convertimos las frecuencias a MHz para hacer la comparación
            channel_frequencies_mhz = {channel['frequencyMHz'] for channel in channels}
            
            avg_snr = defaultdict(int)  # Promedio de SNR por frecuencia
            max_t3 = defaultdict(int)
            max_t4 = defaultdict(int)
            count_snr = defaultdict(int)  # Contador de SNR por frecuencia
            cablemodems_by_frequency = defaultdict(int)  # Total de cablemodems por frecuencia
            affected_cablemodems_by_frequency = defaultdict(int)  # Total de cablemodems por frecuencia
            affected_count = 0  # Cantidad de cablemodems afectados (snr < 28)
            
            mac_addresses = set()
            mac_addresses_snr = []
            mac_addresses_t3 = []
            mac_addresses_t4 = []
            
            # Iteramos sobre la respuesta de 'usChResponse'
            for x in resp:
                for y in x["usChResponse"]:
                    if '-' not in y.get('frequency', ''):
                        # Convertir la frecuencia de Hz a MHz
                        frequency_mhz = int(y.get('frequency', 0)) / 1000000  # Convertir a MHz
                        
                        # Verificar si la frecuencia está en los canales disponibles
                        if frequency_mhz in channel_frequencies_mhz:
                            snr_val = y.get('snr')
                            t3_val = y.get('t3')
                            t4_val = y.get('t4')

                            if t3_val is not None:
                                if max_t3[frequency_mhz] < t3_val:
                                    max_t3[frequency_mhz] = t3_val
                                mac_addresses_snr.append({
                                    "mac": x['mac'],
                                    "t3": t3_val,
                                    "freq": frequency_mhz
                                })
                            if t4_val is not None:
                                if max_t4[frequency_mhz] < t4_val:
                                    max_t4[frequency_mhz] = t4_val
                                mac_addresses_snr.append({
                                    "mac": x['mac'],
                                    "t4": t4_val,
                                    "freq": frequency_mhz
                                })
                            
                            if snr_val is not None:
                                avg_snr[frequency_mhz] += snr_val
                                count_snr[frequency_mhz] += 1
                                
                                # Contar los cablemodems por frecuencia
                                cablemodems_by_frequency[frequency_mhz] += 1
                                
                                # Contar los cablemodems afectados (snr < 28)
                                if snr_val < 28:
                                    affected_count += 1
                                    affected_cablemodems_by_frequency[frequency_mhz] += 1
                                    
                                # Almacenar mac y snr para reportes
                                mac_addresses.add(x['mac'])
                                mac_addresses_snr.append({
                                    "mac": x['mac'],
                                    "snr": snr_val,
                                    "freq": frequency_mhz
                                })
                                
                                #Add request to capacity
            
            # Calcular el promedio de SNR por frecuencia
            avg_snr_by_frequency = {
                freq: avg_snr[freq] / count_snr[freq] if count_snr[freq] > 0 else 0
                for freq in avg_snr
            }

            # calcular el mayor numero de T3 por frecuencia
            max_t3_by_frequency = {
                freq: max_t3[freq] for freq in max_t3
            }

            max_t4_by_frequency = {
                freq: max_t4[freq] for freq in max_t4
            }
            
            # Promedio total del nodo
            total_snr = sum(avg_snr_by_frequency.values())
            total_frequencies = len(avg_snr_by_frequency)
            avg_snr_total_node = total_snr / total_frequencies if total_frequencies > 0 else 0
            
            # Devolver los resultados
            return {
                "mac": mac_addresses,
                "data_snr": mac_addresses_snr,
                "avg_snr_by_frequency": avg_snr_by_frequency,
                "avg_snr_total_node": avg_snr_total_node,  # Promedio total de SNR del nodo
                "affected_count": affected_count,  # Cantidad de cablemodems afectados
                "cablemodems_by_frequency": cablemodems_by_frequency,  # Total de cablemodems por frecuencia
                "affected_cablemodems_by_frequency": affected_cablemodems_by_frequency,  # Total de cablemodems afectados por frecuencia
                "max_t3_by_frequency": max_t3_by_frequency,
                "max_t4_by_frequency": max_t4_by_frequency,
                "type": type
            }
        grouped_data_list = defaultdict(list)
        start_time = time.time()
        results = fetch()
        # print(len(results))
        #remove duplicated macs, get a list only one mac and the multiple values 
        # results = merge_arrays(results["mac_fec"], results["mac_pl"], results["mac_fec_dw"], results["mac_pl_dw"])
        # results = organizar_registros(results)
        # print(results)
        body = write_to_csv(results, nodo_name, qoe, afectacion, total_cm, qoe_hist1, qoe_date1, file_name) 
        # results = list({v['mac']: v for v in results}.values())
        # print(f"Tiempo total usando paralelo: {end_time - start_time:.2f} segundos")
        
        return body
    except Exception as e:
        print(f"Error in the main process: {e}")

def merge_arrays(*arrays):
    merged = []
    for array in arrays:
        merged.extend(array)
    return merged
# def write_to_csv(data, filename='output.csv'):
#     # Abrir el archivo en modo de escritura
#     with open(filename, mode='w', newline='') as file:
#         writer = csv.writer(file)
        
#         # Escribir los encabezados
#         writer.writerow([
#             'mac', 'snr', 'freq', 'avg_snr_by_frequency', 'avg_snr_total_node', 
#             'affected_count', 'total_cablemodems_freq'
#         ])
        
#         # Escribir los datos para cada cablemodem y sus frecuencias
#         for x in data['data_snr']:
#             mac = x['mac']
#             snr = x['snr']
#             freq = x['freq']
            
#             # Obtener el promedio de SNR por frecuencia
#             avg_snr_by_frequency = data['avg_snr_by_frequency'].get(freq, 0)
            
#             # Obtener el promedio total del nodo
#             avg_snr_total_node = data['avg_snr_total_node']
            
#             # Cantidad de cablemodems afectados (con snr < 28)
#             affected_count = data['affected_count']
            
#             # Total de cablemodems por frecuencia
#             total_cablemodems_freq = data['cablemodems_by_frequency'].get(freq, 0)
            
#             # Escribir la fila en el archivo CSV
#             writer.writerow([
#                 mac, snr, freq, avg_snr_by_frequency, avg_snr_total_node, 
#                 affected_count, total_cablemodems_freq
#             ])
    
#     print(f"Datos escritos en {filename}")
def write_to_csv(data, nodo_name, qoe, afectacion, total_cm, qoe_hist1, qoe_date1, filename='output.csv'):
    rows = []

    # Escribir los datos por frecuencia
    for freq in data['avg_snr_by_frequency']:
        avg_snr_by_frequency = data['avg_snr_by_frequency'][freq]
        
        # Obtener el promedio total del nodo
        avg_snr_total_node = data['avg_snr_total_node']
        
        # Cantidad de cablemodems afectados (con snr < 28)
        affected_count = data['affected_cablemodems_by_frequency'].get(freq, 0)
        
        # Total de cablemodems por frecuencia
        total_cablemodems_freq = data['cablemodems_by_frequency'].get(freq, 0)
        t3 = data['max_t3_by_frequency'].get(freq, 0)
        t4 = data['max_t4_by_frequency'].get(freq, 0)
        
        # Determinar si está afectado (avg_snr_by_frequency <= 30)
        is_affected = 'SI' if avg_snr_by_frequency < 30 else 'NO'
        
        # Crear fila como lista
        row = [
            nodo_name, qoe, afectacion, total_cm, qoe_hist1, qoe_date1, freq, is_affected,
            avg_snr_by_frequency, avg_snr_total_node,
            affected_count, total_cablemodems_freq, t3, t4, data['type']
        ]
        rows.append(row)
    
    return rows
import csv
def organizar_registros(data):
    resultado = {}
    
    for item in data:
        mac = item['mac']
        if mac not in resultado:
            resultado[mac] = {"mac": mac, "pl_dw": None, "fec_dw": None, "pl": 0, "fec": 0}
        
        # Si tiene FEC
        if "fec_dw" in item:
            resultado[mac]["fec_dw"] = item["fec_dw"]
            resultado[mac]["fec"] += 1
        
        # Si tiene PL
        if "pl_dw" in item:
            resultado[mac]["pl_dw"] = item["pl_dw"]
            resultado[mac]["pl"] += 1
        if "fec" in item:
            resultado[mac]["fec"] = item["fec"]
            resultado[mac]["fec"] += 1
        
        # Si tiene PL
        if "pl" in item:
            resultado[mac]["pl"] = item["pl"]
            resultado[mac]["pl"] += 1
    
    
    # Pasar de dict a lista
    return list(resultado.values())
# reinicio_main('regional', 11901116, 'NODO PQT (PARQUE CENTRAL_SPN)', 44, 167, 191, 'output.csv')