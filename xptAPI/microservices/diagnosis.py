from flask import Flask, jsonify, request
from math import radians, cos, sin, sqrt, atan2
from noise import modulation_diagnosis
from datetime import date, timedelta, datetime
import requests
from requests.exceptions import Timeout, RequestException
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# Configuration from given FILE
API_BASE_URL = "/api/v1"
API_URL_CENTRO = "https://100.123.88.85/pathtrak/api/"
API_URL_REGIONAL = "https://100.123.88.84/pathtrak/api/"
API_MODEMS = "qoe/modems"
API_QOE = "summary/metric"

# Timeout for all HTTP requests (10 minutes in seconds)
HTTP_REQUEST_TIMEOUT = 600

# Función para calcular la distancia de Haversine en metros
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # Radio de la Tierra en metros
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# Rango de distancia para agrupar (en metros)
distance_threshold = 50

def get_distance(list):
    # Agrupar los datos por cercanía geográfica
    groups = []
    visited = set()

    for i, entry1 in enumerate(list):
        if i in visited:
            continue
        group = [entry1]
        visited.add(i)
        for j, entry2 in enumerate(list):
            if j != i and j not in visited:
                distance = haversine(entry1['latitude'], entry1['longitude'],
                                    entry2['latitude'], entry2['longitude'])
                if distance <= distance_threshold:
                    group.append(entry2)
                    visited.add(j)
        groups.append(group)
    return groups

# Endpoint to get QoE Modems
@app.route(f'{API_BASE_URL}/qoe-modems', methods=['GET'])
def get_qoe_modems():
    region = request.args.get('region')
    nodeId = request.args.get('nodeId')
    current_datetime = datetime.now()
    datetime_plus_5_hours = current_datetime + timedelta(hours=5)
    datetime_plus_5_hours_str = datetime_plus_5_hours.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    if region == 'centro':
        url =  f'{API_URL_CENTRO}node/{nodeId}/{API_MODEMS}?startdatetime={datetime_plus_5_hours_str}'
    elif region == 'regional':
        url = f'{API_URL_REGIONAL}node/{nodeId}/{API_MODEMS}?startdatetime={datetime_plus_5_hours_str}'
    else:
        return jsonify({'error': 'Invalid region parameter'}), 400
    try:
        response = requests.get(url, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
        response.raise_for_status()
        return jsonify(response.json())
    except Timeout:
        return jsonify({'error': 'Request timed out'}), 504  # HTTP 504 Gateway Timeout
    except RequestException as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500
    
@app.route(f'{API_BASE_URL}/qoe', methods=['GET'])
def get_qoe():
    region = request.args.get('region')
    nodeId = request.args.get('nodeId')
    #date now in this format ?startdatetime=2025-10-15T10:23:00.000Z
    start_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    #+5 horas
    start_datetime = (datetime.now() + timedelta(hours=5)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    print(start_datetime)
    if region == 'centro':
        url =  f'{API_URL_CENTRO}node/{nodeId}/{API_QOE}?startdatetime={start_datetime}'
    elif region == 'regional':
        url = f'{API_URL_REGIONAL}node/{nodeId}/{API_QOE}?startdatetime={start_datetime}'
    else:
        return jsonify({'error': 'Invalid region parameter'}), 400
    try:
        response = requests.get(url, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
        response.raise_for_status()
        return jsonify(response.json())
    except Timeout:
        return jsonify({'error': 'Request timed out'}), 504  # HTTP 504 Gateway Timeout
    except RequestException as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500

@app.route(f'{API_BASE_URL}/downstream-diagnosis', methods=['GET'])
def get_dowstream_diagnosis():
    print('Running Downstream diagnostics')
    region = request.args.get('region')
    nodeId = request.args.get('nodeId')
    if region == 'centro':
        base_url = API_URL_CENTRO
    elif region == 'regional':
        base_url = API_URL_REGIONAL
    else:
        return jsonify({'error': 'Invalid region parameter'}), 400
    url = f'{base_url}downstream/node/{nodeId}/modems/impairments'
    current_datetime = datetime.now()
    datetime_plus_5_hours = current_datetime + timedelta(hours=5)
    datetime_plus_5_hours_str = datetime_plus_5_hours.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    url_qoe_modems = f'{base_url}node/{nodeId}/{API_MODEMS}?startdatetime={datetime_plus_5_hours_str}'
    try:
        qoe_modems_response = requests.get(url_qoe_modems, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
        qoe_modems_response.raise_for_status()
        qoe_modems = qoe_modems_response.json()
        downstream_list = requests.get(url, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
        downstream_list.raise_for_status()
        downstream_list = downstream_list.json()
    except Timeout:
        return jsonify({'error': 'Request timed out'}), 504  # HTTP 504 Gateway Timeout
    except RequestException as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500
    affected_cablemodems = []
    flujo_dw_cablemodems = []
    flujo_dw_cablemodems_ripple = []
    dw_levels_diagnosis = []
    fugas_dw_cablemodems = []
    count_titl = 0
    count_ripple = 0
    count_suck_out = 0
    count_fm_ingress = 0
    count_lte_ingress = 0
    count_adjacency = 0
    count_PL = 0
    count_FEC = 0
    count_SNR = 0
    count_recepcion = 0
    results = []
    for downstream_element in downstream_list:
        titl_present = 'NO'
        ripple_present = 'NO'
        suck_out_present = 'NO'
        fm_ingress_present = 'NO'
        lte_ingress_present = 'NO'
        adjacency_present = 'NO'
        levels_data = downstream_element.get('impairments', [])
        tilt_level = 'No Data'
        suck_out_level = 'No Data'
        snr_down_level = 'No Data'
        ripple_status = 'No Data'
        fm_ingress_status = 'No Data'
        lte_ingress_status = 'No Data'
        adjacency_level = 'No Data'
        adjacency_status = 'No Data'
        for modem in levels_data:
            if modem.get('name') == 'tilt':
                tilt_level = modem.get('level', 'No Data')
            if modem.get('name') == 'suck-out':
                suck_out_level = modem.get('level', 'No Data')
            if modem.get('name') == 'docsis-snr':
                snr_down_level = modem.get('level', 'No Data')
            if modem.get('name') == 'ripple':
                ripple_status = modem.get('status', 'No Data')
            if modem.get('name') == 'fm-ingress':
                fm_ingress_status = modem.get('status', 'No Data')
            if modem.get('name') == 'lte-ingress':
                lte_ingress_status = modem.get('status', 'No Data')
            if modem.get('name') == 'adjacency':
                adjacency_level = modem.get('level', 'No Data')
                adjacency_status = modem.get('status', 'No Data')
        for modem in qoe_modems:

            if modem.get('mac') == downstream_element.get('mac'):
                address = modem.get('address', 'No Data')
                latitude = modem.get('latitude', 'No Data')
                longitude = modem.get('longitude', 'No Data')
                
                uccwe_dw = None
                ccwe_dw = None
                snr_dw = None
                rx_dw = None
                for ch_response in modem.get('docsisDsChResponse', []):
                    if '-' in ch_response.get('frequency', ''):
                        uccwe_dw = ch_response.get('uccwe', 'No Data')
                        ccwe_dw = ch_response.get('ccwe', 'No Data')
                        snr_dw = ch_response.get('snr', 'No Data')
                        rx_dw = ch_response.get('level', 'No Data')
                        if uccwe_dw != 'No Data' or ccwe_dw != 'No Data' or snr_dw != 'No Data' or rx_dw != 'No Data':
                            break
                break     

        if (ripple_status != 'No Data' and ripple_status == 'FAIL'):
            count_ripple += 1
            ripple_present = 'YES'
        if (suck_out_level != 'No Data' and suck_out_level >= 4):
            count_suck_out += 1
            suck_out_present = 'YES'
        if (fm_ingress_status != 'No Data' and fm_ingress_status == 'FAIL'):
            fm_ingress_present = 'YES'
            count_fm_ingress += 1
        if (lte_ingress_status != 'No Data' and lte_ingress_status == 'FAIL'):
            lte_ingress_present = 'YES'
            count_lte_ingress += 1
        if (adjacency_status != 'No Data' and adjacency_status == 'FAIL'):
            adjacency_present = 'YES'
            count_adjacency += 1
        if (uccwe_dw != 'No Data' and uccwe_dw >= 1):
            count_PL += 1
        if (ccwe_dw != 'No Data' and ccwe_dw >= 10):
            count_FEC += 1
        if (snr_dw != 'No Data' and snr_dw <= 32):
            count_SNR += 1
        if (rx_dw != 'No Data' and (rx_dw <= -7 or rx_dw >= 12 )):
            count_recepcion += 1
        if (ripple_present == 'YES'):
            flujo_dw_cablemodems_ripple.append({
                'macAddress': downstream_element.get('mac'),
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'tilt': tilt_level,
                'tilt_present':  titl_present,
                'ripple': ripple_status,
                'ripple_present': ripple_present,
                'suck-out': suck_out_level,
                'suck_out_present': suck_out_present,
                'adjacency': adjacency_level,
                'uccwe_dw': uccwe_dw,
                'ccwe_dw': ccwe_dw,
                'snr_dw': snr_dw,
                'rx_dw': rx_dw,
                'fm_ingress_present':  fm_ingress_present,
                'lte_ingress': lte_ingress_status,
                'lte_ingress_present': lte_ingress_present,
                'snr-down': snr_down_level
            })
        if (titl_present == 'YES' or suck_out_present == 'YES' or adjacency_present == 'YES' or ((uccwe_dw != 'No Data' and uccwe_dw >= 1) or (ccwe_dw != 'No Data' and ccwe_dw >= 10))):
            flujo_dw_cablemodems.append({
                'macAddress': downstream_element.get('mac'),
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'tilt': tilt_level,
                'tilt_present':  titl_present,
                'ripple': ripple_status,
                'ripple_present': ripple_present,
                'suck-out': suck_out_level,
                'suck_out_present': suck_out_present,
                'adjacency': adjacency_level,
                'uccwe_dw': uccwe_dw,
                'ccwe_dw': ccwe_dw,
                'snr_dw': snr_dw,
                'rx_dw': rx_dw,
                'fm_ingress_present':  fm_ingress_present,
                'lte_ingress': lte_ingress_status,
                'lte_ingress_present': lte_ingress_present,
                'snr-down': snr_down_level
            })
        if ((rx_dw != 'No Data' and (rx_dw <= -7 or rx_dw >= 12 )) or (snr_dw != 'No Data' and snr_dw <= 32)):
            dw_levels_diagnosis.append({
                'macAddress': downstream_element.get('mac'),
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'tilt': tilt_level,
                'tilt_present':  titl_present,
                'ripple': ripple_status,
                'ripple_present': ripple_present,
                'suck-out': suck_out_level,
                'suck_out_present': suck_out_present,
                'adjacency': adjacency_level,
                'uccwe_dw': uccwe_dw,
                'ccwe_dw': ccwe_dw,
                'snr_dw': snr_dw,
                'rx_dw': rx_dw,
                'fm_ingress_present':  fm_ingress_present,
                'lte_ingress': lte_ingress_status,
                'lte_ingress_present': lte_ingress_present,
                'snr-down': snr_down_level
            })
        if (fm_ingress_present == 'YES' or lte_ingress_present == 'YES'):
            fugas_dw_cablemodems.append({
                'macAddress': downstream_element.get('mac'),
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'tilt': tilt_level,
                'tilt_present':  titl_present,
                'ripple': ripple_status,
                'ripple_present': ripple_present,
                'suck-out': suck_out_level,
                'suck_out_present': suck_out_present,
                'adjacency': adjacency_level,
                'uccwe_dw': uccwe_dw,
                'ccwe_dw': ccwe_dw,
                'snr_dw': snr_dw,
                'rx_dw': rx_dw,
                'fm_ingress_present':  fm_ingress_present,
                'lte_ingress': lte_ingress_status,
                'lte_ingress_present': lte_ingress_present,
                'snr-down': snr_down_level
            })
        if (suck_out_level != 'No Data' and suck_out_level != 0): 
            affected_cablemodems.append({
                'macAddress': downstream_element.get('mac'),
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'tilt': tilt_level,
                'tilt_present':  titl_present,
                'ripple': ripple_status,
                'ripple_present': ripple_present,
                'suck-out': suck_out_level,
                'suck_out_present': suck_out_present,
                'adjacency': adjacency_level,
                'uccwe_dw': uccwe_dw,
                'ccwe_dw': ccwe_dw,
                'snr_dw': snr_dw,
                'rx_dw': rx_dw,
                'fm_ingress_present':  fm_ingress_present,
                'lte_ingress': lte_ingress_status,
                'lte_ingress_present': lte_ingress_present,
                'snr-down': snr_down_level
            })
        results.append({
            'macAddress': downstream_element.get('mac'),
            'address': address,
            'latitude': latitude,
            'longitude': longitude,
            'tilt': tilt_level,
            'tilt_present':  titl_present,
            'ripple': ripple_status,
            'ripple_present': ripple_present,
            'suck-out': suck_out_level,
            'suck_out_present': suck_out_present,
            'adjacency': adjacency_level,
            'uccwe_dw': uccwe_dw,
            'ccwe_dw': ccwe_dw,
            'snr_dw': snr_dw,
            'rx_dw': rx_dw,
            'fm_ingress_present':  fm_ingress_present,
            'lte_ingress': lte_ingress_status,
            'lte_ingress_present': lte_ingress_present,
            'snr-down': snr_down_level
        })
    counters = {
        "tilt": count_titl,
        "ripple": count_ripple,
        "suck_out": count_suck_out,
        "fm_ingress": count_fm_ingress,
        "lte_ingress": count_lte_ingress,
        "adjacency": count_adjacency,
        "pl": count_PL,
        "fec": count_FEC,
        "snr": count_SNR,
        "recepcion": count_recepcion
    }
    return jsonify({'total_affected': len(affected_cablemodems), 'affected_cablemodems': affected_cablemodems, 'total': len(results), 'total_data': results, 'flujo_dw_cablemodems': flujo_dw_cablemodems, 'flujo_dw_cablemodems_ripple': flujo_dw_cablemodems_ripple, 'dw_levels_diagnosis': dw_levels_diagnosis, 'fugas_dw_cablemodems': fugas_dw_cablemodems, 'counters': counters }), 200


@app.route(f'{API_BASE_URL}/upstream-diagnosis', methods=['GET'])
def get_upstream_diagnosis():
    print('Running Upstream diagnostics')
    region = request.args.get('region')
    nodeId = request.args.get('nodeId')
    
    if not region or not nodeId:
        return jsonify({'error': 'Missing region or nodeId parameter'}), 400

    if region == 'centro':
        base_url = API_URL_CENTRO
    elif region == 'regional':
        base_url = API_URL_REGIONAL
    else:
        return jsonify({'error': 'Invalid region parameter'}), 400

    # URL para obtener canales
    url_channels = f'{base_url}preeq/node/{nodeId}/channels'
    current_datetime = datetime.now()
    datetime_plus_5_hours = current_datetime + timedelta(hours=5)
    datetime_plus_5_hours_str = datetime_plus_5_hours.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    url_qoe_modems = f'{base_url}node/{nodeId}/{API_MODEMS}?startdatetime={datetime_plus_5_hours_str}'
    # url_noise_utilization = f"{base_url}node/{nodeId}/capacity/channels/history?startdatetime={datetime_plus_5_hours_str}"
    try:
        channels_response = requests.get(url_channels, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
        channels_response.raise_for_status()
        channels = channels_response.json()
        qoe_modems_response = requests.get(url_qoe_modems, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
        qoe_modems_response.raise_for_status()
        qoe_modems = qoe_modems_response.json()
        cmtsusport_response = requests.get(f"{base_url}elements/type/node/id/{nodeId}", timeout=HTTP_REQUEST_TIMEOUT, verify=False)
        cmtsusport_response.raise_for_status()
        cmtsusport_json = cmtsusport_response.json()
        cmtsusport  = cmtsusport_json.get('cmtsUsPortId')
        url_noise_utilization = f"{base_url}cmtsusport/{cmtsusport}/capacity/channels/history?startdatetime={datetime_plus_5_hours_str}"
        noise_response =  requests.get(url_noise_utilization, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
        noise_response.raise_for_status()
        noise_data = noise_response.json()
    except Timeout:
        return jsonify({'error': 'Request timed out'}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Failed to fetch channels: {str(e)}'}), 500
    
    count_upstreamTxLevel = 0
    macs_upstreamTxLevel = []
    count_snr = 0
    count_etdr = 0
    count_nmter = 0
    count_icfr = 0
    count_uccwe = 0
    macs_uccwe = []
    count_ccwe = 0
    macs_ccwe = []
    macs_snr = []
    macs_nmter = []
    count_snr_up = 0
  
    # Lista para almacenar los resultados
    results = []
    affected_cablemodems =  []
    preeq_cablemodems = []
    noise_validation_cablemodems = []
    qc_preeq_cablemodems = []
    levels_cablemodems = []

    for channel in channels:
        frequency_mhz = channel.get('frequencyMHz')
        if not frequency_mhz:
            continue

        # URL para obtener los grupos disponibles
        url_groups = f'{base_url}preeq/node/{nodeId}/upstream/{frequency_mhz}/groups'
        try:
            groups_response = requests.get(url_groups, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
            groups_response.raise_for_status()
            groups = groups_response.json().get('groups')
        except Timeout:
            return jsonify({'error': 'Request timed out while fetching groups'}), 504
        except requests.exceptions.RequestException as e:
            return jsonify({'error': f'Failed to fetch groups for frequency {frequency_mhz}: {str(e)}'}), 500
        for group in groups:
            group_id = group.get('groupdId')
            if group_id is None:
                continue
            # Excluir grupos que se encuentran bien RC2025 Z All Good, Fallas por acometidas grupo: Z No matches (Z Impedancias)
            # URL para obtener los detalles del grupo
            url_group_details = f'{base_url}preeq/node/{nodeId}/upstream/{frequency_mhz}/group/{group_id}'
            try:
                group_details_response = requests.get(url_group_details, timeout=HTTP_REQUEST_TIMEOUT, verify=False)
                group_details_response.raise_for_status()
                group_details = group_details_response.json()
            except Timeout:
                return jsonify({'error': 'Request timed out while fetching group details'}), 504
            except requests.exceptions.RequestException as e:
                return jsonify({'error': f'Failed to fetch group details for group {group_id}: {str(e)}'}), 500
            preeq_modem_details = group_details.get('preeqModemDetailResponse', [])
            

            for modem_detail in preeq_modem_details:
                for modem in qoe_modems:
                    if modem.get('mac') == modem_detail.get('macAddress'):
                        address = modem.get('address', 'No Data')
                        latitude = modem.get('latitude', 'No Data')
                        longitude  = modem.get('longitude', 'No Data')
                        icfr = None
                        uccwe = None
                        ccwe = None
                        snr_up = None
                        for ch_response in modem.get('usChResponse', []):
                            if '-' in ch_response.get('frequency', ''):
                                icfr = ch_response.get('mrLevel', 'No Data')
                                uccwe = ch_response.get('uccwe', 'No Data')
                                ccwe = ch_response.get('ccwe', 'No Data')
                                snr_up = ch_response.get('snr', 'No Data')
                                if icfr != 'No Data' or uccwe != 'No Data' or ccwe != 'No Data' or snr_up != 'No Data':
                                    break
                        break
                
                etdr = modem_detail.get('etdr', 'No Data')
                nmter = modem_detail.get('nmter', 'No Data')

                snr = modem_detail.get('snr', 'No Data')
                if snr_up == 'No Data':
                    snr_up = snr
                upstream_tx_level = modem_detail.get('upstreamTxLevel', 'No Data')
                if (upstream_tx_level != 'No Data' and (upstream_tx_level < 40 or upstream_tx_level > 49)):
                    # count_upstreamTxLevel += 1
                    macs_upstreamTxLevel.append(modem_detail.get('macAddress'))
                    
                if ((snr_up != 'No Data' and (snr_up <= 28))):
                    count_snr_up += 1
                    count_snr += 1
                    macs_snr.append(modem_detail.get('macAddress'))
                if (nmter != 'No Data' and (frequency_mhz < 32 and nmter > -12 and nmter < -1) or (frequency_mhz >= 32 and nmter > -7 and nmter < -1)):
                    print('NMTER AFFECTED cOUNTER')
                    count_nmter += 1
                    macs_nmter.append(modem_detail.get('macAddress'))
                if (uccwe != 'No Data' and (uccwe >= 1)):
                    macs_uccwe.append(modem_detail.get('macAddress'))
                if  ((ccwe != 'No Data' and (ccwe >= 10))):
                    macs_ccwe.append(modem_detail.get('macAddress'))
                    
                if ( (nmter != 'No Data' and ((frequency_mhz < 32 and (nmter > -12 and nmter < -1)) or (frequency_mhz >= 32 and (nmter > -7 and nmter < -1)))) ):
                    print('NMTER AFFECTED')
                    preeq_cablemodems.append({
                        'macAddress': modem_detail.get('macAddress'),
                        'address': address,
                        'latitude': latitude,
                        'longitude': longitude,
                        'etdr': etdr,
                        'nmter': nmter, 
                        'icfr': icfr,
                        'uccwe': uccwe,
                        'ccwe': ccwe,
                        'snr_up': snr_up if snr_up else snr,
                        'upstreamTxLevel': upstream_tx_level,
                    })
                if ((uccwe != 'No Data' and (uccwe >= 1)) or ((ccwe != 'No Data' and (ccwe >= 10))) or ((snr_up != 'No Data' and (snr_up <= 28)))):
                    noise_validation_cablemodems.append({
                        'macAddress': modem_detail.get('macAddress'),
                        'address': address,
                        'latitude': latitude,
                        'longitude': longitude,
                        'etdr': etdr,
                        'nmter': nmter, 
                        'icfr': icfr,
                        'uccwe': uccwe,
                        'ccwe': ccwe,
                        'snr_up': snr_up if snr_up else snr,
                        'upstreamTxLevel': upstream_tx_level
                    })
                if ((upstream_tx_level != 'No Data' and (upstream_tx_level < 40 or upstream_tx_level > 49)) or ((snr_up != 'No Data' and (snr_up <= 28)))):
                    levels_cablemodems.append({
                        'macAddress': modem_detail.get('macAddress'),
                        'address': address,
                        'latitude': latitude,
                        'longitude': longitude,
                        'etdr': etdr,
                        'nmter': nmter, 
                        'icfr': icfr,
                        'uccwe': uccwe,
                        'ccwe': ccwe,
                        'snr_up': snr_up if snr_up else snr,
                        'upstreamTxLevel': upstream_tx_level
                    })
                results.append({
                    'frequencyMHz': frequency_mhz,
                    'macAddress': modem_detail.get('macAddress'),
                    'address': address,
                    'latitude': latitude,
                    'longitude': longitude,
                    'upstreamTxLevel': modem_detail.get('upstreamTxLevel'),
                    'snr': modem_detail.get('snr'),
                    'etdr': modem_detail.get('etdr'),
                    'nmter': modem_detail.get('nmter'),
                    'icfr': icfr,
                    'uccwe': uccwe,
                    'ccwe': ccwe,
                    'snr_up': snr_up if snr_up else snr
                })

    
    noise_diagnosis = modulation_diagnosis(noise_data)
    
    # Agrupar los datos por cercanía geográfica
    groups = get_distance(affected_cablemodems)
    preeq_group = get_distance(preeq_cablemodems)

    # if not affected_cablemodems:
    #     return jsonify({'message': 'No data available for the specified nodeId and region'}), 404
    
    unique_modems = list({
        modem['macAddress']: modem
        for modem in results
        if modem.get('macAddress')
    }.values())
    # remove duplicates based on macAddress
    macs_upstreamTxLevel = list(set(macs_upstreamTxLevel))
    macs_uccwe = list(set(macs_uccwe))
    macs_ccwe = list(set(macs_ccwe))
    macs_snr = list(set(macs_snr))
    macs_nmter = list(set(macs_nmter))
    count_upstreamTxLevel = len(macs_upstreamTxLevel)
    count_uccwe = len(macs_uccwe)
    count_ccwe = len(macs_ccwe)
    count_snr = len(macs_snr)
    count_nmter = len(macs_nmter)
    counters = {
        "upstreamTxLevel": count_upstreamTxLevel,
        "snr": count_snr,
        "etdr": count_etdr,
        "nmter": count_nmter,
        "icfr": count_icfr,
        "uccwe": count_uccwe,
        "ccwe": count_ccwe,
        "snr_up": count_snr,
    }
    return jsonify({'total_affected': len(affected_cablemodems), 'affected_cablemodems': affected_cablemodems, 'location_radious': groups, 'total_data': results, 'total': len(unique_modems), 'preeq_cablemodems': preeq_cablemodems, 'preeq_group': preeq_group, 'noise_diagnosis': noise_diagnosis, 'noise_validation': noise_validation_cablemodems, 'levels_cablemodems': levels_cablemodems, 'counters': counters}), 200



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8001)