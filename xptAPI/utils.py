import json
import requests
import logging
import os

def format_critical_nodes_message(start_time, end_time, execution_time, critical_nodes):
    if critical_nodes:
        # Mensaje para nodos críticos encontrados
        nodes_info = "\n".join([
            f"🔴 **Node ID**: {node['node_id']} | **Nombre Nodo**: {node['node_name']} | **QoE Score**: {node['qoe_score']} | "
            f"**Status**: {node['node_status']} | **Modems**: {node['total_modems']} | **Impactados**: {node['impacted_modems']}\n"
            for node in critical_nodes
        ])
        message = (
            f"⚠️ **ALERTA: Nodos Críticos Identificados (Prioridad 1)** ⚠️\n\n"
            f"✅ **Proceso finalizado exitosamente**\n\n"
            f"- 🕒 **Inicio**: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- 🕒 **Fin**: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- ⏱️ **Duración**: {execution_time} segundos\n"
            f"- 📊 **Total Nodos Críticos**: {len(critical_nodes)}\n\n"
            f"🔍 **Detalles de los Nodos Críticos:**\n{nodes_info}"
        )
    else:
        # Mensaje cuando no se encuentran nodos críticos
        message = (
            f"✅ **Proceso finalizado exitosamente**\n\n"
            f"- 🕒 **Inicio**: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- 🕒 **Fin**: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- ⏱️ **Duración**: {execution_time} segundos\n"
            f"⚠️ **No se encontraron nodos críticos Prioridad 1.**"
        )
    return message
def send_teams_notification(status, message, details, start_time, end_time, total_nodes):
    webhook_url = os.getenv("WEBHOOK_NOTIFICATION")
    headers = {"Content-Type": "application/json"}
    
    # Construir la tarjeta adaptativa
    card = {
        "type": "MessageCard",
        "themeColor": "0076D7" if status == "success" else "FF0000",
        "title": f"🔔 Proceso de Inserción en MySQL: {'Exitoso' if status == 'success' else 'Fallido'}",
        "summary": "Notificación del proceso automatizado",
        "sections": [
            {
                "activityTitle": f"**Estado:** {'✔️ Éxitoso' if status == 'success' else '❌ Alerta'}",
                "activitySubtitle": f"Proceso iniciado: {start_time.strftime('%Y-%m-%d %H:%M:%S')}<br>"
                                     f"Proceso finalizado: {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
                "facts": [
                    {"name": "Duración:", "value": f"{(end_time - start_time).total_seconds()} segundos"},
                    {"name": "Total de nodos procesados:", "value": str(total_nodes)},
                    {"name": "Mensaje:", "value": message},
                ],
                "markdown": True
            },
            {
                "text": f"**Detalles:**\n```{details}```"
            }
        ]
    }
    
    # Guardar los detalles en un archivo de texto
    try:
        details_file = "details_log.txt"
        with open(details_file, "a", encoding="utf-8") as file:
            file.write(f"Detalles del proceso iniciado: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            file.write(f"Detalles del proceso finalizado: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            file.write(f"Estado: {'Éxitoso' if status == 'success' else 'Fallido'}\n")
            file.write(f"Mensaje: {message}\n")
            file.write(f"Total de nodos procesados: {total_nodes}\n")
            file.write("Detalles:\n")
            file.write(f"{details}\n")
            file.write("-" * 80 + "\n")
        logging.info(f"Detalles guardados correctamente en {details_file}")
    except IOError as e:
        logging.error(f"Error al guardar los detalles en archivo: {e}")
    
    # Enviar la solicitud POST al webhook
    try:
        response = requests.post(webhook_url, headers=headers, data=json.dumps(card), verify=False)
        if response.status_code == 200:
            logging.info("Notificación enviada a Teams correctamente.")
        else:
            logging.error(f"Error al enviar notificación a Teams: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al conectar con el webhook de Teams: {e}")
def send_teams_notification_per_node(status, message, details):
    """
    Envía una notificación al webhook de Teams y guarda el mensaje en un archivo .txt.
    """
    print('Enviar notificación de TEAMS', message)
    webhook_url = os.getenv("WEBHOOK_NOTIFICATION")
    headers = {"Content-Type": "application/json"}
    
    # Construir la tarjeta adaptativa
    card = {
        "type": "MessageCard",
        "themeColor": "0076D7" if status == "success" else "FF0000",
        "title": f"🔔 Proceso de Diagnóstico de Nodo",
        "summary": "Notificación del proceso automatizado",
        "sections": [
            {
                "activityTitle": "**Estado:** ✔️ Diagnóstico de Nodo iniciado correctamente" if status == "success" else "❌ Diagnóstico de Nodo fallido",
                "facts": [
                    {"name": "Mensaje:", "value": message},
                ],
                "markdown": True
            },
            {
                "text": f"**Detalles:**\n```{details}```"
            }
        ]
    }

    # Enviar la solicitud POST al webhook
    try:
        response = requests.post(webhook_url, headers=headers, data=json.dumps(card), verify=False)
        if response.status_code == 200:
            print("Notificación enviada a Teams correctamente.")
        else:
            print(f"Error al enviar notificación a Teams: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con el webhook de Teams: {e}")


def format_critical_nodes_message(start_time, end_time, execution_time, critical_nodes):
    if critical_nodes:
        # Mensaje para nodos críticos encontrados
        nodes_info = "\n".join([
            f"\n\n🔴 **Node ID**: {node['node_id']} | **Nombre Nodo**: {node['node_name']} | **QoE Score**: {node['qoe_score']} | "
            f"**Status**: {node['node_status']} | **Modems**: {node['total_modems']} | **Impactados**: {node['impacted_modems']}\n"
            for node in critical_nodes
        ])
        message = (
            f"⚠️ **ALERTA: Nodos Críticos Identificados (Prioridad 1)** ⚠️\n\n"
            f"✅ **Proceso finalizado exitosamente**\n\n"
            f"- 🕒 **Inicio**: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- 🕒 **Fin**: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- ⏱️ **Duración**: {execution_time} segundos\n"
            f"- 📊 **Total Nodos Críticos**: {len(critical_nodes)}\n\n"
            f"🔍 **Detalles de los Nodos Críticos:**\n{nodes_info}"
        )
    else:
        # Mensaje cuando no se encuentran nodos críticos
        message = (
            f"✅ **Proceso finalizado exitosamente**\n\n"
            f"- 🕒 **Inicio**: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- 🕒 **Fin**: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- ⏱️ **Duración**: {execution_time} segundos\n"
            f"⚠️ **No se encontraron nodos críticos Prioridad 1.**"
        )
    return message

def format_diagnosis_table(diagnosis_data, diagnosis_type):
    if not diagnosis_data:
        return f"No hay afectacion para {'Modulacion' if diagnosis_type == 'noise_up' else diagnosis_type}."

    def safe_value(value, width, float_format=False, decimals=2, postfix=''):
        if value is None or value == '':
            return f"{'-':<{width}}"
        try:
            if float_format:
                formatted = f"{float(value):>{width - len(postfix)}.{decimals}f}"
                return f"{formatted}{postfix}"
            else:
                return f"{str(value):<{width}}"
        except:
            return f"{'-':<{width}}"




    if diagnosis_type == "downstream":
        header = (
            "| Dirección                  | Latitud  | Longitud   | MAC Address       | SNR Down | Extracción | Inclinación | Ondulación |\n"
        )
        separator = (
            "|----------------------------|----------|------------|-------------------|----------|------------|-------------|------------|\n"
        )
        rows = "\n".join(
            f"| {safe_value(modem['address'], 26)} | {safe_value(modem['latitude'], 8)} | {safe_value(modem['longitude'], 10)} | {safe_value(modem['macAddress'], 17)} | {safe_value(modem.get('snr-down'), 8)} | {safe_value(modem.get('suck-out'), 10)} | {safe_value(modem.get('tilt'), 11)} | {safe_value(modem.get('ripple'), 10)} |"
            for modem in diagnosis_data
        )
    elif diagnosis_type == "upstream":
        header = (
                    "| MAC Address       | Dirección                  | Latitud  | Longitud   | Nivel Tx | SNR   | ETDR |  Frecuencias             |\n"
                )
        separator = (
            "|-------------------|---------------------------|----------|------------|----------|-------|------|------|------|-------------------------|\n"
        )
        rows = "\n".join(
            f"| {safe_value(modem.get('macAddress'), 17)} | {safe_value(modem.get('address'), 27)} | {safe_value(modem.get('latitude'), 8)} | {safe_value(modem.get('longitude'), 10)} | {safe_value(modem.get('upstreamTxLevel'), 8)} | {safe_value(modem.get('snr'), 5)} | {safe_value(modem.get('etdr'), 4)} | {safe_value(modem.get('frequencyMHz'), 23)} |"
            for modem in diagnosis_data
        )
    elif diagnosis_type == "upstream-preeq":
        header = (
            f"| {'Dirección MAC':<18} | {'Dirección de calle':<35} | {'Latitud':<10} | {'Longitud':<10} |  {'NMTER (dB)':<10} | {'eTDR (m)':<10} | {'ICRF':<10} | {'TxLevel':<10} |\n"
        )
        separator = (
            "|" + "-"*20 + "|" + "-"*37 + "|" + "-"*12 + "|" + "-"*12 + "|"  + "-"*13 + "|" + "-"*13 + "|" + "-"*13 + "|" + "-"*13 + "| \n"
        )
        rows = "\n".join(
            f"| {modem['macAddress']:<18} | {modem['address']:<30} | {modem['latitude']:>10.6f} | {modem['longitude']:>10.6f} | {modem['nmter']:<10} | {modem['etdr']:<10} |{modem['icfr']:<10}|{safe_value(modem['upstreamTxLevel'], 8)} |"
            for modem in diagnosis_data
        )

    elif diagnosis_type == "flujo_dw":
        header = (
            f"| {'Dirección MAC':<18} | {'Dirección de calle':<35} | {'Latitud':<10} | {'Longitud':<10} |  {'Extracción':<10} | {'Inclinación':<10} | {'Ondulación ':<10} |  {'PL ':<10} | {'FEC ':<10} | {'snr down':<10} | {'RxLevel ':<10} |\n"
        )
        separator = (
            "|" + "-"*20 + "|" + "-"*37 + "|" + "-"*12 + "|" + "-"*12 + "|"  + "-"*13 + "|" + "-"*13 + "|" + "-"*13 + "|" + "-"*13 + "|" + "-"*12 + "|" + "-"*12 + "|" + "-"*12 + "| \n"
        )
        rows = "\n".join(
            f"| {safe_value(modem.get('macAddress'), 18)} | {safe_value(modem.get('address'), 35)} | {safe_value(modem.get('latitude'), 10)} | {safe_value(modem.get('longitude'), 10)} | {safe_value(modem.get('suck-out'), 11, float_format=True)} | {safe_value(modem.get('tilt'), 11, float_format=True)} | {safe_value(modem.get('ripple'), 11)} | {safe_value(modem.get('uccwe_dw'), 11)} | {safe_value(modem.get('ccwe_dw'), 10)} | {safe_value(modem.get('snr_dw'), 10)} | {safe_value(modem.get('rx_dw'), 10)} |"
            for modem in diagnosis_data
        )
    
    elif diagnosis_type == "fugas_dw":
        header = (
            f"| {'Dirección MAC':<18} | {'Dirección de calle':<35} | {'Latitud':<10} | {'Longitud':<10} |  {'SNR DW':<10} | {'LTE':<10} | {'FM':<10} |\n"

        )
        separator = (
            "|" + "-"*20 + "|" + "-"*37 + "|" + "-"*12 + "|" + "-"*12 + "|"  + "-"*13 + "|" + "-"*12 + "|" + "-"*12 + "| \n"
        )
        rows = "\n".join(
            f"| {safe_value(modem.get('macAddress'), 18)} | {safe_value(modem.get('address'), 35)} | {safe_value(modem.get('latitude'), 10)} | {safe_value(modem.get('longitude'), 10)} | {safe_value(modem.get('snr-down'), 11)} | {safe_value(modem.get('lte_ingress_present'), 10)} | {safe_value(modem.get('fm_ingress_present'), 10)} |"
            for modem in diagnosis_data
        )

    elif diagnosis_type == "noise_up":
        header = (
           f"{'Portadora':<18} {'modType':<10} {'Max usCapUtilize':<15} {'Rango de Tiempo':<45} {'Total Time (hours)':<20}\n"
        )
        separator = (
            "------------------------------------------------------------------------------------------------------------------------------------\n"
        )
        rows = "\n".join(
            f"{safe_value(modem.get('frequency'), 7 + 4, float_format=True, decimals=1, postfix=' MHz')}{'':<8} {safe_value(modem.get('mod_type'), 10)} {safe_value(modem.get('max_utilize'), 15)} {safe_value(modem.get('start_time'), 20)} to {safe_value(modem.get('end_time'), 25)} {safe_value(modem.get('total_time_hours'), 20, float_format=True, decimals=2)}"
            for modem in diagnosis_data
        )
    
    elif diagnosis_type == "noise_up_kpis":
        header = (
            f"| {'Dirección MAC':<18} | {'Dirección de calle':<35} | {'Latitud':<10} | {'Longitud':<10} | {'PL':<5} | {'FEC':<5} | {'SNRUP':<5} | {'Valor PL':<10} | {'Valor FEC':<10} | {'Valor SNR':<10} | {'TxLevel':<10} |\n"
        )
        separator = (
            "|" + "-"*20 + "|" + "-"*37 + "|" + "-"*12 + "|" + "-"*12 + "|" + "-"*7 + "|" + "-"*7 + "|" + "-"*7 + "|" + "-"*12 + "|" + "-"*12 + "|" + "-"*12 + "|" + "-"*12 + "| \n"
        )

        rows = ''

        for modem in diagnosis_data:
            pl_status = "SI" if modem['uccwe'] != 'No Data' and modem['uccwe'] >= 1 else "NO"
            fec_status = "SI" if modem['ccwe'] != 'No Data' and modem['ccwe'] >= 10 else "NO"
            snrup_status = "SI" if modem['snr_up'] != 'No Data' and modem['snr_up'] <= 28 else "NO"

            rows += (
                f"| {safe_value(modem.get('macAddress'), 18)} | " \
                f"{safe_value(modem.get('address'), 35)} | " \
                f"{safe_value(modem.get('latitude'), 10, float_format=True, decimals=6)} | " \
                f"{safe_value(modem.get('longitude'), 10, float_format=True, decimals=6)} | " \
                f"{safe_value(pl_status, 5)} | " \
                f"{safe_value(fec_status, 5)} | " \
                f"{safe_value(snrup_status, 5)} | " \
                f"{safe_value(modem.get('uccwe', 'No Data'), 10, float_format=True, decimals=1)} | " \
                f"{safe_value(modem.get('ccwe', 'No Data'), 10, float_format=True, decimals=2)} | " \
                f"{safe_value(modem.get('snr_up', 'No Data'), 10, float_format=True, decimals=2)} | "\
                f"{safe_value(modem.get('upstreamTxLevel', 'No Data'), 10, float_format=True, decimals=2)} |\n"
            )
    elif diagnosis_type == "consolidated_table_old":
        header = (
            f"| {'Dirección MAC':<18} | {'Valor PL':<10} | {'Valor FEC':<10} | {'Valor SNR':<10} | {'TxLevel':<10} | {'Dirección de calle':<50} |\n"
        )
        separator = (
            "|" + "-"*20 + "|" + "-"*12 + "|" + "-"*12 + "|" + "-"*12 + "|" + "-"*12 + "|" + "-"*52 + "| \n"
        )

        rows = ''

        for modem in diagnosis_data:

            rows += (
                f"| {safe_value(modem.get('macAddress'), 18)} | " \
                f"{safe_value(modem.get('uccwe', 'No Data'), 10, float_format=True, decimals=1)} | " \
                f"{safe_value(modem.get('ccwe', 'No Data'), 10, float_format=True, decimals=2)} | " \
                f"{safe_value(modem.get('snr_up', 'No Data'), 10, float_format=True, decimals=2)} | "\
                f"{safe_value(modem.get('upstreamTxLevel', 'No Data'), 10, float_format=True, decimals=2)} | "
                f"{safe_value(modem.get('address', 'No Data'), 50)} | \n" \
            )

    elif diagnosis_type == "consolidated_table_old":
        def generate_responsive_html_table(diagnosis_data):
            subheaders = [
                "Dirección MAC", "NMTER (dB)", "Adyacencia", "Extracción", "Ondulación", "Inclinación", "PL Down", "FEC Down",
                "Ingresos FM", "Ingresos LTE", "Modulación", "PL UP", "FEC UP", "TX", "RX", "SNRDOWN", "SNRUP", "Dirección"
            ]

            header_groups = [
                ("General", 1), ("Preeq", 1), ("Down", 6), ("FUGAS", 2),
                ("RUIDO", 3), ("NIVELES", 4), ("General", 1)
            ]

            html = """
                <div class="table-container">
                    <h3>Cable modems afectados</h3>
                    <table id="affected_cablemodems">
                        <thead>
                            <tr class="group-header">
            """

            for group, span in header_groups:
                html += f'<th colspan="{span}">{group}</th>'

            html += """
                            </tr>
                            <tr class="sub-header">
            """

            for sub in subheaders:
                html += f'<th>{sub}</th>'

            html += """
                            </tr>
                        </thead>
                        <tbody>
            """

            def safe(val, float_fmt=False, decimals=2):
                if val in [None, "", "No Data"]:
                    return "No Data"
                try:
                    if float_fmt:
                        return f"{float(val):.{decimals}f}"
                    return str(val)
                except:
                    return "Err"

            for modem in diagnosis_data:
                html += f'<tr id="{safe(modem.get("macAddress"))}">'
                html += f"<td>{safe(modem.get('macAddress'))}</td>"
                html += f"<td>{safe(modem.get('nmter'), True)}</td>"
                html += f"<td>{safe(modem.get('adjacency'))}</td>"
                html += f"<td>{safe(modem.get('suck-out'), True)}</td>"
                html += f"<td>{safe(modem.get('ripple'))}</td>"
                html += f"<td>{safe(modem.get('tilt'), True)}</td>"
                html += f"<td>{safe(modem.get('uccwe_dw'))}</td>"
                html += f"<td>{safe(modem.get('ccwe_dw'))}</td>"
                html += f"<td>{safe(modem.get('fm_ingress_present'))}</td>"
                html += f"<td>{safe(modem.get('lte_ingress_present'))}</td>"
                html += f"<td>{safe(modem.get('lte_ingress_present'))}</td>"
                html += f"<td>{safe(modem.get('uccwe'), True, 1)}</td>"
                html += f"<td>{safe(modem.get('ccwe'), True, 1)}</td>"
                html += f"<td>{safe(modem.get('upstreamTxLevel'), True)}</td>"
                html += f"<td>{safe(modem.get('rx_dw'))}</td>"
                html += f"<td>{safe(modem.get('snr_dw'))}</td>"
                html += f"<td>{safe(modem.get('snr_up'))}</td>"
                html += f"<td>{safe(modem.get('address'))}</td>"
                html += "</tr>"

            html += """
                        </tbody>
                    </table>
                </div>
            """
            return html
        return generate_responsive_html_table(diagnosis_data)
    elif diagnosis_type == "consolidated_table":
        def generate_responsive_html_table(diagnosis_data):
            # Define los encabezados de grupo y subcolumnas
            subheaders = [
                "Dirección MAC", "NMTER (dB)", "Adyacencia", "Extracción", "Ondulación", "Inclinación", "PL Down", "FEC Down",
                "Ingresos FM", "Ingresos LTE", "Modulación", "PL UP", "FEC UP", "TX", "RX", "SNRDOWN", "SNRUP", "Dirección"
            ]

            header_groups = [
                ("General", 1), ("Preeq", 1), ("Down", 6), ("FUGAS", 2),
                ("RUIDO", 3), ("NIVELES", 4), ("General", 1)
            ]

            # Inicia la estructura HTML de la tabla
            html = """
                <div class="table-container">
                    <h3>Cable modems afectados</h3>
                    <table id="affected_cablemodems" class="table table-striped table-bordered">
                        <thead>
                            <tr class="group-header">
            """

            # Agrega los encabezados de grupo
            for group, span in header_groups:
                html += f'<th colspan="{span}">{group}</th>'

            html += """
                            </tr>
                            <tr class="sub-header">
            """

            # Agrega los encabezados de subcolumnas
            for sub in subheaders:
                html += f'<th>{sub}</th>'

            html += """
                            </tr>
                        </thead>
                        <tbody>
            """

            # Función auxiliar para manejar valores seguros
            def safe(val, float_fmt=False, decimals=2):
                if val in [None, "", "No Data"]:
                    return "No Data"
                try:
                    if float_fmt:
                        return f"{float(val):.{decimals}f}"
                    return str(val)
                except:
                    return "Err"

            # Itera sobre los datos y genera las filas de la tabla
            for modem in diagnosis_data:
                html += f'<tr id="{safe(modem.get("macAddress"))}">'
                html += f"<td>{safe(modem.get('macAddress'))}</td>"
                html += f"<td>{safe(modem.get('nmter'), True)}</td>"
                html += f"<td>{safe(modem.get('adjacency'))}</td>"
                html += f"<td>{safe(modem.get('suck-out'), True)}</td>"
                html += f"<td>{safe(modem.get('ripple'))}</td>"
                html += f"<td>{safe(modem.get('tilt'), True)}</td>"
                html += f"<td>{safe(modem.get('uccwe_dw'))}</td>"
                html += f"<td>{safe(modem.get('ccwe_dw'))}</td>"
                html += f"<td>{safe(modem.get('fm_ingress_present'))}</td>"
                html += f"<td>{safe(modem.get('lte_ingress_present'))}</td>"
                html += f"<td>{safe(modem.get('lte_ingress_present'))}</td>"
                html += f"<td>{safe(modem.get('uccwe'), True, 1)}</td>"
                html += f"<td>{safe(modem.get('ccwe'), True, 1)}</td>"
                html += f"<td>{safe(modem.get('upstreamTxLevel'), True)}</td>"
                html += f"<td>{safe(modem.get('rx_dw'))}</td>"
                html += f"<td>{safe(modem.get('snr_dw'))}</td>"
                html += f"<td>{safe(modem.get('snr_up'))}</td>"
                html += f"<td>{safe(modem.get('address'))}</td>"
                html += "</tr>"

            # Cierra las etiquetas de la tabla
            html += """
                        </tbody>
                    </table>
                </div>
            """
            return html

        # Llama a la función para generar la tabla
        return generate_responsive_html_table(diagnosis_data)
    elif diagnosis_type == "cons-table":
        html_table = f'<table id="{diagnosis_type}" border="1">'
        
        # Create header row
        headers = ["KPI", "Nivel", "Tiempo (Promedio)", "Afectados", "% Afectados"]
        html_table += '<tr><th>' + '</th><th>'.join(headers) + '</th></tr>'

        # Iterate through the keys and create rows
        for kpi_name, values in diagnosis_data.items():
            level = values["level"]
            affected = values["affected"]
            avg_time = values.get("average_time")
            affected_avg = values["affected_avg"]

            # Create a new row for each KPI
            if level is not None:
                level_txt = f"{level:.2f}"
            else:
                level_txt = "N/A"
            if avg_time is not None:
                avg_time_txt = f"{avg_time:.2f}"
            else:
                avg_time_txt = "N/A"
            html_table += (
                f'<tr>'
                f'<td>{kpi_name}</td>'
                f'<td>{level_txt}</td>'
                f'<td>{avg_time_txt}</td>'
                f'<td>{affected}</td>'
                f'<td>{affected_avg:.2f}%</td>'
                f'</tr>'
            )

        html_table += '</table>'
        return html_table
    elif diagnosis_type == "cons-table-txt":
        # Crear encabezados de columnas
        headers = ["KPI", "Nivel", "Afectados", "% Afectados"]
        column_widths = [15, 10, 10, 15]  # Ancho de cada columna
        header_row = "".join([f"{header:<{width}}" for header, width in zip(headers, column_widths)])
        separator_row = "-" * sum(column_widths)  # Línea separadora

        # Crear filas con los datos
        rows = []
        for kpi_name, values in diagnosis_data.items():
            level = values["level"]
            affected = values["affected"]
            affected_avg = values["affected_avg"]

            # Formatear cada fila
            row = (
                f"{kpi_name:<{column_widths[0]}}"  # KPI
                f"{safe_value(level, column_widths[1]):<{column_widths[1]}}"  # Nivel
                f"{safe_value(affected, column_widths[2]):<{column_widths[2]}}"  # Afectados
                f"{safe_value(f'{affected_avg:.2f}%', column_widths[3]):<{column_widths[3]}}"  # % Afectados
            )
            rows.append(row)

        # Combinar encabezados, separadores y filas en un solo texto
        table_txt = f"{header_row}\n{separator_row}\n" + "\n".join(rows)
        return table_txt
    elif diagnosis_type == "cons-table-txt-2":
        headers_group_sizes = [18, 13] + [12]*6 + [12]*2 + [12]*3 + [12]*4 + [25]
        subheaders = [
            "Dirección MAC", "NMTER (dB)", "Adyacencia", "Extracción", "Ondulación", "Inclinación", "PL Down", "FEC Down",
            "Ingresos FM", "Ingresos LTE", "Modulación", "PL UP", "FEC UP", "TX", "RX", "SNRDOWN", "SNRUP", "Dirección"
        ]

        header_groups = [
            ("General", 1), ("Preeq", 1), ("Down", 6), ("FUGAS", 2),
            ("RUIDO", 3), ("NIVELES", 4), ("General", 1)
        ]

        def center(text, width):
            return text.center(width)

        # Crear línea de grupo de encabezados
        group_header = ""
        idx = 0
        for i, (group_name, count) in enumerate(header_groups):
            width = sum(headers_group_sizes[idx:idx+count])
            group_header += center(group_name, width)
            if i < len(header_groups) - 1:
                group_header += "│"
            idx += count
        group_header += "\n"

        # Línea horizontal separadora (como border inferior del header de grupo)
        group_separator = ""
        idx = 0
        for i, (group_name, count) in enumerate(header_groups):
            width = sum(headers_group_sizes[idx:idx+count])
            group_separator += "─" * width
            if i < len(header_groups) - 1:
                group_separator += "┼"
            idx += count
        group_separator += "\n"

        # Línea de subencabezados
        subheader_line = ""
        for i, (title, width) in enumerate(zip(subheaders, headers_group_sizes)):
            subheader_line += title.ljust(width)
            if i in [0, 1, 7, 9, 12, 16]:
                subheader_line += "│"
        subheader_line += "\n"

        # Línea horizontal separadora para subheaders
        subheader_separator = ""
        for i, width in enumerate(headers_group_sizes):
            subheader_separator += "─" * width
            if i in [0, 1, 7, 9, 12, 16]:
                subheader_separator += "┼"
        subheader_separator += "\n"

        # Función para asegurar el valor y formato
        def safe_value(val, width=12, float_format=False, decimals=2):
            if val is None or val == "":
                return "-".ljust(width)
            try:
                if float_format:
                    val = f"{float(val):.{decimals}f}"
                return str(val).ljust(width)
            except:
                return "Err".ljust(width)

        # Filas de datos
        rows = ""
        for modem in diagnosis_data:
            values = [
                safe_value(modem.get("macAddress"), 18),
                safe_value(modem.get("nmter", "No Data"), 13),
                safe_value(modem.get("adjacency", "No Data"), 12),
                safe_value(modem.get("suck-out", "No Data"), 12),
                safe_value(modem.get("ripple", "No Data"), 12),
                safe_value(modem.get("tilt", "No Data"), 12),
                safe_value(modem.get("uccwe_dw", "No Data"), 12),
                safe_value(modem.get("ccwe_dw", "No Data"), 12),
                safe_value(modem.get("fm_ingress_present", "No Data"), 12),
                safe_value(modem.get("lte_ingress_present", "No Data"), 12),
                safe_value(modem.get("lte_ingress_present", "No Data"), 12),
                safe_value(modem.get("uccwe", "No Data"), 12),
                safe_value(modem.get("ccwe", "No Data"), 12),
                safe_value(modem.get("upstreamTxLevel", "No Data"), 12),
                safe_value(modem.get("rx_dw", "No Data"), 12),
                safe_value(modem.get("snr_dw", "No Data"), 12),
                safe_value(modem.get("snr_up", "No Data"), 12),
                safe_value(modem.get("address", "No Data"), 25)
            ]

            for i in reversed([0, 1, 7, 9, 12, 16]):
                values.insert(i + 1, "│")
            rows += "".join(values) + "\n"

        return "TABLA DE CM AFECTADOS\n\n" + group_header + group_separator + subheader_line + subheader_separator + rows


    else:
        return "Tipo de diagnóstico no reconocido."
    return header + separator + rows