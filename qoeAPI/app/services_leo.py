
import json
import os
from datetime import datetime
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.helpers import serialize_object
from zeep.transports import Transport
from zeep.exceptions import XMLSyntaxError, Fault
import time
import requests
import xml.etree.ElementTree as ET
import html


USER = "wsmax"
PASSWORD = "Cl4r0**s1p"
ruta_certificado = r"C:\Users\ADMINISTRATOR\Documents\devs\fabrica_jose\itsm\claro_chain.pem"

def check_open_tickets(node, route):
    """Obtiene la información de tickets abiertos de una ruta de clasificación"""
    url = "https://controldesk.claro.net.co:8444/meaweb/services/WS_CL_Incident_query"
    try:
        status_filters = "<max:STATUS>=ABIERTO,=ASIGNADO,=INPROG,=PENDIENTE</max:STATUS>"

        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:max="http://www.ibm.com/maximo">
           <soapenv:Header/>
           <soapenv:Body>
              <max:QueryCL_TICKET_R>
                 <max:CL_TICKET_RQuery>
                    <max:INCIDENT>
                       <max:CLASS>INCIDENT</max:CLASS>
                       <max:CLASSSTRUCTUREID>{route}</max:CLASSSTRUCTUREID>
                       {status_filters}
                    </max:INCIDENT>
                 </max:CL_TICKET_RQuery>
              </max:QueryCL_TICKET_R>
           </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": "QueryCL_TICKET_R"
        }

        response = requests.post(
            url,
            auth=(USER, PASSWORD),
            headers=headers,
            data=soap_body.encode("utf-8"),
            verify=ruta_certificado
        )

        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: {response.text}")

        # Parse XML response
        ns = {
            "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
            "max": "http://www.ibm.com/maximo"
        }

        root = ET.fromstring(response.content)
        incidents = root.findall(".//max:INCIDENT", namespaces=ns)

        tickets = []

        for inc in incidents:
            def get_value(name):
                el = inc.find(f"max:{name}", namespaces=ns)
                return el.text if el is not None else None

            ticket = {
                "ticket": get_value("TICKETID"),
                "description": get_value("DESCRIPTION"),
                "status": get_value("STATUS"),
                "creationdate": get_value("CREATIONDATE"),
                "ownergroup": get_value("OWNERGROUP"),
                "internalpriority": get_value("INTERNALPRIORITY"),
                "classstructureid": get_value("CLASSSTRUCTUREID"),
                "location": get_value("LOCATION"),
                "cinum": get_value("CINUM"),
            }
            tickets.append(ticket)

        return tickets

    except Exception as e:
        error = {
            'error_method': 'check_open_tickets_requests',
            'exception': type(e).__name__,
            'lineError': e.__traceback__.tb_lineno,
            'message_develop': f'Error en el servicio SOAP de Maximo {url}',
            'message_python': str(e)
        }
        raise Exception(error)

 
def get_incident(incident):
    """Obtener la información de un incidente usando requests + SOAP"""
    url = "https://controldesk.claro.net.co:8444/meaweb/services/WS_CL_Incident_query"

    try:
        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:max="http://www.ibm.com/maximo">
           <soapenv:Header/>
           <soapenv:Body>
              <max:QueryCL_TICKET_R>
                 <max:CL_TICKET_RQuery>
                    <max:INCIDENT>
                       <max:CLASS>INCIDENT</max:CLASS>
                       <max:TICKETID>{incident}</max:TICKETID>
                    </max:INCIDENT>
                 </max:CL_TICKET_RQuery>
              </max:QueryCL_TICKET_R>
           </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": "QueryCL_TICKET_R"
        }

        response = requests.post(
            url,
            auth=(USER, PASSWORD),
            headers=headers,
            data=soap_body.encode("utf-8"),
            verify=ruta_certificado
        )

        if response.status_code != 200:
            raise Exception(f"Error HTTP {response.status_code}: {response.text}")

        # Parsear XML
        ns = {
            "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
            "max": "http://www.ibm.com/maximo"
        }
        root = ET.fromstring(response.content)
        incident_elem = root.find(".//max:INCIDENT", namespaces=ns)

        if incident_elem is None:
            raise Exception("No se encontró información del incidente.")

        def get_value(elem_name):
            node = incident_elem.find(f"max:{elem_name}", namespaces=ns)
            return node.text if node is not None else None

        ticket = {
            "ticket": get_value("TICKETID"),
            "description": get_value("DESCRIPTION"),
            "status": get_value("STATUS"),
            "creationdate": get_value("CREATIONDATE"),
            "ownergroup": get_value("OWNERGROUP"),
            "internalpriority": get_value("INTERNALPRIORITY"),
            "classstructureid": get_value("CLASSSTRUCTUREID"),
            "location": get_value("LOCATION"),
            "cinum": get_value("CINUM"),
            "failurecode": get_value("FAILURECODE"),
            "incsolucion": get_value("INCSOLUCION"),
            "problemcode": get_value("PROBLEMCODE"),
            "fr1code": get_value("FR1CODE"),
            "fr2code": get_value("FR2CODE")
        }

        return ticket

    except Exception as e:
        error = {
            'error_method': 'get_incident_requests',
            'exception': type(e).__name__,
            'lineError': e.__traceback__.tb_lineno,
            'message_develop': f'Error en el servicio SOAP de Maximo {url}',
            'message_python': str(e)
        }
        raise Exception(error)

def searchIncidenttotal(node):
    """Obtiene los incidentes de afectación de servicio usando requests y SOAP XML."""
    try:
        url = "https://controldesk.claro.net.co:8444/meaweb/services/WS_CL_Incident_query"
        
        where_clause = (
            "status in ('ABIERTO','INPROG','ASIGNADO','PENDIENTE') and "
            "creationdate >= TRUNC(SYSDATE) - 3 and "
            "creationdate <= SYSDATE - INTERVAL '4' HOUR and "
            "classstructureid in ('5326','5347','5351','5355','5361','5365','I1317','I1312','5623','5012','6215','I1368','I1367','I1311','I1349','I1320','I1314','I1319','I1350','5345','5349','5353','5357','5359','5363','5367','I1321','I1351','I1417','I1370','I1313','I1416','5322','5324','I1369','I1318','3606','I1315','I1415','I1352','I1371','I1366','I1316','I1348','3772','6355','5683','I4472') and "
            "exists(select 1 from multiassetlocci where recordkey=incident.ticketid and "
            "recordclass=incident.class and exists(select 1 from ci where cinum=multiassetlocci.cinum and status='OPERATING' and classstructureid = '1116' "
            f"and description like '{node}%'))"
        )

        # Escapar el contenido XML sensible
        import html
        where_clause_escaped = html.escape(where_clause)

        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:max="http://www.ibm.com/maximo">
           <soapenv:Header/>
           <soapenv:Body>
              <max:QueryCL_TICKET_R>
                 <max:CL_TICKET_RQuery>
                    <max:WHERE>{where_clause_escaped}</max:WHERE>
                 </max:CL_TICKET_RQuery>
              </max:QueryCL_TICKET_R>
           </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": "QueryCL_TICKET_R"
        }

        response = requests.post(
            url,
            auth=(USER, PASSWORD),
            headers=headers,
            data=soap_body.encode("utf-8"),
            verify=ruta_certificado
        )

        if response.status_code != 200:
            raise Exception(f"Error HTTP {response.status_code}: {response.text}")

        # Aquí puedes usar una librería como `xmltodict` o `ElementTree` para convertir XML a dict/json.
        # Ejemplo básico con xml.etree.ElementTree:
        incidents = []

        root = ET.fromstring(response.content)
        ns = {'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/', 'max': 'http://www.ibm.com/maximo'}

        for incident in root.findall('.//max:INCIDENT', ns):
            incident_data = {
                "ticketid": incident.find('max:TICKETID', ns).text if incident.find('max:TICKETID', ns) is not None else "",
                "description": incident.find('max:DESCRIPTION', ns).text if incident.find('max:DESCRIPTION', ns) is not None else "",
                "creationdate": incident.find('max:CREATIONDATE', ns).text if incident.find('max:CREATIONDATE', ns) is not None else "",
                "status": incident.find('max:STATUS', ns).text if incident.find('max:STATUS', ns) is not None else "",
                "description_longdescription": incident.find('max:DESCRIPTION_LONGDESCRIPTION', ns).text if incident.find('max:DESCRIPTION_LONGDESCRIPTION', ns) is not None else "",
                "location": incident.find('max:LOCATION', ns).text if incident.find('max:LOCATION', ns) is not None else "",
                "cinum": incident.find('max:CINUM', ns).text if incident.find('max:CINUM', ns) is not None else "",
            }
            incidents.append(incident_data)

        return incidents

    except Exception as e:
        raise Exception({
            'error_method': 'searchIncidenttotal_requests',
            'exception': type(e).__name__,
            'lineError': e.__traceback__.tb_lineno,
            'message_develop': f'Error en el servicio SOAP de Maximo {url}',
            'message_python': str(e)
        })

 
def checkOT_QC(ticketid, nemonico):
    """Valida si el incidente tiene OT abierta con el nemónico correspondiente (sin usar zeep)"""
    try:
        url = "https://controldesk.claro.net.co:8444/meaweb/services/WS_CL_Incident_query"

        nemonico = str(nemonico).upper()

        where_clause = (
            f"ticketid = '{ticketid}' and "
            "exists(select 1 from workorder where workorder.origrecordid = incident.ticketid "
            "and woclass = 'WORKORDER' "
            "and status not in ('CLOSE','COMP','CAN','COMPLETADO','COMPL') "
            f"and description like '%({nemonico})%')"
        )

        where_clause_escaped = html.escape(where_clause)

        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:max="http://www.ibm.com/maximo">
           <soapenv:Header/>
           <soapenv:Body>
              <max:QueryCL_TICKET_R>
                 <max:CL_TICKET_RQuery>
                    <max:WHERE>{where_clause_escaped}</max:WHERE>
                 </max:CL_TICKET_RQuery>
              </max:QueryCL_TICKET_R>
           </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": "QueryCL_TICKET_R"
        }

        response = requests.post(
            url,
            auth=(USER, PASSWORD),
            headers=headers,
            data=soap_body.encode("utf-8"),
            verify=ruta_certificado
        )

        if response.status_code != 200:
            raise Exception(f"Error HTTP {response.status_code}: {response.text}")
        
        root = ET.fromstring(response.content)

        # Extraer INCIDENT info del XML
        incidents = []
        namespace = {
            'soapenv': "http://schemas.xmlsoap.org/soap/envelope/",
            'max': "http://www.ibm.com/maximo"
        }

        for incident in root.findall(".//max:INCIDENT", namespaces=namespace):
            #print(f"el incidente es linea 325: {incident}")
            
            def get_field(tag):
                el = incident.find(f"max:{tag}", namespaces=namespace)
                return el.text if el is not None else None
            
            incidents.append({
                "ticketid": get_field("TICKETID"),
                "description": get_field("DESCRIPTION"),
                "creationdate": get_field("CREATIONDATE"),
                "status": get_field("STATUS"),
                "description_longdescription": get_field("DESCRIPTION_LONGDESCRIPTION"),
                "location": get_field("LOCATION"),
                "cinum": get_field("CINUM")
            })

        return incidents

    except Exception as e:
        error = {
            'error_method': 'checkOT_QC_requests',
            'exception': type(e).__name__,
            'lineError': e.__traceback__.tb_lineno,
            'message_develop': f'Error en el servicio SOAP de Maximo {url}',
            'message_python': str(e)
        }
        raise Exception(error)
        
def create_incident(description, longdescription, impact, urgency, route, location, cinum, ownergroup, status):
    try:
        url = "https://controldesk.claro.net.co:8444/meaweb/services/WS_CL_Incident_Crear"

        # Armado del cuerpo XML con los datos del incidente
        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:max="http://www.ibm.com/maximo">
           <soapenv:Header/>
           <soapenv:Body>
              <max:CreateCL_TICKET_R>
                 <max:CL_TICKET_RSet>
                    <max:INCIDENT>
                       <max:CLASS>INCIDENT</max:CLASS>
                       <max:DESCRIPTION>{html.escape(description)}</max:DESCRIPTION>
                       <max:DESCRIPTION_LONGDESCRIPTION>{html.escape(longdescription)}</max:DESCRIPTION_LONGDESCRIPTION>
                       <max:IMPACT>{impact}</max:IMPACT>
                       <max:URGENCY>{urgency}</max:URGENCY>
                       <max:SITEID>CLAROMOV</max:SITEID>
                       <max:ORGID>CLARO</max:ORGID>
                       <max:HISTORYFLAG>0</max:HISTORYFLAG>
                       <max:ASSETSITEID>CLAROMOV</max:ASSETSITEID>
                       <max:CLASSSTRUCTUREID>{route}</max:CLASSSTRUCTUREID>
                       <max:ASSETORGID>CLARO</max:ASSETORGID>
                       <max:LOCATION>{location}</max:LOCATION>
                       <max:CINUM>{cinum}</max:CINUM>
                       <max:OWNERGROUP>{ownergroup}</max:OWNERGROUP>
                       <max:CREATEDBY>WSMAX</max:CREATEDBY>
                       <max:STATUS>{status}</max:STATUS>
                    </max:INCIDENT>
                 </max:CL_TICKET_RSet>
              </max:CreateCL_TICKET_R>
           </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": "CreateCL_TICKET_R"
        }

        response = requests.post(
            url,
            auth=(USER, PASSWORD),
            headers=headers,
            data=soap_body.encode("utf-8"),
            verify=ruta_certificado
        )

        if response.status_code != 200:
            raise Exception(f"Error HTTP {response.status_code}: {response.text}")

        root = ET.fromstring(response.content)

        namespace = {
            'max': "http://www.ibm.com/maximo"
        }
       
        ticket_elem = root.find(".//max:TICKETID", namespaces=namespace)
        if ticket_elem is not None:
                ticketid = ticket_elem.text
        else:
            raise Exception(f"No se encontró TICKETID. XML: {ET.tostring(root, encoding='unicode')}")

        return ticketid

    except Exception as e:
        error = {
            'error_method': 'create_incident_requests',
            'exception': type(e).__name__,
            'lineError': e.__traceback__.tb_lineno,
            'message_develop': f'Error en el servicio SOAP de Maximo {url}',
            'message_python': str(e)
        }
        raise Exception(error)
 
def create_note(ticketid, note, createdby="WSMAX"):
    try:
        url = "https://controldesk.claro.net.co:8444/meaweb/services/WS_CreateWorklogInc"

        # Armado del cuerpo SOAP
        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:max="http://www.ibm.com/maximo">
           <soapenv:Header/>
           <soapenv:Body>
              <max:UpdateINCWORKLOG>
                 <max:INCWORKLOGSet>
                    <max:INCIDENT>
                       <max:TICKETID>{ticketid}</max:TICKETID>
                       <max:CLASS>INCIDENT</max:CLASS>
                       <max:ORGID>CLARO</max:ORGID>
                       <max:SITEID>CLAROMOV</max:SITEID>
                       <max:WORKLOG>
                          <max:DESCRIPTION>{html.escape(note)}</max:DESCRIPTION>
                          <max:CREATEBY>{html.escape(createdby)}</max:CREATEBY>
                       </max:WORKLOG>
                    </max:INCIDENT>
                 </max:INCWORKLOGSet>
              </max:UpdateINCWORKLOG>
           </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": "UpdateINCWORKLOG"
        }

        response = requests.post(
            url,
            auth=(USER, PASSWORD),
            headers=headers,
            data=soap_body.encode("utf-8"),
            verify=ruta_certificado
        )

        if response.status_code != 200:
            raise Exception(f"Error HTTP {response.status_code}: {response.text}")

        # Validación básica de que la nota fue aceptada
        root = ET.fromstring(response.content)
        # Puedes ajustar esta búsqueda según lo que necesites extraer del XML
        return "OK"

    except Exception as e:
        error = {
            'error_method': 'create_note_requests',
            'exception': type(e).__name__,
            'lineError': e.__traceback__.tb_lineno,
            'message_develop': f'Error en el servicio SOAP de Maximo {url}',
            'message_python': str(e)
        }
        raise Exception(error)


def create_ot(ticketid, description_ot, worktype, location, wopriority, cinum, route, createdby="WSMAX"):
    url = 'https://controldesk.claro.net.co:8444/meaweb/services/WS_Cl_WoTicket'

    try:
        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:max="http://www.ibm.com/maximo">
           <soapenv:Header/>
           <soapenv:Body>
              <max:SyncCL_WOTICKET>
                 <max:CL_WOTICKETSet>
                    <max:WORKORDER>
                       <max:ORIGRECORDID>{ticketid}</max:ORIGRECORDID>
                       <max:DESCRIPTION>{html.escape(description_ot)}</max:DESCRIPTION>
                       <max:LOCATION>{location}</max:LOCATION>
                       <max:CINUM>{cinum}</max:CINUM>
                       <max:CLASSSTRUCTUREID>{route}</max:CLASSSTRUCTUREID>
                       <max:WOPRIORITY>{wopriority}</max:WOPRIORITY>
                       <max:CHANGEBY>{createdby}</max:CHANGEBY>
                       <max:REPORTEDBY>{createdby}</max:REPORTEDBY>
                       <max:WORKTYPE>{worktype}</max:WORKTYPE>
                       <max:ORGID>CLARO</max:ORGID>
                       <max:ORIGRECORDCLASS>INCIDENT</max:ORIGRECORDCLASS>
                       <max:SITEID>CLAROMOV</max:SITEID>
                    </max:WORKORDER>
                 </max:CL_WOTICKETSet>
              </max:SyncCL_WOTICKET>
           </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": "SyncCL_WOTICKET"
        }

        response = requests.post(
            url,
            auth=(USER, PASSWORD),
            headers=headers,
            data=soap_body.encode("utf-8"),
            verify=ruta_certificado
        )

        if response.status_code != 200:
            raise Exception(f"Error HTTP {response.status_code}: {response.text}")

        
        root = ET.fromstring(response.content)

        namespace = {
            'max': "http://www.ibm.com/maximo"
        }
       
        wonum = root.find(".//max:WONUM", namespaces=namespace)
        if wonum is not None:
                wonum = wonum.text
        else:
            raise Exception(f"No se encontró wonum. XML: {ET.tostring(root, encoding='unicode')}")

        return wonum

    except Exception as e:
        error = {
            'error_method': 'create_ot_requests',
            'exception': type(e).__name__,
            'lineError': e.__traceback__.tb_lineno,
            'message_develop': f'Error en el servicio SOAP de Maximo {url}',
            'message_python': str(e)
        }
        raise Exception(error)

   
def createTask(ticketid, description, ownergroup, route, cinum, location, wopriority):
    url = "https://controldesk.claro.net.co:8444/meaweb/services/WS_CreateTaskIncident"

    try:
        soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:max="http://www.ibm.com/maximo">
           <soapenv:Header/>
           <soapenv:Body>
              <max:CreateWOACTIVITY>
                 <max:WOACTIVITYSet>
                    <max:WOACTIVITY>
                       <max:DESCRIPTION>{html.escape(description)}</max:DESCRIPTION>
                       <max:SITEID>CLAROMOV</max:SITEID>
                       <max:ORIGRECORDID>{ticketid}</max:ORIGRECORDID>
                       <max:ORIGRECORDCLASS>INCIDENT</max:ORIGRECORDCLASS>
                       <max:LOCATION>{location}</max:LOCATION>
                       <max:CINUM>{cinum}</max:CINUM>
                       <max:CLASSSTRUCTUREID>{route}</max:CLASSSTRUCTUREID>
                       <max:OWNERGROUP>{ownergroup}</max:OWNERGROUP>
                       <max:WOPRIORITY>{wopriority}</max:WOPRIORITY>
                    </max:WOACTIVITY>
                 </max:WOACTIVITYSet>
              </max:CreateWOACTIVITY>
           </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": "CreateWOACTIVITY"
        }

        response = requests.post(
            url,
            auth=(USER, PASSWORD),
            headers=headers,
            data=soap_body.encode("utf-8"),
            verify=ruta_certificado
        )

        if response.status_code != 200:
            raise Exception(f"Error HTTP {response.status_code}: {response.text}")

        root = ET.fromstring(response.content)

        namespace = {
            'max': "http://www.ibm.com/maximo"
        }
       
        wonum = root.find(".//max:WONUM", namespaces=namespace)
        if wonum is not None:
                wonum = wonum.text
        else:
            raise Exception(f"No se encontró wonum. XML: {ET.tostring(root, encoding='unicode')}")

        return wonum

    except Exception as e:
        error = {
            "error_method": "create_task_requests",
            "exception": type(e).__name__,
            "lineError": e.__traceback__.tb_lineno,
            "message_develop": f"Error en el servicio SOAP de Maximo {url}",
            "message_python": str(e)
        }
        raise Exception(error)

def document_incident_closure(
        ticketid: str,
        failurecode: str,
        incsolucion: str,
        problemcode: str,
        fr1code: str,
        fr2code: str,
        solo_cierre:False
    ):
        
    """Documenta el cierre de un incidente mediante el servicio web de MAXIMO.

    Parameters:
        - ticketid (str): El incidente que se cerrará.
        - failurecode (str): DESEMPENO_ACCESO_MOVIL_MESCAL.
        - problemcode (str): codigo del problema .
        - fr1code (str): codigo de la causa.
        - fr2code (str): "codigo del remedio".
        - incsolucion (str): mensaje de la solucion. 
    """
    try:
        urlDocClosure = "https://controldesk.claro.net.co:8444/meaweb/services/WS_Cl_Inc_Update?wsdl"
        
        if solo_cierre:
            query = {
                'INCIDENT':{
                    'CHANGEBY': 'WSMAX',
                    'CLASS': 'INCIDENT',
                    'DESCERROR':'CERRADO',
                    'DIAGNOSTICADO':True,
                    'TICKETID' : ticketid,
                    'HISTORYFLAG':True,
                    'STATUS': 'CERRADO'
                }
            }
        else:
            query = {
                'INCIDENT':{
                    'CHANGEBY': 'WSMAX',
                    'CLASS': 'INCIDENT',
                    'DESCERROR':'CERRADO',
                    'DIAGNOSTICADO':True,
                    'TICKETID' : ticketid,
                    'FAILURECODE': failurecode,
                    'PROBLEMCODE': problemcode,
                    'FR1CODE': fr1code,
                    'FR2CODE': fr2code,
                    'INCSOLUCION': incsolucion,
                    'HISTORYFLAG':True,
                    'STATUS': 'CERRADO'
                }
            }

        print(query)

        with Session() as session:
            session.verify  = ruta_certificado
            session.auth = HTTPBasicAuth(USER, PASSWORD)
            client = Client(urlDocClosure, transport=Transport(session=session))
            client.service.UpdateCLINCIDENT(CLINCIDENTSet=query)
        
    except Exception as e:
        error= {
            "error_method": "document_incident_closure",
            "exception": type(e).__name__,
            "lineError":e.__traceback__.tb_lineno,
            "message_develop": f"Error en el servicio SOAP de maximo {urlDocClosure}",
            "message_python": str(e)
        }
        raise Exception(error)
    

# result = check_open_tickets('NODO TZA (TERRAZAS SINCELEJO)','6492')
# print(result)

# result =searchIncidenttotal('IDS3F-')
# print(result)

# INC=create_incident("esto es un test", "test test", "3", 3, "I1311", "IDS-FLANDES ORQUIDEA 2", "36091522", "FOHFC", "INPROG")
# print(INC)

# wonum=createTask('INC28495033', 'INC28495033', 'BOE_MOVIL', "I1311", "36091522", "IDS-FLANDES ORQUIDEA 2", 3)
# print(wonum)

# wonum = create_ot('INC28495033', "(SOC) esto es un test", "MC", "IDS-FLANDES ORQUIDEA 2", 3, "36091522", "I1311", createdby="WSMAX")
# print(wonum)

# create_note("INC28495033", "hola mundo", createdby="WSMAX")

# result = get_incident("INC28495033")
# print(result)

# result = checkOT_QC("INC28495033", "SOC")
# print(result)



