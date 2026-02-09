from .models import *

import pandas as pd
import requests, os, re, json
from dotenv import load_dotenv
import traceback


load_dotenv()
URL_WEBHOOK = os.getenv("URL_WEBHOOK1")


def get_node_search(node):
    """Devuelve una lista de nodos a buscar dependiendo de la informacion que viene de pathtrak"""
    node = str(node).upper()
    matches = re.findall(r'(NODO|CLUSTER)\s+(.*)\s+\(', node)
    node_generic = []
    if matches:
        if matches[0][0] == "CLUSTER" or matches[0][0] == "NODO":
            nodes = matches[0][1].split(",")
            nodes = [f"{node}-" for node in nodes]
            return nodes
        return [f"{matches[0][1]}-"]
    else:
        return node_generic




def get_cinum(node):    
    nodes = get_node_search(node)     
    #classstructureid = para encontrar la classstructure.classificationid
    for node in nodes:
        query = Dg_maximo_ci.query.filter(
            Dg_maximo_ci.status == 'OPERATING',
            Dg_maximo_ci.classstructureid=='1116',
            Dg_maximo_ci.description.like(f'{node}%'))
                    
        for row in query:
            print(f"{row.cinum} - {row.description}")
            return row.cinum, row.cilocation
    # raise Exception("No hay cinum asociado al nodo")
    return None, None

def get_info_incident(all_tickets, node):
    """obtiene el incidente creado de forma mas reciente y confirma que el nombre del nodo se encuentra en el summary"""
    df = pd.DataFrame(all_tickets)
    df = df.sort_values(by='ticket', ascending=False)
    for _, row in df.iterrows():
        text = str(row['description']).upper()
        pattern = "QOE\s*-\s*\[(.*?)\]"
        result = re.findall(pattern,text)
        if result and result[0] == str(node).upper():
            return row['ticket'], row['location'], row['cinum']
    return None, None, None

def check_ots(incident):
    """Obtiene una lista de diccionario con las OTs abiertas sobre el incidente mediante dataguard"""
    list_estatus = ['COMP','APPR','WAPPR','INPRG','PENDING','SEGUIMIENTO','APROB','INICIADO','INPROG','SUSPENDIDO','ASIGNADO']
    query = Dg_maximo_workorder.query.filter(
            Dg_maximo_workorder.origrecordid == incident,
            Dg_maximo_workorder.status.in_(list_estatus))
    
    list_result =[]
    for row in query:
        list_result.append(row.as_dict())
    
    return list_result


def validate_creation_ots(list_ots_open, list_description_ots, data, incident):
    """Devuelve una lista con las ots ya existente en el incidentes y otra lista con las ots a crear"""

    ots_existente, ots_a_crear=[], []
    for nemonico_ot in list_description_ots:
        nemonico_clean = nemonico_ot.replace("(","").replace(")","")
        for register_ots_open in list_ots_open:
            if nemonico_ot.lower() in register_ots_open["description"].lower():
                ots_existente.append({'name':nemonico_clean, 'wonum':register_ots_open["wonum"]})
                break
        else:
            ots_a_crear.append({'name':nemonico_clean, 'wonum':"", "notes":[]})
    ots_a_crear = ots_to_create_inc_open(ots_a_crear, data)
    ots_existente = notes_to_create_inc_open(ots_existente, data, incident)
    return ots_existente, ots_a_crear

def check_tas_bo(incident, ownergroup):
    """valida si hay una TAS abierta sobre un grupo en especifico para el incidente mediante dataguard"""
    list_estatus = ['COMP','APPR','WAPPR','INPRG','PENDING','SEGUIMIENTO','APROB','INICIADO','INPROG','SUSPENDIDO','ASIGNADO']
    query = Dg_maximo_woactivity.query.filter(
            Dg_maximo_woactivity.origrecordid == incident,
            Dg_maximo_woactivity.ownergroup == ownergroup,
            Dg_maximo_woactivity.status.in_(list_estatus))
    
    list_result =[]
    for row in query:
        list_result.append(row.as_dict())
    
    if list_result:
        return list_result[0]['wonum']
    return ""


def get_notes_inc(incident):
    """obtiene todas las notas  de un incidente"""
    query = Dg_maximo_worklog.query.filter(
            Dg_maximo_worklog.recordkey == incident
        )
    list_result =[]
    for row in query:
        list_result.append(row.as_dict())
    return list_result


def ots_to_create(list_description_ots, data):
    """retorna una lista de diccionario, donde cada diccionario es el tipo de OT a Crear,
    y  dentro de la llave notas se retorna las notas que se desean agregar por la OT 
    este procedimiento aplica unicamente cuando se crea el incidente"""
    list_ots = []
    for description_ot in list_description_ots:
        ot = {
                'name':description_ot.replace("(","").replace(")",""), 
                'wonum':""
            }
        for worklog in data['worklogs']:
            if ot['name'] == worklog['name']:
                ot['notes_to_create'] = worklog['notes']
                break
        list_ots.append(ot)
    return list_ots


def ots_to_create_inc_open(ots_a_crear, data):
    """retorna una lista de diccionario, donde cada diccionario es el tipo de OT a Crear,
    y  dentro de la llave notas se retorna las notas que se desean agregar por la OT 
    este procedimiento aplica unicamente  para incidentes que ya estan abiertos"""
    list_ots = []
    for description_ot in ots_a_crear:
        ot = {
                'name':description_ot['name'], 
                'wonum':description_ot['wonum'],

            }
        for worklog in data['worklogs']:
            if ot['name'] == worklog['name']:
                ot['notes_to_create'] = worklog['notes']
                break
        list_ots.append(ot)
    return list_ots


def get_nemonico_ot(nota_maximo, ot):
    pattern = f"\({ot}-(.*?)\)"
    result = re.findall(pattern,nota_maximo)
    if result:
        return str(result[0]).upper()

def notes_to_create_inc_open(ots_existente, data, incident):
    """retorna una lista de diccionario, donde cada diccionario es la ot existente,
    y  dentro de la llave notas se retorna las notas que se desean agregar para la OT 
    este procedimiento aplica unicamente  para incidentes que ya estan abiertos"""

    try:
        list_notes = get_notes_inc(incident)
        df_notes = pd.DataFrame(list_notes)

        print(df_notes)

        list_ots = []
        for description_ot in ots_existente:
            ot = {
                    'name':description_ot['name'], 
                    'wonum':description_ot['wonum'],

                }
            #print(ot)
            list_nemonico_existente = []
            for worklog in data['worklogs']:
                if ot['name'] == worklog['name']:
                    if df_notes.empty:
                        ot['notes_to_create'] = worklog['notes']
                    else:
                        for _, row in df_notes.iterrows():
                            result = get_nemonico_ot(str(row['description']), ot['wonum'])
                            if result is not None:
                                list_nemonico_existente.append(result)
                        
                        #print(f"la lista de los nemonicos existente sobre la ot es: {list_nemonico_existente}")
                        list_notes_create = []
                        for dict_note in worklog['notes']:
                            if str(dict_note['name']).upper() not in  list_nemonico_existente:
                                list_notes_create.append(dict_note)
                        ot['notes_to_create'] = list_notes_create
                        #print(f"la lista de las notas a crear sobre la ot son: {list_notes_create}")
                    break
            list_ots.append(ot)
        #print(f"las notas a agregar son: {list_ots}")
        return list_ots
    except Exception as e:
        raise Exception(f"error en la funcion note: {traceback.format_exc()}")

def notification_teams(nodo, descripcion, mensaje, mensaje_python, error = True):
    try:
        if error:
            title = '🔔Proceso ITSM: Fallido'
            themeColor = 'FF0000'
        else:
            title = '🔔Proceso ITSM: completado'
            themeColor = '0B6623'

        card_content = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": "Notificación de incidente",
            "themeColor": themeColor,  # Color rojo para indicar un error
            "title": title,
            "sections": [
                {
                    "activityTitle": f"**Nodo:** {nodo}",
                    "activitySubtitle": f"**Descripción:** {descripcion}",
                    "text": f"**Mensaje**: {mensaje}",
                    "facts": [
                        {
                            "name": "**Error Técnico:**",
                            "value": f"{mensaje_python}"
                        }
                    ]
                }
            ]
        }

        # Convertir el contenido de la tarjeta a JSON
        card_content_json = json.dumps(card_content)

        # Enviar la solicitud POST al webhook
        response = requests.post(
            URL_WEBHOOK,
            data=card_content_json,
            headers={'Content-Type': 'application/json'}
        )

        # Verificar el resultado
        if response.status_code == 200:
            print("Notificación enviada exitosamente.")
    except Exception as e:
        print(e)
        pass