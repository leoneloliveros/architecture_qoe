from flask import Blueprint, request, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from .services_leo import searchIncidenttotal, checkOT_QC, check_open_tickets, create_incident, create_note, createTask,create_ot, get_incident,document_incident_closure
from .utils import get_node_search, get_cinum, get_info_incident, check_ots, validate_creation_ots, ots_to_create,check_tas_bo,notification_teams


main = Blueprint('main', __name__)
auth = HTTPBasicAuth()

list_description_ots_global = ['(SOC)', '(TEST)']
def get_ots_to_create(data):
    list_description_ots_lower = [nemonico.lower() for nemonico in list_description_ots_global]
    list_description_ots=[f"({worklog['name']})" for worklog in data['worklogs'] if f"({str(worklog['name']).lower()})" in list_description_ots_lower]
    return list_description_ots


# Simulación de una base de datos de usuarios
users = {
    "user_qoe": generate_password_hash("Claro2025;")
}

@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username


def retry_operation(operation, max_attempts, *args, **kwargs):
    """
        Función genérica para intentar una operación varias veces.
        
        :param operation: Función que realiza la operación.
        :param max_attempts: Número máximo de intentos.
        :param args: Argumentos posicionales para la operación.
        :param kwargs: Argumentos de palabra clave para la operación.
        :return: Resultado de la operación si tiene éxito.
        :raises: Última excepción capturada si no se logra completar la operación.
    """
    for attempt in range(max_attempts):
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            if attempt == max_attempts - 1:
                raise Exception(e)
            

def validate_data_itsm_qoe(data, required_keys = ['node_name','node_id', 'description', 'long_description', 'impact','urgency','public_url', 'worklogs']):
    """valida que los datos contenga los parametros necesarios para usar el endpoint del  itsm_qoe"""
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        notification_teams(nodo=data.get("node_name", "No especificado"),
                        descripcion=data.get("description", "No especificado"),
                        mensaje=f"El Json ingresado no contiene los parametros necesarios para el consumo del servicio: {missing_keys}",
                        mensaje_python="No aplica")
        return jsonify({'error': 'Invalid request', 'missing_keys': missing_keys}), 400
    
    for column in ('impact', 'urgency'):
        if data[column] not in ('1','2','3'):
            notification_teams(nodo=data.get("node_name", "No especificado"),
                            descripcion=data.get("description", "No especificado"),
                            mensaje=f"el campo priority debe contener el valor de '1' o '2' o '3'",
                            mensaje_python="No aplica")
            return jsonify({'error': 'Invalid request', 'missing_keys': f"The {column} field must contain the value of '1' or '2' or '3'"}), 400


def validate_data_itsm_qoe_qc(data):
    """valida que la data tenga los parametros necesarios para utilizar el endpoint: itsm_qoe_qc"""
    required_keys = ['incident','qc','public_url','worklogs']
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        notification_teams(nodo=data.get("node_name", "No especificado"),
                        descripcion=data.get("description", "No especificado"),
                        mensaje=f"El Json ingresado no contiene los parametros necesarios para el consumo del servicio itsm_qoe_qc: {missing_keys}",
                        mensaje_python="No aplica")
        return jsonify({'error': 'Invalid request', 'missing_keys': missing_keys}), 400

    if data['qc'] not in ('0', '1'):
        notification_teams(nodo=data.get("node_name", "No especificado"),
                            descripcion=data.get("description", "No especificado"),
                            mensaje=f"el campo qc debe contener el valor de '0' o '1' endpoint itsm_qoe_qc",
                            mensaje_python="No aplica")
        return jsonify({'error': 'Invalid request', 'missing_keys': f"The qc field must contain the value of '0' or '1'"}), 400


def validate_data_worklog(data):
    try:
        """Valida que los worklogs este correctamente creado para usarlos"""

        for worklog in data['worklogs']:
            required_keys = ['name', 'notes']
            missing_keys = [key for key in required_keys if key not in worklog]
            if missing_keys:
                notification_teams(nodo=data.get("node_name", "No especificado"),
                            descripcion=data.get("description", "No especificado"),
                            mensaje="El Json ingresado no contiene los parametros necesarios para el consumo del servicio",
                            mensaje_python="No aplica")
                return jsonify({'error': 'Invalid request', 'missing_keys["worklogs"]': missing_keys}), 400
            
        for worklog in data['worklogs']:
            for worklog in worklog['notes']:
                required_keys = ['name', 'note']
                missing_keys = [key for key in required_keys if key not in worklog]
                if missing_keys:
                    notification_teams(nodo=data.get("node_name", "No especificado"),
                                descripcion=data.get("description", "No especificado"),
                                mensaje="El Json ingresado no contiene los parametros necesarios para el consumo del servicio",
                                mensaje_python="No aplica")
                    return jsonify({'error': 'Invalid request', 'missing_keys["worklogs"]["notes"]': missing_keys}), 400
    except Exception as e:
        print(e)

def validate_data_itsm_close(data):
    """valida que la data contenga los parametros necesarios para usar el endpoint del  itsm_close"""
    required_keys = ['incident']
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        notification_teams(nodo=data.get("node_name", "No especificado"),
                        descripcion=data.get("description", "No especificado"),
                        mensaje=f"El Json ingresado no contiene los parametros necesarios para el consumo del servicio itsm_close: {missing_keys}",
                        mensaje_python="No aplica")
        return jsonify({'error': 'Invalid request', 'missing_keys': missing_keys}), 400



def proccess_tas_create(tas_a_crear, attempts,incident,node, route, location,cinum,data, ownergroup='BO_ALAM_HOG', wopriority='3'):
    try:
        if tas_a_crear == "":
            print(f"creacion de tas para el incidente {incident}")
            description = str(data['description']).replace("[","(").replace("]",")")
            tas_a_crear = retry_operation(operation=createTask,
                                    max_attempts=attempts,
                                    ticketid=incident, 
                                    description= description, 
                                    ownergroup=ownergroup, 
                                    route=route, 
                                    location=location, 
                                    cinum=cinum,
                                    wopriority=wopriority
                                    )
            return True, tas_a_crear
        return True, tas_a_crear
    except Exception as e:
        notification_teams(nodo=data.get("node_name", "No especificado"),
                    descripcion=data.get("description", "No especificado"),
                    mensaje=f"No se pudo crear la tarea al BO Alambrico - Incidente {incident}, por favor crear Tareas y OT necesarias de forma manual con su respectivas notas",
                    mensaje_python=str(e))
        return False, jsonify({"incident":incident,"node":node,"error": "Could not create TAS - (soap maximo)"}), 503  


def proccess_ot_new(incident, ots_a_crear,location, cinum, route, node, data, nemonico_qc="", public_url="", attempts=3):
    try:
        print(f"Proceso de creacion de nuevas OT - {incident} con sus notas: {ots_a_crear}")
        created_ot=[]
        for register_ot in ots_a_crear:
            posicion_inicio = data['description'].find("- Degradacion QoE")
            if posicion_inicio != -1:
                description_ot = data['description'][posicion_inicio:]
            else:
                description_ot = data['description'] 
            description_ot= f"({register_ot['name']}) {description_ot}"

            if nemonico_qc!="":
                description_ot = f"{nemonico_qc} - {description_ot}"
            print(description_ot)

            try:
                ot = retry_operation(operation=create_ot,
                                    max_attempts=attempts,
                                    ticketid=incident, 
                                    description_ot= description_ot, 
                                    worktype='CCOAX', 
                                    location=location, 
                                    wopriority=data['urgency'], 
                                    cinum=cinum, 
                                    route=route)
            except Exception as e:
                notification_teams(nodo=data.get("node_name", "No especificado"),
                    descripcion=data.get("description", "No especificado"),
                    mensaje=f"No se pudo crear la OT ({register_ot['name']}) para el incidente {incident} por favor crear la OT de manera manual y las notas correspondientes",
                    mensaje_python=str(e))
                return  False, jsonify({"incident":incident, "node":node,"error": "Work order could not be created - (Soap Maximo)"})


            for indice, note_ot in enumerate(register_ot['notes_to_create']):
                try:
                    if indice==0:
                        note = f"({ot}-{note_ot['name']}) URL: {public_url} {note_ot['note'][:456]}"
                    else:
                        note = f"({ot}-{note_ot['name']}) {note_ot['note'][:456]}"

                    note  = note.replace("[","(").replace("]",")")
                    print(note)
                    retry_operation(operation=create_note,max_attempts=attempts,ticketid=incident,note=note) 
                except Exception as e:
                    notification_teams(nodo=data.get("node_name", "No especificado"),
                            descripcion=data.get("description", "No especificado"),
                            mensaje=f"No se pudo crear una Nota para el incidente {incident} - {ot} por favor termine de documentar las notas de forma manual",
                            mensaje_python=str(e))
                    return False, jsonify({"incident":incident,"node":node,"error": "Note could not be created - (Soap Maximo)"})
            register_ot['wonum'] = ot
            created_ot.append(register_ot)
        return True, created_ot
    except Exception as e:
        raise Exception(f"error en proccess_ot_new: {str(e)}")


def proccess_generate_incident_gesconf(location,cinum, data, route):
    """proceso para validar la creacion de un incidente de auditoria
    cuando en cilocation contiene la palabra SDS o  esta vacia"""
    location = str(location).upper()
    node = data['node_name']
    print(location)
    if "SDS" in location or location=="NONE" or location =="":
        try:
            incident = retry_operation(
                operation=create_incident, 
                max_attempts=3, 
                description=f"Falla en la relación entre Ubicación y Nodo HCF para el nodo ({node}) cinum: {cinum}",
                longdescription=f"Falla en la relación entre Ubicación y Nodo HCF para el nodo ({node}) cinum: {cinum}",
                impact=3,
                urgency=3,
                route=route,
                location="NODO_GENERICO",
                cinum = "102",
                ownergroup='GESCONF',
                status='INPROG')
            print(f"el incidente es: {incident}")
            return incident
        except Exception as e:
            print(f"error: {e}")


@main.route('/ticket', methods=['POST'])
@auth.login_required
def only_ticket():
    try:
        
        data = request.get_json()
        response = validate_data_itsm_qoe(data = data, 
                               required_keys = ['node_name',
                                                'node_id', 
                                                'description', 
                                                'long_description', 
                                                'impact',
                                                'urgency',
                                                'public_url', 
                                                'worklogs'])
        if response:
            return response
        
        attempts=3
        node = data['node_name']
        route = '6492'
        public_url = data['public_url']

        try:
            cinum, location =get_cinum(node)
        except Exception as e:
            notification_teams(nodo=data.get("node_name", "No especificado"),
                descripcion=data.get("description", "No especificado"),
                mensaje="No se encontró cinum asociado al nodo buscado",
                mensaje_python=str(e))
            return jsonify({"incident":"","node":node,"error": "No cinum associated with the node was found (Dataguard) /ticket"}), 200

        
        try:
            all_tickets = None
            all_tickets = retry_operation(check_open_tickets, attempts, node, route)
            if all_tickets is not None:
                incident, loc, ci = get_info_incident(all_tickets, node)
            else:
                incident = None

        except Exception as e:
            notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje="No se pudo validar los incidentes abiertos",
                           mensaje_python=str(e))
            return jsonify({"incident":"","node":node,"error": "Could not validate open incidents - (Soap Maximo)"}), 503
        
        if incident is not None:
            return jsonify({"incident":incident,"incidente_nuevo":False}), 201


        try:
            route_gesconf = "6497"
            incident_auditoria=proccess_generate_incident_gesconf(location,cinum, data, route_gesconf)
            if cinum is None:
                raise Exception("No hay cinum asociado al nodo")
            if 'INC' in str(incident_auditoria):
                raise Exception(f"La locacion del nodo es invalida, locacion = {location}, se crea el incidente: {incident_auditoria} al grupo GESCONF")

            incident = retry_operation(
                operation=create_incident, 
                max_attempts=attempts, 
                description=data['description'],
                longdescription=data['long_description'],
                impact=data['impact'],
                urgency=data['urgency'],
                route=route,
                location=location,
                cinum = cinum,
                ownergroup='SOC CALIDAD',
                status='INPROG')
            
        except Exception as e:
            notification_teams(nodo=data.get("node_name", "No especificado"),
                        descripcion=data.get("description", "No especificado"),
                        mensaje="No se creó el incidente /ticket",
                        mensaje_python=str(e))
            return jsonify({"incident":"","node":node,"error": "Incident could not be created - (Soap Maximo) /ticket"}), 503                              

        list_description_ots = get_ots_to_create(data)
        ots_a_crear = ots_to_create(list_description_ots, data)

        try:
            print(f"Proceso de creacion de notas - {incident}")
            for register_ot in ots_a_crear:
                for indice, note_ot in enumerate(register_ot['notes_to_create']):
                    try:
                        if indice==0:
                            note = f"({note_ot['name']}) URL: {public_url} {note_ot['note'][:456]}"
                        else:
                            note = f"({note_ot['name']}) {note_ot['note'][:456]}"

                        note  = note.replace("[","(").replace("]",")")
                        retry_operation(operation=create_note,max_attempts=attempts,ticketid=incident,note=note) 
                    except Exception as e:
                        notification_teams(nodo=data.get("node_name", "No especificado"),
                                descripcion=data.get("description", "No especificado"),
                                mensaje=f"No se pudo crear una Nota para el incidente {incident} - por favor termine de documentar las notas de forma manual",
                                mensaje_python=str(e))
                        return False, jsonify({"incident":incident,"node":node,"error": "Note could not be created - (Soap Maximo)"})
            
            return jsonify({"incident":incident,"incidente_nuevo":True}), 201
        except Exception as e:
            raise Exception(f"error en proccess_ot_new: {str(e)}")
    
    except Exception as e:
        notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje=f"Error general - no identificado /ticket",
                           mensaje_python=str(e))
        print(e)
        print(e.__traceback__.tb_lineno)
        return jsonify({"incident":"", "node":node,"error": "general error /ticket"}), 503 





@main.route('/itsm_qoe', methods=['POST'])
@auth.login_required
def process_request():
    """Endpoint encargado de realizar la creacion del incidente para el Nodo reportado.
       la primera revision que se realiza: es revisar si existen incidentes abiertos por el automatismo
       SI:
            valida OT abiertas por parte del automatismo
            SI:
                solo agrega las notas al 
        """

    try:
        data = request.get_json()
        response = validate_data_itsm_qoe(data)
        if response:
            return response
        response = validate_data_worklog(data)
        if response:
            return response

        attempts=3
        node = data['node_name']
        node_id =data['node_id']
        route = '6492'
        public_url = data['public_url']
        #route = 'INC1005'

        #obtiene los tipos de ots que se deben validar para el incidente
        list_description_ots = get_ots_to_create(data)
        try:
            all_tickets = None
            all_tickets = retry_operation(check_open_tickets, attempts, node, route)
            if all_tickets is not None:
                incident, location, cinum = get_info_incident(all_tickets, node)
            else:
                incident = None

        except Exception as e:
            notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje="No se pudo validar los incidentes abiertos",
                           mensaje_python=str(e))
            return jsonify({"incident":"","node":node,"error": "Could not validate open incidents - (Soap Maximo)"}), 503

        if incident is None:
            print(f"proceso de creacion de incidente - Nodo: {node}")
            nodes = get_node_search(node)
            for name in nodes:
                try:
                    print(f"el nodo es {name}")
                    list_incident = retry_operation(
                        operation=searchIncidenttotal, 
                        max_attempts=attempts, 
                        node=name)
                except Exception as e:
                    notification_teams(nodo=data.get("node_name", "No especificado"),
                            descripcion=data.get("description", "No especificado"),
                            mensaje="No se pudo validar los incidentes de fuera de servicio",
                            mensaje_python=str(e))
                    return jsonify({"incident":"","node":name,"error": "Could not validate incidents fs - (Soap Maximo)"}), 503
                
                if len(list_incident)>0:
                    list_incident = [register['ticketid'] for register in list_incident]
                    notification_teams(nodo=data.get("node_name", "No especificado"),
                            descripcion=data.get("description", "No especificado"),
                            mensaje=f"Existen incidentes de fuera de servicio creados en los últimos 3 días y menor o igual a 4 horas antes de la fecha y hora actual: {list_incident}",
                            mensaje_python="NA",
                            error=False)
                    return jsonify({"incident":list_incident,"node":name,"error": "Existen incidentes de fuera de servicio creados en los últimos 3 días y menor o igual a 4 horas antes de la fecha y hora actual"}), 200


            try:
                cinum, location =get_cinum(node)
            except Exception as e:
                notification_teams(nodo=data.get("node_name", "No especificado"),
                    descripcion=data.get("description", "No especificado"),
                    mensaje="No se encontró cinum asociado al nodo",
                    mensaje_python=str(e))
                return jsonify({"incident":"","node":name,"error": "No cinum associated with the node was found (Dataguard)"}), 200

            ots_existente = []
            ots_a_crear = ots_to_create(list_description_ots, data)
            tas_a_crear = ""
            try:
                route_gesconf = "6497"
                incident_auditoria=proccess_generate_incident_gesconf(location,cinum, data, route_gesconf)
                print(incident_auditoria)
                if cinum is None:
                    raise Exception("No hay cinum asociado al nodo")
                if 'INC' in str(incident_auditoria):
                    raise Exception(f"La locacion del nodo es invalida, locacion = {location}, se crea el incidente: {incident_auditoria} al grupo GESCONF")

                incident = retry_operation(
                    operation=create_incident, 
                    max_attempts=attempts, 
                    description=data['description'],
                    longdescription=data['long_description'],
                    impact=data['impact'],
                    urgency=data['urgency'],
                    route=route,
                    location=location,
                    cinum = cinum,
                    ownergroup='SOC CALIDAD',
                    status='INPROG')

            except Exception as e:
                notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje="No se pudo crear el incidente",
                           mensaje_python=str(e))
                return jsonify({"incident":"","node":node,"error": "Incident could not be created - (Soap Maximo)"}), 503                              
        else:
            try:
                print(f"proceso para validacion de OTs abiertas del incidente {incident}")
                list_ots_open = check_ots(incident)
            except Exception as e:
                notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje=f"No se pudo validar las ots abiertas para el incidente {incident}",
                           mensaje_python=str(e))
                return jsonify({"incident":incident,"node":node,"error": "Could not validate open work orders - (Dataguard)"}), 503

            try:
                print(f"proceso para validacion de OTs existentes y abiertas: {incident}")
                ots_existente, ots_a_crear = validate_creation_ots(list_ots_open, list_description_ots, data,incident)
                tas_a_crear = check_tas_bo(incident, 'BO_ALAM_HOG')
            except Exception as e:
                notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje=f"Error en la revision de notas de las OTs existente para el incidente: {incident}",
                           mensaje_python=str(e))
                return jsonify({"incident":incident,"node":node,"error": "Error in reviewing the notes of the existing OTs for the incident:"}), 200

        # return jsonify({"incident":incident}), 201
        #'BO_ALAM_HOG'
        bandera,  tas_a_crear = proccess_tas_create(tas_a_crear, attempts,incident,node, route, location,cinum,data,'BO_ALAM_HOG',data['urgency'])
        if bandera == False:
            return tas_a_crear

        bandera, created_ot = proccess_ot_new(incident, ots_a_crear,location, cinum, route, node, data,"",public_url, attempts)
        if bandera == False:
            return created_ot, 503
        
        print(f"Proceso de creacion de notas sobre OTs existente - {incident}")
        print(ots_existente)
        for register_ot in ots_existente:
            ot = register_ot['wonum']
            for note_ot in register_ot['notes_to_create']:
                try:
                    note  = f"({ot}-{note_ot['name']}) {note_ot['note'][:456]}"
                    note  = note.replace("[","(").replace("]",")")
                    retry_operation(operation=create_note,max_attempts=attempts,ticketid=incident,note=note) 
                except Exception as e:
                    notification_teams(nodo=data.get("node_name", "No especificado"),
                            descripcion=data.get("description", "No especificado"),
                            mensaje=f"No se pudo crear una Nota para el incidente {incident} - {ot} por favor termine de documentar las notas de forma manual",
                            mensaje_python=str(e))
                    return jsonify({"incident":incident,"node":node,"error": "Note could not be created - (Soap Maximo)"}), 503  
        
        return jsonify({'incident': incident, "existing_ot":ots_existente, "created_ot":created_ot, "tas_bo":tas_a_crear}), 201
        
    except Exception as e:
        notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje=f"Error general - no identificado",
                           mensaje_python=str(e))
        print(e)
        print(e.__traceback__.tb_lineno)
        return jsonify({"incident":"", "node":node,"error": "general error"}), 503 
    

@main.route('/itsm_qoe_qc', methods=['POST'])
@auth.login_required
def process_request_qc():
    try:
        data = request.get_json()
        response = validate_data_itsm_qoe_qc(data)
        if response:
            return response
        response = validate_data_worklog(data)
        if response:
            return response
    
        try:
            dict_inf_incident = get_incident(data['incident'])
        except Exception as e:
            notification_teams(nodo=data.get("node_name", "No especificado"),
                        descripcion=data.get("description", "No especificado"),
                        mensaje=f"No se pudo crear la tarea al BO Alambrico - Incidente {data['incident']}, por favor crear Tareas y OT necesarias de forma manual con su respectivas notas",
                        mensaje_python=str(e))
            return jsonify({"incident":data['incident'],"node":"node","error": "Could not create TAS - (soap maximo)"}), 503  

        if dict_inf_incident['status'] in ['CERRADO', 'CANCELADO']:
            notification_teams(nodo=data.get("node_name", "No especificado"),
                            descripcion=data.get("description", "No especificado"),
                            mensaje=f"El incidente no esta abierto {data['incident']}",
                            mensaje_python="NA", error=False)
            return jsonify ({'incident':data['incident'], 'error':'The incident is not open'}), 200
        
        attempts = 3
        incident = data['incident']
        route = dict_inf_incident['classstructureid']
        location = dict_inf_incident['location']
        node = location
        cinum = dict_inf_incident['cinum']
        data['urgency'] = dict_inf_incident['internalpriority']
        data['description'] = dict_inf_incident['description']

        nemonico_qc=""
        if data["qc"]=='1':
            nemonico_qc="QC"
        list_description_ots = get_ots_to_create(data)
        ots_a_crear = ots_to_create(list_description_ots, data)
        
        tas_a_crear = check_tas_bo(incident, 'BO_ALAM_HOG')
        bandera, tas_a_crear = proccess_tas_create(tas_a_crear, attempts,incident,node, route, location,cinum,data, 'BO_ALAM_HOG',data['urgency'])
        if bandera == False:
            return tas_a_crear
        
        # print(f"¿Necesitamos crear tarea? {bandera}")
        #print(f"las ots a crear son: {ots_a_crear}")

        ots_a_crear_aux = []
        for ot in ots_a_crear:
            nemonico = ot['name']
            result = checkOT_QC(ticketid=incident, nemonico=nemonico)
            if len(result)==0:
                ots_a_crear_aux.append(ot)
        ots_a_crear = ots_a_crear_aux
        if len(ots_a_crear)<=0:
            notification_teams(nodo=node,
                            descripcion=data.get("description", "No especificado"),
                            mensaje=f"El incidente: {incident} cuenta con una OT abierta de tipo: {nemonico}",
                            mensaje_python="NA",
                            error=False)
            return jsonify({"incident":incident, "node":node, "error": f"The incident already has an OT of type: {nemonico}"}), 200

        bandera, created_ot = proccess_ot_new(incident, ots_a_crear,location, cinum, route, node, data, nemonico_qc,data['public_url'], attempts)
        if bandera == False:
            return created_ot
        
        return jsonify({'incident': incident, "existing_ot":[], "created_ot":created_ot, "tas_bo":tas_a_crear}), 201
        
    except Exception as e:
        notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje=f"Error general endpoint: itsm_qoe_qc -  incidente: {data.get('incident', 'No especificado')}",
                           mensaje_python=str(e))
        print(e.__traceback__.tb_lineno)
        print(e)
        return jsonify({"incident":data.get("incident","No especificado"), "error": "general error endpoint itsm_qoe_qc"}), 503 
    

@main.route('/itsm_close', methods=['POST'])
@auth.login_required
def process_request_close():
    try:
        failurecode="REQUERIMIENTOS INTERNOS"
        problem="REPORTES"
        cause="REPORTE AMPLIADO"
        remedy="ENVIO DE REPORTE"
        incsolucion="test de prueba"
        data = request.get_json()
        response = validate_data_itsm_close(data)
        if response:
            return response
        
        dict_inf_incident = get_incident(data['incident'])
        if dict_inf_incident['status'] in ['CERRADO', 'CANCELADO']:
            return jsonify ({'incident':data['incident'], 'error':'The incident is not open'}), 200
        
        list_ots_open = check_ots(data['incident'])
        result=[]
        for ot in list_ots_open:
            print(ot['wonum'])
            if "TAS" not in ot['wonum']:
                result.append(ot)
        if len(result):
            return jsonify ({'incident':data['incident'], 'error':f'The incident has an open work order: {result}'}), 400

        
        for column in ['failurecode','problemcode','fr1code','fr2code']:
            if dict_inf_incident[column] is None or dict_inf_incident[column]=="":
                return jsonify ({'incident':data['incident'], 'error':'The incident has no cause for closure or is incomplete'}), 400
                # solo_cierre=False
                # break
        
        solo_cierre=True
        document_incident_closure(ticketid= data['incident'],
                                failurecode=failurecode,
                                problemcode=problem,
                                fr1code=cause,
                                fr2code=remedy,
                                incsolucion=incsolucion,  
                                solo_cierre=solo_cierre)
            
        return jsonify({"success":"ok"}),200

    except Exception as e:
        notification_teams(nodo=data.get("node_name", "No especificado"),
                           descripcion=data.get("description", "No especificado"),
                           mensaje=f"Error general endpoint: itsm_close - no identificado para el incidente: {data.get('incident', 'No especificado')}",
                           mensaje_python=str(e))
        # print(e.__traceback__.tb_lineno)
        # print(e)
        return jsonify({"incident":data.get("incident","No especificado"), "error": "general error endpoint itsm_qoe_qc"}), 503 



@main.route('/test', methods=['POST'])
@auth.login_required
def test():
    """test para probar funcionamiento de la api de generacion de incidente a al grupo GESCONF"""
    location = None
    cinum = "102"
    data = {'node_name':"test"}
    route = "6497"
    incident = proccess_generate_incident_gesconf(location,cinum, data, route)
    print(incident)
    return jsonify({"result":"success", "incident":incident}), 201