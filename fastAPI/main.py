#!/usr/bin/env python3
from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.responses import StreamingResponse, JSONResponse, Response
from fastapi import status

import os
import io
import re
import json
import gzip
import base64
import hashlib
import time
import secrets
from typing import Any, Dict, List, Optional

import requests
import paramiko
from dotenv import load_dotenv

# ============================================================
# Cargar .env desde la "raíz" (misma carpeta de este main.py)
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

app = FastAPI(
    title="API Gateway - SOP (SFTP + MedUX + PILOT Fake)",
    description="Gateway: descarga archivos desde SFTP, proxy a MedUX, y API fake PILOT desde snapshots en SFTP.",
    version="2.5.0",
)

# ============================================================
# Configuración deployment PILOT (para prefijo real)
# Real: http://{{PILOT_Manager_IP}}/{{PMF_deployment}}/{{PILOT_Manager_deployment}}/...
# Fake debe exponer EXACTAMENTE el mismo path base.
# ============================================================
PMF_DEPLOYMENT = os.getenv("PMF_deployment", "pmf")
PILOT_MANAGER_DEPLOYMENT = os.getenv("PILOT_Manager_deployment", "pms31")
FAKE_PILOT_PREFIX = f"/{PMF_DEPLOYMENT}/{PILOT_MANAGER_DEPLOYMENT}"

# ============================================================
# Credenciales PILOT (para validar body del /users/token fake)
# OJO: en tu collector usas pilot_userName / pilot_password
# ============================================================
PILOT_USER = os.getenv("pilot_userName", os.getenv("PILOT_USER", ""))
PILOT_PASSWORD = os.getenv("pilot_password", os.getenv("PILOT_PASSWORD", ""))

# ============================================================
# Token fake con TTL (solo aplica a endpoints lógicos SKIP)
# ============================================================
FAKE_TOKEN_TTL_SECONDS = int(os.getenv("FAKE_TOKEN_TTL_SECONDS", "3600"))
_TOKEN_STORE: Dict[str, Dict[str, Any]] = {}


def _now() -> int:
    return int(time.time())


def _cleanup_expired_tokens():
    now = _now()
    expired = [t for t, v in _TOKEN_STORE.items() if int(v.get("expires_at", 0)) <= now]
    for t in expired:
        _TOKEN_STORE.pop(t, None)


def _issue_token() -> Dict[str, Any]:
    _cleanup_expired_tokens()
    access_token = secrets.token_hex(32)
    refresh_token = secrets.token_hex(32)
    expires_in = FAKE_TOKEN_TTL_SECONDS
    _TOKEN_STORE[access_token] = {
        "expires_at": _now() + expires_in,
        "refresh_token": refresh_token,
    }
    # estructura tipo OAuth-ish
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": expires_in,
        "token_type": "Bearer",
    }


def _extract_bearer_token(auth_header: Optional[str]) -> Optional[str]:
    if not auth_header:
        return None
    s = auth_header.strip()
    if not s.lower().startswith("bearer "):
        return None
    token = s.split(" ", 1)[1].strip()
    return token or None


def _is_token_valid(token: str) -> bool:
    info = _TOKEN_STORE.get(token)
    if not info:
        return False
    return int(info.get("expires_at", 0)) > _now()


# ✅ SOLO estos requieren token (los “skip” lógicos)
AUTH_ONLY = {
    # device lógicos
    "/pilot-manager-open-api/device/devicesByIds",
    "/pilot-manager-open-api/device/device",
    "/pilot-manager-open-api/device/devicesByType",
    # mux lógicos
    "/pilot-manager-open-api/mux_agent/getMuxIos",
    "/pilot-manager-open-api/mux_agent/getMuxServices",
    "/pilot-manager-open-api/mux_agent/getMuxOutputServices",
    # live lógicos
    "/pilot-manager-open-api/live_agent/getLiveServices",
    "/pilot-manager-open-api/live_agent/liveServiceConfigurationDetails",
}


@app.middleware("http")
async def pilot_auth_middleware(request: Request, call_next):
    """
    Valida token SOLO para endpoints lógicos (SKIP).
    Importante: como ahora PILOT está bajo /{pmf}/{pms31}, calculamos relative_path.
    """
    path = request.url.path

    # Si el request llega con el prefijo real, lo removemos para comparar con AUTH_ONLY
    relative_path = path
    if path.startswith(FAKE_PILOT_PREFIX):
        relative_path = path[len(FAKE_PILOT_PREFIX):]
        if not relative_path.startswith("/"):
            relative_path = "/" + relative_path

    if relative_path in AUTH_ONLY:
        auth = request.headers.get("authorization")
        token = _extract_bearer_token(auth)

        if not token:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=["Authorization is required"],
            )

        _cleanup_expired_tokens()
        if not _is_token_valid(token):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=["Token expired or invalid"],
            )

    return await call_next(request)


# ============================================================
# Configuración SFTP (común)
# ============================================================
SFTP_HOST = os.getenv("SFTP_HOST", "100.72.137.94")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER", "poller_user")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD", "Noviembre2025*")

SFTP_BASE_PATH = os.getenv("SFTP_BASE_PATH", "/upload")
SFTP_FAKE_BASE_PATH = os.getenv("SFTP_FAKE_BASE_PATH", "/upload/pilot_fake")

# ============================================================
# Configuración MedUX
# ============================================================
MEDUX_BASE_URL = os.getenv("MEDUX_BASE_URL", "https://portal-co.medux.app")
MEDUX_VERIFY_SSL = os.getenv("MEDUX_VERIFY_SSL", "true").strip().lower() != "false"

# ============================================================
# Configuración Postman Collection
# ============================================================
POSTMAN_COLLECTION_PATH = os.getenv(
    "POSTMAN_COLLECTION_PATH",
    os.path.join(BASE_DIR, "PILOT Manager REST API.postman_collection.json"),
)

_loaded_get_endpoints: List[Dict[str, str]] = []

# ============================================================
# Helpers SFTP
# ============================================================
def get_sftp_client():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return transport, sftp


def stream_sftp_file(filename: str, media_type: str):
    remote_path = f"{SFTP_BASE_PATH.rstrip('/')}/{filename}"

    transport = None
    sftp = None
    try:
        transport, sftp = get_sftp_client()
        file_obj = io.BytesIO()
        sftp.getfo(remote_path, file_obj)
        file_obj.seek(0)

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al acceder al SFTP: {str(e)}")
    finally:
        if sftp is not None:
            sftp.close()
        if transport is not None:
            transport.close()

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(file_obj, media_type=media_type, headers=headers)

# ============================================================
# ENDPOINTS SFTP (archivos)
# ============================================================
@app.get("/archivos/cm_levels_{yyyymmdd}.zip")
def get_cm_levels(yyyymmdd: str):
    return stream_sftp_file(f"cm_levels_{yyyymmdd}.zip", "application/zip")


@app.get("/archivos/CMsfueradenivel.zip")
def get_cm_fuera_de_nivel():
    return stream_sftp_file("CMsfueradenivel.zip", "application/zip")


@app.get("/archivos/fw_cm_completo.zip")
def get_fw_cm_completo():
    return stream_sftp_file("fw_cm_completo.zip", "application/zip")


@app.get("/archivos/fw_cm_{yyyymmdd}.zip")
def get_fw_cm(yyyymmdd: str):
    return stream_sftp_file(f"fw_cm_{yyyymmdd}.zip", "application/zip")


@app.get("/archivos/ontxoltxcuenta.zip")
def get_ontx_oltx_cuenta():
    return stream_sftp_file("ontxoltxcuenta.zip", "application/zip")


@app.get("/archivos/marcacionNodosPoller.zip")
def get_marcacion_nodos_poller():
    return stream_sftp_file("marcacionNodosPoller.zip", "application/zip")


@app.get("/archivos/nodosPollervsRR.zip")
def get_nodos_poller_vs_rr():
    return stream_sftp_file("nodosPollervsRR.zip", "application/zip")


@app.get("/archivos/dailyNodes_{yyyymmdd}.zip")
def get_daily_nodes(yyyymmdd: str):
    return stream_sftp_file(f"dailyNodes_{yyyymmdd}.zip", "application/zip")


@app.get("/archivos/NodeInformation.zip")
def get_node_information():
    return stream_sftp_file("NodeInformation.zip", "application/zip")


@app.get("/archivos/rosa_{yyyymmdd}.txt")
def get_rosa(yyyymmdd: str):
    return stream_sftp_file(f"rosa_{yyyymmdd}.txt", "text/plain")


@app.get("/archivos/aurora_{yyyymmdd}.txt")
def get_aurora(yyyymmdd: str):
    return stream_sftp_file(f"aurora_{yyyymmdd}.txt", "text/plain")

# ============================================================
# PROXY MedUX
# ============================================================
@app.post("/api/export")
def medux_export(body: dict = Body(...)):
    url = f"{MEDUX_BASE_URL}/api/export"
    try:
        resp = requests.post(url, json=body, verify=MEDUX_VERIFY_SSL)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error conectando a MedUX export: {str(e)}")

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return JSONResponse(content=resp.json(), status_code=resp.status_code)


@app.get("/api/export/progress/{export_id}")
def medux_export_progress(export_id: str):
    url = f"{MEDUX_BASE_URL}/api/export/progress/{export_id}"
    try:
        resp = requests.get(url, verify=MEDUX_VERIFY_SSL)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error conectando a MedUX progress: {str(e)}")

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return JSONResponse(content=resp.json(), status_code=resp.status_code)


@app.post("/api/export/download/{export_id}")
def medux_export_download(export_id: str, body: dict = Body(...)):
    url = f"{MEDUX_BASE_URL}/api/export/download/{export_id}"

    try:
        resp = requests.post(url, json=body, verify=MEDUX_VERIFY_SSL)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error conectando a MedUX download: {str(e)}")

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    try:
        csv_bytes = gzip.decompress(resp.content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No se pudo descomprimir el .gz de MedUX: {str(e)}")

    file_obj = io.BytesIO(csv_bytes)
    file_obj.seek(0)

    content_disp = resp.headers.get("Content-Disposition", "")
    filename = None
    if content_disp:
        m = re.search(r'filename="?([^";]+)"?', content_disp)
        if m:
            filename = m.group(1)

    if filename and filename.endswith(".gz"):
        filename = filename[:-3]
    if not filename:
        filename = f"medux_export_{export_id}.csv"

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(file_obj, media_type="text/csv", headers=headers)

# ============================================================
# PILOT Fake (snapshots desde SFTP)
# ============================================================
def _extract_requests_from_postman(item: Dict[str, Any], acc: List[Dict[str, Any]]):
    if "request" in item and isinstance(item["request"], dict):
        acc.append(item)
        return
    for ch in item.get("item", []) or []:
        _extract_requests_from_postman(ch, acc)


def _normalize_local_path(url_obj: Any) -> Optional[str]:
    if not isinstance(url_obj, dict):
        return None
    path = url_obj.get("path")
    if not isinstance(path, list) or not path:
        return None

    drop = 0
    if len(path) >= 1 and isinstance(path[0], str) and "PMF_deployment" in path[0]:
        drop = 1
    if len(path) >= 2 and isinstance(path[1], str) and "PILOT_Manager_deployment" in path[1]:
        drop = 2

    suffix = path[drop:]
    if not suffix:
        return None

    return "/" + "/".join(str(p).strip("/") for p in suffix)


def endpoint_id(method: str, local_path: str, query: Dict[str, str]) -> str:
    q = "&".join([f"{k}={query[k]}" for k in sorted(query.keys())])
    sig = f"{method.upper()} {local_path}?{q}"
    h = hashlib.md5(sig.encode("utf-8")).hexdigest()[:10]
    safe_path = local_path.strip("/").replace("/", "_").replace("{", "").replace("}", "")
    return f"{method.lower()}__{safe_path}__{h}"


def read_latest_snapshot(sftp: paramiko.SFTPClient, ep_id: str) -> Dict[str, Any]:
    """
    Lee snapshot exacto; si no existe, fallback por prefijo (method__path__) al folder más nuevo.
    """
    base = SFTP_FAKE_BASE_PATH.rstrip("/")
    folder = f"{base}/{ep_id}"

    def _read_latest_in_folder(folder_path: str) -> Dict[str, Any]:
        entries = sftp.listdir_attr(folder_path)
        if not entries:
            raise HTTPException(status_code=404, detail=f"Carpeta vacía: {folder_path}")
        latest = max(entries, key=lambda e: e.st_mtime)
        remote_file = f"{folder_path}/{latest.filename}"
        with sftp.file(remote_file, "rb") as f:
            data = f.read()
        return json.loads(data.decode("utf-8"))

    try:
        return _read_latest_in_folder(folder)
    except FileNotFoundError:
        pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo snapshot exacto {folder}: {str(e)}")

    prefix = ep_id.rsplit("__", 1)[0] + "__"

    try:
        dirs = sftp.listdir_attr(base)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No se pudo listar base SFTP {base}: {str(e)}")

    candidates = [d for d in dirs if d.filename.startswith(prefix)]
    if not candidates:
        raise HTTPException(status_code=404, detail=f"No hay snapshots en SFTP para endpoint. prefix={prefix}")

    newest_dir = max(candidates, key=lambda d: d.st_mtime)
    fallback_folder = f"{base}/{newest_dir.filename}"

    try:
        return _read_latest_in_folder(fallback_folder)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo snapshot fallback {fallback_folder}: {str(e)}")


async def serve_from_snapshot(local_path: str, request: Request):
    query = {k: v for k, v in request.query_params.items()}
    ep_id = endpoint_id("GET", local_path, query)

    transport, sftp = get_sftp_client()
    try:
        snap = read_latest_snapshot(sftp, ep_id)
    finally:
        try:
            sftp.close()
            transport.close()
        except Exception:
            pass

    status_code = int(snap.get("status_code", 200))
    headers = snap.get("headers", {}) or {}
    body_b64 = snap.get("body_b64", "")

    try:
        body = base64.b64decode(body_b64.encode("ascii")) if body_b64 else b""
    except Exception:
        body = b""

    content_type = headers.get("Content-Type") or headers.get("content-type") or "application/json"
    return Response(content=body, status_code=status_code, media_type=content_type)

# ============================================================
# Helpers genéricos
# ============================================================
def _parse_one_str(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s if s else None


def _parse_list_param(raw: Optional[str], request: Request, param_name: str) -> List[str]:
    """
    Soporta:
      - ?param=["a","b"]
      - ?param=a
      - ?param=a,b
      - ?param=a&param=b
    Devuelve lista UNIQUE preservando orden.
    """
    values: List[str] = []

    multi = request.query_params.getlist(param_name)
    if multi and len(multi) > 1:
        values = [str(x) for x in multi]
    else:
        if not raw:
            return []
        raw = raw.strip()

        if raw.startswith("[") and raw.endswith("]"):
            try:
                arr = json.loads(raw)
                values = [str(x) for x in arr] if isinstance(arr, list) else [str(arr)]
            except Exception:
                inner = raw[1:-1].strip()
                values = [x.strip().strip('"').strip("'") for x in inner.split(",") if x.strip()] if inner else []
        else:
            if "," in raw:
                values = [x.strip().strip('"').strip("'") for x in raw.split(",") if x.strip()]
            else:
                values = [raw.strip().strip('"').strip("'")]

    out: List[str] = []
    seen = set()
    for v in values:
        v = v.strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _extract_list_from_body(body: Any) -> List[Dict[str, Any]]:
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if isinstance(body, dict):
        for key in ("items", "data", "results", "services", "muxes", "devices"):
            val = body.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def _load_json_latest_for_path(path: str) -> Optional[Any]:
    """
    Carga el snapshot MÁS RECIENTE para este path, sin importar qué query haya sido usado por el collector.
    """
    ep = endpoint_id("GET", path, {})  # query vacío => cae al fallback por prefijo
    transport, sftp = get_sftp_client()
    try:
        snap = read_latest_snapshot(sftp, ep)
    finally:
        try:
            sftp.close()
            transport.close()
        except Exception:
            pass

    body_b64 = snap.get("body_b64", "")
    try:
        raw_body = base64.b64decode(body_b64.encode("ascii")) if body_b64 else b""
        if not raw_body:
            return None
        return json.loads(raw_body.decode("utf-8"))
    except Exception:
        return None

# ============================================================
# DEVICE endpoints (lógica desde /device/devices)
# ============================================================
def _parse_ids_param(raw: Optional[str], request: Request) -> List[str]:
    multi = request.query_params.getlist("ids")
    if multi and len(multi) > 1:
        values = multi
    else:
        if not raw:
            return []
        raw = raw.strip()

        if raw.startswith("[") and raw.endswith("]"):
            try:
                arr = json.loads(raw)
                values = [str(x) for x in arr] if isinstance(arr, list) else [str(arr)]
            except Exception:
                inner = raw[1:-1].strip()
                values = [x.strip().strip('"').strip("'") for x in inner.split(",") if x.strip()] if inner else []
        else:
            if "," in raw:
                values = [x.strip().strip('"').strip("'") for x in raw.split(",") if x.strip()]
            else:
                values = [raw.strip().strip('"').strip("'")]

    out: List[str] = []
    seen = set()
    for v in values:
        v = v.strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _load_devices_from_snapshot() -> List[Dict[str, Any]]:
    base_path = "/pilot-manager-open-api/device/devices"
    body = _load_json_latest_for_path(base_path)
    return _extract_list_from_body(body)


@app.get(f"{FAKE_PILOT_PREFIX}/pilot-manager-open-api/device/devicesByIds")
async def pilot_devices_by_ids(request: Request, ids: Optional[str] = None):
    """
    Regla:
      - ids es array (string JSON) o múltiples params.
      - Si alguno NO existe => ["Cannot get devices by ids"]
      - Repite el mismo id => solo 1 (dedupe)
    """
    ids_list = _parse_ids_param(ids, request)
    if not ids_list:
        return JSONResponse(content=["Cannot get devices by ids"], status_code=400)

    devices_list = _load_devices_from_snapshot()
    by_id: Dict[str, Dict[str, Any]] = {}
    for d in devices_list:
        did = d.get("id")
        if did is not None:
            by_id[str(did)] = d

    for did in ids_list:
        if did not in by_id:
            return JSONResponse(content=["Cannot get devices by ids"], status_code=400)

    result = [by_id[did] for did in ids_list]
    return JSONResponse(content=result, status_code=200)


@app.get(f"{FAKE_PILOT_PREFIX}/pilot-manager-open-api/device/device")
async def pilot_device(request: Request, id: Optional[str] = None):
    """
    Regla:
      - recibe un solo id
      - si no existe => ["Cannot get one device"]
      - si existe => objeto JSON
    """
    device_id = _parse_one_str(id)
    if not device_id:
        return JSONResponse(content=["Cannot get one device"], status_code=400)

    devices_list = _load_devices_from_snapshot()
    for d in devices_list:
        if str(d.get("id", "")) == device_id:
            return JSONResponse(content=d, status_code=200)

    return JSONResponse(content=["Cannot get one device"], status_code=400)


@app.get(f"{FAKE_PILOT_PREFIX}/pilot-manager-open-api/device/devicesByType")
async def pilot_devices_by_type(request: Request, type: Optional[str] = None):
    """
    Regla:
      - Si NO envían type => "Impossible to get variable for query"
      - Si envían type => filtra por substring (case-insensitive) en key 'type'
      - Si no hay match => []
    """
    qtype = _parse_one_str(type)
    if not qtype:
        return Response(content="Impossible to get variable for query", media_type="text/plain", status_code=400)

    qtype_l = qtype.lower()
    devices_list = _load_devices_from_snapshot()
    result: List[Dict[str, Any]] = []
    for d in devices_list:
        d_type = d.get("type")
        if d_type is None:
            continue
        if qtype_l in str(d_type).lower():
            result.append(d)

    return JSONResponse(content=result, status_code=200)

# ============================================================
# LIVE endpoints
# ============================================================
@app.get(f"{FAKE_PILOT_PREFIX}/pilot-manager-open-api/live_agent/getLiveServices")
async def pilot_get_live_services(request: Request, liveId: Optional[str] = None):
    """
    /pilot-manager-open-api/live_agent/getLiveServices?liveId=[...]
    - Carga el snapshot más reciente del PATH y filtra por key 'liveId'
    - Si no hay match => []
    - Si no envían liveId => []
    """
    live_ids = _parse_list_param(liveId, request, "liveId")
    if not live_ids:
        return JSONResponse(content=[], status_code=200)

    path = "/pilot-manager-open-api/live_agent/getLiveServices"
    base_body = _load_json_latest_for_path(path)
    all_items = _extract_list_from_body(base_body)

    wanted = set(str(x) for x in live_ids)
    result: List[Dict[str, Any]] = []
    for it in all_items:
        lid = it.get("liveId")
        if lid is None:
            continue
        if str(lid) in wanted:
            result.append(it)

    return JSONResponse(content=result, status_code=200)


@app.get(f"{FAKE_PILOT_PREFIX}/pilot-manager-open-api/live_agent/liveServiceConfigurationDetails")
async def pilot_live_service_configuration_details_from_base(
    request: Request,
    liveId: Optional[str] = None,
    serviceUid: Optional[str] = None
):
    """
    Lee base consolidada:
      /upload/pilot_fake/__bases__/liveServiceConfigurationDetails_base/latest.json
    Filtra por (liveId, serviceUid) y retorna el body (JSON) guardado en upstream.
    """
    lid = _parse_one_str(liveId)
    suid = _parse_one_str(serviceUid)

    if not lid or not suid:
        return JSONResponse(content={}, status_code=200)

    transport, sftp = get_sftp_client()
    try:
        base_file = f"{SFTP_FAKE_BASE_PATH.rstrip('/')}/__bases__/liveServiceConfigurationDetails_base/latest.json"
        with sftp.file(base_file, "rb") as f:
            payload = json.loads(f.read().decode("utf-8"))
    except FileNotFoundError:
        return JSONResponse(content={"error": "Base file not found"}, status_code=500)
    finally:
        try:
            sftp.close()
            transport.close()
        except Exception:
            pass

    items = payload.get("items", [])
    if not isinstance(items, list):
        return JSONResponse(content={}, status_code=200)

    for it in items:
        if not isinstance(it, dict):
            continue
        if str(it.get("liveId", "")) == str(lid) and str(it.get("serviceUid", "")) == str(suid):
            upstream = it.get("upstream", {}) or {}
            body = upstream.get("body", {})
            return JSONResponse(content=body, status_code=200)

    return JSONResponse(content={}, status_code=200)

# ============================================================
# MUX endpoints
# ============================================================
@app.get(f"{FAKE_PILOT_PREFIX}/pilot-manager-open-api/mux_agent/getMuxIos")
async def pilot_get_mux_ios(request: Request, muxIds: Optional[str] = None):
    """
    /pilot-manager-open-api/mux_agent/getMuxIos?muxIds=[...]
    Base: /pilot-manager-open-api/mux_agent/getAllMuxIos
    """
    mux_ids = _parse_list_param(muxIds, request, "muxIds")
    if not mux_ids:
        return JSONResponse(content=[], status_code=200)

    base_path = "/pilot-manager-open-api/mux_agent/getAllMuxIos"
    base_body = _load_json_latest_for_path(base_path)
    mux_list = _extract_list_from_body(base_body)

    by_muxid: Dict[str, Dict[str, Any]] = {}
    for m in mux_list:
        mid = m.get("muxId")
        if mid is not None:
            by_muxid[str(mid)] = m

    result: List[Dict[str, Any]] = []
    for mid in mux_ids:
        if mid in by_muxid:
            result.append(by_muxid[mid])

    return JSONResponse(content=result, status_code=200)


@app.get(f"{FAKE_PILOT_PREFIX}/pilot-manager-open-api/mux_agent/getMuxServices")
async def pilot_get_mux_services(request: Request, muxId: Optional[str] = None):
    """
    /pilot-manager-open-api/mux_agent/getMuxServices?muxId=[...]
    Carga snapshot más reciente del PATH y filtra por muxId.
    """
    mux_ids = _parse_list_param(muxId, request, "muxId")
    if not mux_ids:
        return JSONResponse(content=[], status_code=200)

    path = "/pilot-manager-open-api/mux_agent/getMuxServices"
    base_body = _load_json_latest_for_path(path)
    all_items = _extract_list_from_body(base_body)

    wanted = set(str(x) for x in mux_ids)
    result: List[Dict[str, Any]] = []
    for it in all_items:
        mid = it.get("muxId")
        if mid is None:
            continue
        if str(mid) in wanted:
            result.append(it)

    return JSONResponse(content=result, status_code=200)


@app.get(f"{FAKE_PILOT_PREFIX}/pilot-manager-open-api/mux_agent/getMuxOutputServices")
async def pilot_get_mux_output_services(
    request: Request,
    muxId: Optional[str] = None,
    outputUid: Optional[str] = None
):
    """
    /pilot-manager-open-api/mux_agent/getMuxOutputServices?muxId="..."&outputUid="141"
    Busca dentro del snapshot más reciente de /pilot-manager-open-api/mux_agent/getMuxServices:
      1) outputs con keys (uid + muxId)
      2) fallback: programas con keys (muxOutputUid + muxId)
    Si no hay match => []
    """
    mid = _parse_one_str(muxId)
    out_uid_raw = _parse_one_str(outputUid)

    if not mid or not out_uid_raw:
        return JSONResponse(content=[], status_code=200)

    out_uid_int: Optional[int] = None
    try:
        out_uid_int = int(out_uid_raw)
    except Exception:
        out_uid_int = None

    path = "/pilot-manager-open-api/mux_agent/getMuxServices"
    base_body = _load_json_latest_for_path(path)

    def walk(obj: Any) -> List[Dict[str, Any]]:
        found: List[Dict[str, Any]] = []
        if isinstance(obj, dict):
            found.append(obj)
            for v in obj.values():
                found.extend(walk(v))
        elif isinstance(obj, list):
            for x in obj:
                found.extend(walk(x))
        return found

    dicts = walk(base_body)

    # 1) outputs: uid + muxId
    outputs: List[Dict[str, Any]] = []
    for d in dicts:
        if "uid" in d and "muxId" in d:
            if str(d.get("muxId")) == str(mid):
                uid_val = d.get("uid")
                if (out_uid_int is not None and isinstance(uid_val, (int, float)) and int(uid_val) == out_uid_int) or (
                    str(uid_val) == str(out_uid_raw)
                ):
                    outputs.append(d)

    if outputs:
        return JSONResponse(content=outputs, status_code=200)

    # 2) programs: muxOutputUid + muxId
    programs: List[Dict[str, Any]] = []
    for d in dicts:
        if "muxOutputUid" in d and "muxId" in d:
            if str(d.get("muxId")) == str(mid):
                mou = d.get("muxOutputUid")
                if (out_uid_int is not None and isinstance(mou, (int, float)) and int(mou) == out_uid_int) or (
                    str(mou) == str(out_uid_raw)
                ):
                    programs.append(d)

    return JSONResponse(content=programs, status_code=200)

# ============================================================
# Registro dinámico GET desde Postman (omitimos endpoints lógicos)
# - Importante: registrar con prefijo /{pmf}/{pms31}
# ============================================================
def register_get_endpoints_from_postman():
    if not os.path.isfile(POSTMAN_COLLECTION_PATH):
        print(f"[WARN] No existe Postman JSON en {POSTMAN_COLLECTION_PATH}")
        return

    with open(POSTMAN_COLLECTION_PATH, "r", encoding="utf-8") as f:
        postman = json.load(f)

    flat: List[Dict[str, Any]] = []
    for top in postman.get("item", []) or []:
        _extract_requests_from_postman(top, flat)

    seen = set()

    SKIP = {
        # device lógicos
        "/pilot-manager-open-api/device/devicesByIds",
        "/pilot-manager-open-api/device/device",
        "/pilot-manager-open-api/device/devicesByType",
        # mux lógicos
        "/pilot-manager-open-api/mux_agent/getMuxIos",
        "/pilot-manager-open-api/mux_agent/getMuxServices",
        "/pilot-manager-open-api/mux_agent/getMuxOutputServices",
        # live lógicos
        "/pilot-manager-open-api/live_agent/getLiveServices",
        "/pilot-manager-open-api/live_agent/liveServiceConfigurationDetails",
    }

    for it in flat:
        req = it.get("request", {})
        method = str(req.get("method", "")).upper()
        if method != "GET":
            continue

        url_obj = req.get("url")
        local_path = _normalize_local_path(url_obj)
        if not local_path:
            continue

        if not local_path.startswith("/pilot-manager-open-api/"):
            continue

        if local_path in SKIP:
            continue

        if local_path in seen:
            continue
        seen.add(local_path)

        async def handler(request: Request, _p=local_path):
            return await serve_from_snapshot(_p, request)

        full_path = f"{FAKE_PILOT_PREFIX}{local_path}"
        app.add_api_route(full_path, handler, methods=["GET"], include_in_schema=True)
        _loaded_get_endpoints.append({"method": "GET", "path": full_path})


register_get_endpoints_from_postman()

# ============================================================
# Token fake: DEBE validar body contra env y si falla => 400 + ["Invalid username or password"]
# Endpoint con prefijo real: /{pmf}/{pms31}/users/token
# ============================================================
@app.post(f"{FAKE_PILOT_PREFIX}/users/token")
async def fake_token(body: dict = Body(...)):
    # Body obligatorio y con estas 2 keys
    username = body.get("username")
    password = body.get("password")

    if not username or not password:
        return JSONResponse(content=["Invalid username or password"], status_code=400)

    # Comparar contra variables resueltas arriba (PILOT_USER / PILOT_PASSWORD)
    if str(username) != str(PILOT_USER) or str(password) != str(PILOT_PASSWORD):
        return JSONResponse(content=["Invalid username or password"], status_code=400)

    return JSONResponse(content=_issue_token(), status_code=200)

# ============================================================
# Debug & health
# ============================================================
@app.get("/__fake/endpoints")
def list_fake_endpoints():
    return {"count": len(_loaded_get_endpoints), "endpoints": _loaded_get_endpoints}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "env_path_used": ENV_PATH,
        "pmf_deployment": PMF_DEPLOYMENT,
        "pilot_manager_deployment": PILOT_MANAGER_DEPLOYMENT,
        "fake_pilot_prefix": FAKE_PILOT_PREFIX,
        "sftp_host": SFTP_HOST,
        "sftp_port": SFTP_PORT,
        "sftp_user": SFTP_USER,
        "sftp_base_path": SFTP_BASE_PATH,
        "sftp_fake_base_path": SFTP_FAKE_BASE_PATH,
        "medux_base_url": MEDUX_BASE_URL,
        "medux_verify_ssl": MEDUX_VERIFY_SSL,
        "postman_collection_path": POSTMAN_COLLECTION_PATH,
        "loaded_get_endpoints": len(_loaded_get_endpoints),
        "fake_token_ttl_seconds": FAKE_TOKEN_TTL_SECONDS,
    }