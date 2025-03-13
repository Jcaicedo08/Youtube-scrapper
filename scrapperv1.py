"""
YouTube Channel Scraper - Versión Optimizada

Este script extrae datos de un canal de YouTube utilizando las APIs de YouTube Data y Analytics.
Recopila estadísticas de visualizaciones anuales, videos publicados y métricas detalladas
para su uso en una aplicación de escritorio.

Requisitos previos:
1. Crear un proyecto en Google Cloud Console (https://console.cloud.google.com/)
2. Habilitar YouTube Data API v3 y YouTube Analytics API
3. Crear credenciales OAuth 2.0 y descargar el archivo JSON
4. Colocar el archivo de credenciales como "client_secrets.json" en el mismo directorio que este script
"""

import datetime
import json
import os
import time
from typing import Dict, List, Tuple, Optional, Any
import logging
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# Configuración de logging para seguimiento de ejecución y depuración
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("youtube_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("youtube_scraper")

# Archivo de configuración para parámetros del scraper
CONFIG_FILE = "config.json"

def load_config() -> Dict[str, Any]:
    """
    Carga la configuración desde un archivo JSON.
    Si el archivo no existe, crea uno con valores predeterminados.

    Returns:
        Dict: Configuración del scraper
    """
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        # Configuración por defecto si no existe el archivo
        default_config = {
            # IMPORTANTE: Coloca aquí el nombre del archivo JSON de credenciales
            # descargado desde Google Cloud Console
            "client_secrets_file": "client_secret_844657651833-kuq37s029uuna9jhtnkji47dcpsb0rc2.apps.googleusercontent.com.json",  # ← ARCHIVO DE CREDENCIALES DE GOOGLE
            "token_file": "token.json",
            "channel_id": "",  # Se solicitará al usuario si está vacío
            "years_to_analyze": 5,
            "output_file": "youtube_data.json",
            "scopes": [
                "https://www.googleapis.com/auth/youtube.readonly",
                "https://www.googleapis.com/auth/yt-analytics.readonly"
            ],
            "api_service_name": {
                "data": "youtube",
                "analytics": "youtubeAnalytics"
            },
            "api_version": {
                "data": "v3",
                "analytics": "v2"
            },
            "request_delay": 0.5  # Tiempo de espera entre solicitudes (segundos)
        }
        # Guardar configuración por defecto
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=4)
        logger.info(f"Archivo de configuración creado: {CONFIG_FILE}")
        return default_config

def get_authenticated_services(config: Dict[str, Any]) -> Tuple:
    """
    Realiza la autenticación y devuelve los servicios para Data API y Analytics API.
    Implementa persistencia de token para evitar autenticación repetida.

    Args:
        config: Configuración del scraper

    Returns:
        Tuple: (youtube_service, youtube_analytics_service)

    Raises:
        Exception: Si falla la autenticación o construcción de servicios
    """
    creds = None
    token_file = config["token_file"]

    # NOTA: Asegúrate de que el archivo client_secrets.json (descargado de Google Cloud Console)
    # esté en el mismo directorio que este script o actualiza la ruta en config.json

    # Verificar si ya existe un token guardado
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_info(
                json.loads(Path(token_file).read_text()),
                config["scopes"]
            )
        except Exception as e:
            logger.error(f"Error al cargar credenciales: {e}")

    # Si no hay credenciales válidas o están expiradas, solicitar nuevas
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.error(f"Error al refrescar token: {e}")
                creds = None

        if not creds:
            try:
                # IMPORTANTE: Aquí se utiliza el archivo de credenciales de Google
                # Asegúrate de que el archivo exista en la ruta especificada
                client_secrets_path = config["client_secrets_file"]
                if not os.path.exists(client_secrets_path):
                    logger.error(f"Archivo de credenciales no encontrado: {client_secrets_path}")
                    print(f"\n⚠️ ERROR: No se encontró el archivo de credenciales '{client_secrets_path}'")
                    print("Por favor, descarga el archivo JSON de credenciales desde Google Cloud Console")
                    print("y colócalo en el mismo directorio que este script con el nombre 'client_secrets.json'")
                    print("o actualiza la ruta en el archivo config.json\n")
                    raise FileNotFoundError(f"Archivo de credenciales no encontrado: {client_secrets_path}")

                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secrets_path,
                    config["scopes"]
                )
                # Esto abrirá una ventana del navegador para autenticación
                creds = flow.run_local_server(port=0)

                # Guardar credenciales para la próxima ejecución
                with open(token_file, 'w') as token:
                    token.write(creds.to_json())
                logger.info("Nuevas credenciales guardadas")
            except Exception as e:
                logger.error(f"Error en el proceso de autenticación: {e}")
                raise

    try:
        # Construir servicios de API con las credenciales
        youtube = build(
            config["api_service_name"]["data"],
            config["api_version"]["data"],
            credentials=creds
        )
        youtube_analytics = build(
            config["api_service_name"]["analytics"],
            config["api_version"]["analytics"],
            credentials=creds
        )
        return youtube, youtube_analytics
    except Exception as e:
        logger.error(f"Error al construir servicios de API: {e}")
        raise

def get_annual_views(youtube_analytics, channel_id: str, start_year: int, end_year: int,
                     delay: float = 0.5) -> Dict[int, int]:
    """
    Obtiene las visualizaciones totales por año desde start_year hasta end_year.

    Args:
        youtube_analytics: Servicio de YouTube Analytics
        channel_id: ID del canal
        start_year: Año inicial
        end_year: Año final
        delay: Tiempo de espera entre solicitudes para evitar límites de API

    Returns:
        Dict: Diccionario con años como claves y visualizaciones como valores
    """
    annual_views = {}
    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        try:
            # Consulta a la API de Analytics para obtener visualizaciones del año
            response = youtube_analytics.reports().query(
                ids=f"channel=={channel_id}",
                startDate=start_date,
                endDate=end_date,
                metrics="views"
            ).execute()
            # Se espera que el reporte tenga filas con el valor de views
            views = response.get("rows", [[0]])[0][0]
            annual_views[year] = int(views)
            logger.info(f"Obtenidas {views} visualizaciones para el año {year}")
            time.sleep(delay)  # Evitar límites de tasa de API
        except HttpError as e:
            logger.error(f"Error al obtener visualizaciones para {year}: {e}")
            annual_views[year] = 0
    return annual_views

def get_channel_uploads_playlist(youtube, channel_id: str) -> Optional[str]:
    """
    Obtiene el ID de la lista de reproducción de uploads del canal.
    Cada canal de YouTube tiene una playlist automática con todos sus videos.

    Args:
        youtube: Servicio de YouTube Data API
        channel_id: ID del canal

    Returns:
        str: ID de la lista de reproducción de uploads o None si hay error
    """
    try:
        response = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()

        if not response.get("items"):
            logger.error(f"No se encontró el canal con ID: {channel_id}")
            return None

        # Extraer el ID de la playlist de uploads del canal
        uploads_playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        logger.info(f"ID de playlist de uploads: {uploads_playlist_id}")
        return uploads_playlist_id
    except HttpError as e:
        logger.error(f"Error al obtener playlist de uploads: {e}")
        return None

def get_videos_by_year(youtube, uploads_playlist_id: str, delay: float = 0.5) -> Dict[str, List[str]]:
    """
    Extrae los videos del canal y los organiza por año (basándose en su fecha de publicación).
    Utiliza paginación para obtener todos los videos del canal.

    Args:
        youtube: Servicio de YouTube Data API
        uploads_playlist_id: ID de la lista de reproducción de uploads
        delay: Tiempo de espera entre solicitudes para evitar límites de API

    Returns:
        Dict: Diccionario con años como claves y listas de IDs de videos como valores
    """
    videos_by_year = {}
    nextPageToken = None
    total_videos = 0

    try:
        # Bucle para manejar la paginación de resultados
        while True:
            playlist_response = youtube.playlistItems().list(
                part="snippet",
                playlistId=uploads_playlist_id,
                maxResults=50,  # Máximo permitido por la API
                pageToken=nextPageToken
            ).execute()

            # Procesar cada video en la respuesta
            for item in playlist_response.get("items", []):
                video_id = item["snippet"]["resourceId"]["videoId"]
                published_at = item["snippet"]["publishedAt"]  # Formato ISO 8601
                year = published_at[:4]  # Extraer solo el año (primeros 4 caracteres)

                # Agregar video al diccionario organizado por año
                if year not in videos_by_year:
                    videos_by_year[year] = []
                videos_by_year[year].append(video_id)
                total_videos += 1

            # Verificar si hay más páginas de resultados
            nextPageToken = playlist_response.get("nextPageToken")
            if not nextPageToken:
                break  # No hay más páginas

            time.sleep(delay)  # Evitar límites de tasa de API

        logger.info(f"Total de videos extraídos: {total_videos} en {len(videos_by_year)} años")
        return videos_by_year
    except HttpError as e:
        logger.error(f"Error al obtener videos por año: {e}")
        return videos_by_year  # Devolver lo que se haya recopilado hasta el error

def get_video_stats(youtube, video_ids: List[str], delay: float = 0.5) -> Dict[str, Dict]:
    """
    Obtiene estadísticas detalladas de una lista de videos.
    Se procesa en grupos de 50 (límite de la API).

    Args:
        youtube: Servicio de YouTube Data API
        video_ids: Lista de IDs de videos
        delay: Tiempo de espera entre solicitudes para evitar límites de API

    Returns:
        Dict: Diccionario con IDs de videos como claves y estadísticas como valores
    """
    stats = {}
    # Procesar videos en lotes de 50 (límite de la API)
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            # Obtener detalles de los videos en un solo llamado a la API
            response = youtube.videos().list(
                part="statistics,snippet,contentDetails",
                id=",".join(batch)
            ).execute()

            # Procesar cada video en la respuesta
            for item in response.get("items", []):
                video_id = item["id"]
                title = item["snippet"]["title"]
                statistics = item.get("statistics", {})
                content_details = item.get("contentDetails", {})

                # Convertir duración ISO 8601 a segundos
                duration_iso = content_details.get("duration", "PT0S")
                duration_seconds = parse_duration(duration_iso)

                # Almacenar datos del video
                stats[video_id] = {
                    "title": title,
                    "views": int(statistics.get("viewCount", 0)),
                    "likes": int(statistics.get("likeCount", 0)),
                    "comments": int(statistics.get("commentCount", 0)),
                    "duration": duration_seconds,
                    "published_at": item["snippet"]["publishedAt"],
                    "thumbnail": item["snippet"]["thumbnails"].get("high", {}).get("url", "")
                }

            time.sleep(delay)  # Evitar límites de tasa de API
            logger.info(f"Procesados {len(batch)} videos (IDs {i}-{i+len(batch)-1})")
        except HttpError as e:
            logger.error(f"Error al obtener estadísticas para el lote {i}-{i+len(batch)-1}: {e}")

    return stats

def parse_duration(duration_iso: str) -> int:
    """
    Convierte una duración en formato ISO 8601 a segundos.
    Ejemplo: PT1H30M15S -> 5415 segundos (1h 30m 15s)

    Args:
        duration_iso: Duración en formato ISO 8601

    Returns:
        int: Duración en segundos
    """
    duration = 0
    # Eliminar 'PT' del inicio (formato ISO 8601 para duración)
    time_str = duration_iso[2:]

    # Extraer horas, minutos y segundos
    hours = 0
    minutes = 0
    seconds = 0

    # Buscar y extraer horas (H)
    h_pos = time_str.find('H')
    if h_pos != -1:
        hours = int(time_str[:h_pos])
        time_str = time_str[h_pos+1:]

    # Buscar y extraer minutos (M)
    m_pos = time_str.find('M')
    if m_pos != -1:
        minutes = int(time_str[:m_pos])
        time_str = time_str[m_pos+1:]

    # Buscar y extraer segundos (S)
    s_pos = time_str.find('S')
    if s_pos != -1:
        seconds = int(time_str[:s_pos])

    # Calcular duración total en segundos
    duration = hours * 3600 + minutes * 60 + seconds
    return duration

def get_top_videos(stats: Dict[str, Dict], metric: str, limit: int = 5) -> List[Tuple[str, Dict]]:
    """
    Devuelve los videos con los mayores valores para la métrica indicada.

    Args:
        stats: Diccionario con estadísticas de videos
        metric: Métrica a considerar (views, likes, comments)
        limit: Cantidad de videos a devolver

    Returns:
        List: Lista de tuplas (video_id, datos) ordenada por la métrica
    """
    # Ordenar videos por la métrica especificada (de mayor a menor)
    sorted_videos = sorted(
        stats.items(),
        key=lambda x: x[1].get(metric, 0),
        reverse=True
    )
    # Devolver los primeros 'limit' videos
    return sorted_videos[:limit]

def save_data_to_json(data: Dict, filename: str) -> None:
    """
    Guarda los datos en un archivo JSON para la aplicación de escritorio.

    Args:
        data: Datos a guardar
        filename: Nombre del archivo
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Datos guardados en {filename}")
    except Exception as e:
        logger.error(f"Error al guardar datos en {filename}: {e}")

def main():
    """
    Función principal del scraper.
    Coordina todo el proceso de extracción y almacenamiento de datos.
    """
    try:
        # Cargar configuración
        config = load_config()

        # Verificar si se ha configurado un channel_id
        if not config["channel_id"]:
            channel_id = input("Ingresa el ID del canal de YouTube: ")
            config["channel_id"] = channel_id
            # Actualizar configuración
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
        else:
            channel_id = config["channel_id"]

        # Autenticación y construcción de servicios
        youtube, youtube_analytics = get_authenticated_services(config)

        # Rango de años que deseas analizar
        current_year = datetime.datetime.now().year
        start_year = current_year - config["years_to_analyze"]
        end_year = current_year

        # Estructura para almacenar todos los datos
        channel_data = {
            "channel_id": channel_id,
            "extraction_date": datetime.datetime.now().isoformat(),
            "annual_views": {},
            "videos_by_year": {},
            "video_stats": {},
            "top_videos": {}
        }

        # 1. Visualizaciones totales por año usando Analytics API
        print("Obteniendo visualizaciones anuales...")
        annual_views = get_annual_views(
            youtube_analytics,
            channel_id,
            start_year,
            end_year,
            config["request_delay"]
        )
        channel_data["annual_views"] = annual_views

        # 2. Extraer videos publicados y organizarlos por año
        print("Obteniendo lista de videos del canal...")
        uploads_playlist_id = get_channel_uploads_playlist(youtube, channel_id)
        if not uploads_playlist_id:
            logger.error("No se pudo obtener la playlist de uploads. Finalizando.")
            print("Error: No se pudo obtener la lista de videos del canal.")
            return

        videos_by_year = get_videos_by_year(
            youtube,
            uploads_playlist_id,
            config["request_delay"]
        )
        channel_data["videos_by_year"] = videos_by_year

        # 3. Obtener estadísticas para cada video (para todos los años)
        print("Obteniendo estadísticas detalladas de videos...")
        all_video_stats = {}
        for year, video_ids in videos_by_year.items():
            if video_ids:
                logger.info(f"Obteniendo estadísticas para {len(video_ids)} videos del año {year}")
                print(f"  Procesando {len(video_ids)} videos del año {year}...")
                year_stats = get_video_stats(
                    youtube,
                    video_ids,
                    config["request_delay"]
                )
                all_video_stats.update(year_stats)

        channel_data["video_stats"] = all_video_stats

        # 4. Encontrar los mejores videos por diferentes métricas
        print("Identificando videos destacados...")
        top_videos = {
            "views": get_top_videos(all_video_stats, "views"),
            "likes": get_top_videos(all_video_stats, "likes"),
            "comments": get_top_videos(all_video_stats, "comments")
        }

        # Convertir las tuplas a formato serializable para JSON
        for metric, videos in top_videos.items():
            top_videos[metric] = [
                {"video_id": vid, "data": data} for vid, data in videos
            ]

        channel_data["top_videos"] = top_videos

        # 5. Guardar todos los datos en un archivo JSON para la app de escritorio
        print(f"Guardando datos en {config['output_file']}...")
        save_data_to_json(channel_data, config["output_file"])

        # Mostrar resumen
        print("\n=== Resumen de datos extraídos ===")
        print(f"Canal ID: {channel_id}")
        print(f"Años analizados: {start_year}-{end_year}")
        print(f"Total de videos procesados: {len(all_video_stats)}")
        for year, count in sorted([(y, len(v)) for y, v in videos_by_year.items()]):
            print(f"  {year}: {count} videos")
        print(f"Datos guardados en: {config['output_file']}")
        print("\nPuedes utilizar este archivo JSON en tu aplicación de escritorio.")

    except Exception as e:
        logger.error(f"Error en la ejecución principal: {e}", exc_info=True)
        print(f"Error: {e}")
        print("Consulta el archivo de log para más detalles: youtube_scraper.log")

if __name__ == "__main__":
    main()