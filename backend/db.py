import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
REGIONS_TABLE = os.getenv("REGIONS_TABLE", "regions")
FEATURES_TABLE = os.getenv("FEATURES_TABLE", "osm_spb_features")

if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL. Put it in .env or environment variables.")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def rows_to_featurecollection(rows, geom_key="geom_geojson"):
    features = []
    for r in rows:
        r = dict(r)
        geom = r.pop(geom_key, None)
        if geom is None:
            continue
        # geom is already a JSON object (from ST_AsGeoJSON(...)::json)
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": r
        })
    return {"type": "FeatureCollection", "features": features}

def one_geom_to_feature(geom_obj, properties=None):
    return {
        "type": "Feature",
        "geometry": geom_obj,
        "properties": properties or {}
    }

def parse_geojson_geometry(geojson_obj):
    """
    Accept either:
      - a GeoJSON Feature (with "geometry")
      - a GeoJSON Geometry
    Return the Geometry dict.
    """
    if geojson_obj is None:
        raise ValueError("geojson is required")
    # 允许前端误传字符串（例如直接传 JSON 字符串）
    if isinstance(geojson_obj, str):
        geojson_obj = json.loads(geojson_obj)

    if not isinstance(geojson_obj, dict) or "type" not in geojson_obj:
        raise ValueError("Invalid GeoJSON: must be a dict with a 'type' field")

    if geojson_obj.get("type") == "Feature":
        return geojson_obj.get("geometry")
    return geojson_obj
