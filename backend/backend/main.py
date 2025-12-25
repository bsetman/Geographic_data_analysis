import json
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from db import (
    get_conn, RealDictCursor,
    REGIONS_TABLE, FEATURES_TABLE,
    rows_to_featurecollection, one_geom_to_feature, parse_geojson_geometry
)

app = FastAPI(title="PostGIS GeoJSON API (Leaflet)")

# 开发阶段允许跨域，前端直接打开本地 html 也能调用
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- 0) 健康检查 ----------
@app.get("/health")
def health():
    return {"ok": True}


# ---------- 1) 点在多边形内（Point in Polygon） ----------
# 默认对 FEATURES_TABLE（osm_spb_features）做点落面查询；如需对 regions 表查询，传 source="regions"
@app.post("/q/pip")
def point_in_polygon(payload: Dict[str, Any] = Body(...)):
    lon = payload.get("lon")
    lat = payload.get("lat")
    source = (payload.get("source") or "features").lower()  # features | regions
    limit = int(payload.get("limit", 200))
    if lon is None or lat is None:
        raise HTTPException(400, "lon/lat required")

    table = REGIONS_TABLE if source == "regions" else FEATURES_TABLE
    # 只对面要素进行点落面，避免对点/线做 covers（会返回空）
    geom_filter = "GeometryType(geom) IN ('POLYGON','MULTIPOLYGON')"
    sql = f"""
    SELECT id,
           COALESCE(name, '') AS name,
           ST_AsGeoJSON(geom)::json AS geom_geojson
    FROM {table}
    WHERE {geom_filter}
      AND ST_Covers(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
    LIMIT %s;
    """

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (lon, lat, limit))
        rows = cur.fetchall()

    return rows_to_featurecollection(rows)


# ---------- 2) 多边形相交查询（Polygon intersects） ----------
# 默认与 FEATURES_TABLE 做相交查询；如需与 regions 表查询，传 source="regions"
@app.post("/q/intersects")
def polygon_intersects(payload: Dict[str, Any] = Body(...)):
    geojson = payload.get("geojson")
    geom = parse_geojson_geometry(geojson)
    source = (payload.get("source") or "features").lower()
    limit = int(payload.get("limit", 500))

    table = REGIONS_TABLE if source == "regions" else FEATURES_TABLE
    sql = f"""
    WITH q AS (
      SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS g
    )
    SELECT id,
           COALESCE(name, '') AS name,
           ST_AsGeoJSON(geom)::json AS geom_geojson
    FROM {table}, q
    WHERE ST_Intersects(geom, q.g)
    LIMIT %s;
    """

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (json.dumps(geom), limit))
        rows = cur.fetchall()

    return rows_to_featurecollection(rows)


# ---------- 3) 距离范围查询（DWithin） ----------
@app.post("/q/within-distance")
def within_distance(payload: Dict[str, Any] = Body(...)):
    lon = payload.get("lon")
    lat = payload.get("lat")
    radius_m = payload.get("radius_m")
    limit = int(payload.get("limit", 500))

    if lon is None or lat is None or radius_m is None:
        raise HTTPException(400, "lon/lat/radius_m required")

    sql = f"""
    WITH p AS (
      SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326) AS pt
    )
    SELECT id, osmid, element_type, name,
           tags,
           ST_Distance(geom::geography, p.pt::geography) AS dist_m,
           ST_AsGeoJSON(geom)::json AS geom_geojson
    FROM {FEATURES_TABLE}, p
    WHERE ST_DWithin(geom::geography, p.pt::geography, %s)
    ORDER BY dist_m
    LIMIT %s;
    """

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (lon, lat, radius_m, limit))
        rows = cur.fetchall()

    return rows_to_featurecollection(rows)


# ---------- 4) 缓冲区分析（Buffer + 选中要素） ----------
@app.post("/q/buffer")
def buffer_analysis(payload: Dict[str, Any] = Body(...)):
    lon = payload.get("lon")
    lat = payload.get("lat")
    buffer_m = payload.get("buffer_m")
    limit = int(payload.get("limit", 1000))

    if lon is None or lat is None or buffer_m is None:
        raise HTTPException(400, "lon/lat/buffer_m required")

    # 返回：buffer polygon（Feature） + 命中要素（FeatureCollection）
    buffer_sql = """
    SELECT ST_AsGeoJSON(
             ST_Buffer(ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography, %s)::geometry
           )::json AS geom_geojson;
    """

    hits_sql = f"""
    WITH buf AS (
      SELECT ST_Buffer(ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography, %s)::geometry AS g
    )
    SELECT id, osmid, element_type, name, tags,
           ST_AsGeoJSON(geom)::json AS geom_geojson
    FROM {FEATURES_TABLE}, buf
    WHERE ST_Intersects(geom, buf.g)
    LIMIT %s;
    """

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(buffer_sql, (lon, lat, buffer_m))
        buf_geom = cur.fetchone()["geom_geojson"]

        cur.execute(hits_sql, (lon, lat, buffer_m, limit))
        rows = cur.fetchall()

    return {
        "buffer": one_geom_to_feature(buf_geom, {"buffer_m": buffer_m}),
        "hits": rows_to_featurecollection(rows)
    }


# ---------- 5) 计算多边形面积（Area） ----------
@app.post("/q/area")
def polygon_area(payload: Dict[str, Any] = Body(...)):
    geojson = payload.get("geojson")
    geom = parse_geojson_geometry(geojson)

    sql = """
    WITH q AS (
      SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS g
    )
    SELECT
      ST_Area(q.g::geography) AS area_m2,
      ST_Area(q.g::geography)/1e6 AS area_km2
    FROM q;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (json.dumps(geom),))
        area_m2, area_km2 = cur.fetchone()

    return {"area_m2": float(area_m2), "area_km2": float(area_km2)}


# ---------- 6) 计算多边形周长（Perimeter） ----------
@app.post("/q/perimeter")
def polygon_perimeter(payload: Dict[str, Any] = Body(...)):
    geojson = payload.get("geojson")
    geom = parse_geojson_geometry(geojson)

    sql = """
    WITH q AS (
      SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS g
    )
    SELECT
      ST_Perimeter(q.g::geography) AS perim_m,
      ST_Perimeter(q.g::geography)/1000 AS perim_km
    FROM q;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (json.dumps(geom),))
        perim_m, perim_km = cur.fetchone()

    return {"perimeter_m": float(perim_m), "perimeter_km": float(perim_km)}


# ---------- 7) 最近邻（KNN） ----------
@app.post("/q/knn")
def knn(payload: Dict[str, Any] = Body(...)):
    lon = payload.get("lon")
    lat = payload.get("lat")
    k = int(payload.get("k", 10))

    if lon is None or lat is None:
        raise HTTPException(400, "lon/lat required")

    sql = f"""
    WITH p AS (
      SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326) AS pt
    )
    SELECT id, osmid, element_type, name, tags,
           ST_Distance(geom::geography, p.pt::geography) AS dist_m,
           ST_AsGeoJSON(geom)::json AS geom_geojson
    FROM {FEATURES_TABLE}, p
    ORDER BY geom <-> p.pt
    LIMIT %s;
    """

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (lon, lat, k))
        rows = cur.fetchall()

    return rows_to_featurecollection(rows)


# ---------- 8) 聚合操作：多边形合并（Union） ----------
@app.post("/q/union")
def union_polygons(payload: Dict[str, Any] = Body(...)):
    # 方式A：传 regions 的 id 列表
    region_ids: Optional[List[int]] = payload.get("region_ids")
    # 方式B：传多边形数组（GeoJSON geometry/feature）
    geoms = payload.get("geoms")

    if region_ids:
        sql = f"""
        SELECT ST_AsGeoJSON(ST_Union(geom))::json AS geom_geojson
        FROM {REGIONS_TABLE}
        WHERE id = ANY(%s);
        """
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (region_ids,))
            row = cur.fetchone()
        if not row or row[0] is None:
            raise HTTPException(404, "No geometries found for given region_ids")
        return one_geom_to_feature(row[0], {"source": "regions", "region_ids": region_ids})

    if geoms:
        geom_list = [parse_geojson_geometry(g) for g in geoms]
        sql = """
        WITH arr AS (
          SELECT ARRAY(
            SELECT ST_SetSRID(ST_GeomFromGeoJSON(x), 4326)
            FROM unnest(%s::text[]) AS x
          ) AS gs
        )
        SELECT ST_AsGeoJSON(ST_Union(gs))::json AS geom_geojson
        FROM arr;
        """
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, ([json.dumps(g) for g in geom_list],))
            row = cur.fetchone()
        return one_geom_to_feature(row[0], {"source": "geoms", "count": len(geom_list)})

    raise HTTPException(400, "Provide region_ids or geoms")


# ---------- 9) 求交：多边形交集（Intersection） ----------
@app.post("/q/intersection")
def intersection(payload: Dict[str, Any] = Body(...)):
    a = payload.get("a")
    b = payload.get("b")
    if a is None or b is None:
        raise HTTPException(400, "Provide a and b (GeoJSON geometry or Feature)")

    ga = parse_geojson_geometry(a)
    gb = parse_geojson_geometry(b)

    sql = """
    WITH q AS (
      SELECT
        ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS a,
        ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS b
    )
    SELECT
      ST_AsGeoJSON(ST_Intersection(a, b))::json AS geom_geojson,
      ST_Area(ST_Intersection(a, b)::geography) AS area_m2
    FROM q;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (json.dumps(ga), json.dumps(gb)))
        geom_geojson, area_m2 = cur.fetchone()

    if geom_geojson is None:
        return {"type": "Feature", "geometry": None, "properties": {"area_m2": 0.0}}

    return one_geom_to_feature(geom_geojson, {"area_m2": float(area_m2)})


# ---------- 10) 坐标转换（Transform） ----------
@app.post("/q/transform")
def transform(payload: Dict[str, Any] = Body(...)):
    geojson = payload.get("geojson")
    to_epsg = int(payload.get("to_epsg", 3857))
    geom = parse_geojson_geometry(geojson)

    # 说明：Leaflet/GeoJSON 客户端默认按 EPSG:4326（经纬度）解释坐标。
    # 为了“既能可视化、又能拿到目标投影坐标”，这里返回：
    #   - geometry: 仍为 4326（原始）用于前端直接显示
    #   - properties.geom_transformed: 目标 EPSG 的几何（真正转换结果）
    sql = """
    WITH q AS (
      SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS g
    )
    SELECT
      ST_AsGeoJSON(q.g)::json AS geom_wgs84,
      ST_AsGeoJSON(ST_Transform(q.g, %s))::json AS geom_transformed
    FROM q;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (json.dumps(geom), to_epsg))
        geom_wgs84, geom_transformed = cur.fetchone()

    return one_geom_to_feature(
        geom_wgs84,
        {"to_epsg": to_epsg, "geom_transformed": geom_transformed}
    )
