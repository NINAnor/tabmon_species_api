"""Site metadata from site_info.csv, shared by both apps."""

import streamlit as st

from config import SITE_INFO_S3_PATH
from core.storage import get_duckdb_connection


@st.cache_data(ttl=3600)
def get_available_countries():
    """Countries listed in site_info.csv, sorted."""
    rows = (
        get_duckdb_connection()
        .execute(
            f"SELECT DISTINCT Country FROM '{SITE_INFO_S3_PATH}' "
            "WHERE Country IS NOT NULL ORDER BY Country"
        )
        .fetchall()
    )
    return [row[0] for row in rows]


@st.cache_data(ttl=3600, show_spinner="Loading sites...")
def get_sites_for_country(country):
    """Map site name -> device ID for every site in a country."""
    rows = (
        get_duckdb_connection()
        .execute(
            f"SELECT DISTINCT DeviceID, Site FROM '{SITE_INFO_S3_PATH}' "
            "WHERE Country = ? AND Site IS NOT NULL ORDER BY DeviceID",
            [country],
        )
        .fetchall()
    )
    return {site: device_id for device_id, site in rows}


@st.cache_data(ttl=3600)
def get_device_site_map():
    """Map device ID and deployment ID -> {"site", "cluster", "country"}."""
    try:
        rows = (
            get_duckdb_connection()
            .execute(
                "SELECT DeviceID, DeploymentID, Site, Cluster, Country "
                f"FROM '{SITE_INFO_S3_PATH}'"
            )
            .fetchall()
        )
    except Exception:
        return {}

    result = {}
    for device_id, deployment_id, site, cluster, country in rows:
        info = {"site": site, "cluster": cluster, "country": country}
        result[device_id] = info
        if deployment_id:
            result[deployment_id] = info
    return result


def get_site_info_for_deployment(deployment_id):
    """Look up site/cluster/country metadata for a clip's deployment_id.

    Tries a direct deployment_id lookup first, then falls back to the
    device_id (last segment after '_'). Returns {} when nothing matches.
    """
    device_site_map = get_device_site_map()
    site_info = device_site_map.get(deployment_id)
    if site_info:
        return site_info
    device_id = deployment_id.rsplit("_", 1)[-1]
    return device_site_map.get(device_id, {})
