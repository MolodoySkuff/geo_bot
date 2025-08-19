# bot/providers/external.py
from typing import Tuple
from shapely.geometry import base as shapely_geom
from .nspd import get_geometry_and_meta_by_cadnum_nspd

def get_geometry_and_meta_by_cadnum(cadnum: str) -> Tuple[shapely_geom.BaseGeometry, dict]:
    """
    Единственный провайдер: NSPD.
    meta: { source:'nspd', source_label:'NSPD (точный контур)', attrs:{...}, raw:{...} }
    """
    return get_geometry_and_meta_by_cadnum_nspd(cadnum)

def get_geometry_by_cadnum(cadnum: str):
    geom, _ = get_geometry_and_meta_by_cadnum(cadnum)
    return geom