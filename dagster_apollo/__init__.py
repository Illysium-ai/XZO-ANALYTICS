from dagster import Definitions
from .dbt_assets import build_defs


defs = build_defs()
