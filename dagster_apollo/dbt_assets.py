from pathlib import Path
import os
from typing import Any, Dict, List, Optional
from dagster import AssetExecutionContext, Definitions, job, op, Config, ScheduleDefinition, define_asset_job
from dagster_dbt import (
	DbtCliResource,
	DbtProject,
	dbt_assets,
	build_schedule_from_dbt_selection,
	build_dbt_asset_selection,
)

from .profiles import ensure_profiles

DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "tenants" / "dbt_williamgrant"


class DbtBuildConfig(Config):
	select: Optional[List[str]] = None
	exclude: Optional[List[str]] = None
	full_refresh: bool = False
	vars: Optional[Dict[str, Any]] = None
	state: Optional[str] = None
	defer_: bool = False  # "defer" is reserved in Python
	target: Optional[str] = None


def _build_dbt_build_args(cfg: DbtBuildConfig) -> List[str]:
	args: List[str] = ["build"]
	if cfg.select:
		for s in cfg.select:
			args.extend(["--select", s])
	if cfg.exclude:
		for x in cfg.exclude:
			args.extend(["--exclude", x])
	if cfg.full_refresh:
		args.append("--full-refresh")
	if cfg.vars:
		# dbt expects a JSON/YAML string
		import json
		args.extend(["--vars", json.dumps(cfg.vars)])
	if cfg.state:
		args.extend(["--state", cfg.state])
	if cfg.defer_:
		args.append("--defer")
	if cfg.target:
		args.extend(["--target", cfg.target])
	return args


@op
def run_dbt_build(config: DbtBuildConfig, dbt: DbtCliResource):
	args = _build_dbt_build_args(config)
	# Use wait() to avoid asset-event mapping when no manifest is provided
	dbt.cli(args).wait()


@job
def dbt_build_job():
	run_dbt_build()  # resource bound via Definitions


def build_defs() -> Definitions:
	# Ensure DBT_PROFILES_DIR is set and profiles.yml exists
	ensure_profiles()

	my_project = DbtProject(project_dir=DBT_PROJECT_DIR)

	profiles_dir = os.environ.get("DBT_PROFILES_DIR")
	dbt_resource = DbtCliResource(
		project_dir=my_project,
		profiles_dir=profiles_dir,
	)

	# Ensure manifest exists in dev: run deps/parse only if missing
	if not my_project.manifest_path.exists():
		for cmd in ("deps", "parse"):
			for evt in dbt_resource.cli([cmd]).stream():
				pass

	@dbt_assets(manifest=my_project.manifest_path)
	def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
		yield from dbt.cli(["build"], context=context).stream()

	# Daily schedule (UTC 02:00) for full asset graph build via manifest
	daily_all_models = build_schedule_from_dbt_selection(
		[dbt_models],
		"materialize_dbt_models_prod",
		"0 2 * * *",
		dbt_select="fqn:*",
	)

	# Asset-based job and schedule for "dbt build --exclude seeds/"
	sel_excluding_seeds = build_dbt_asset_selection(
		manifest=my_project.manifest_path,
		dbt_select="fqn:* -resource_type:seed",
	)
	dbt_build_excluding_seeds_asset_job = define_asset_job(
		name="dbt_build_excluding_seeds_asset_job",
		selection=sel_excluding_seeds,
	)
	daily_build_excluding_seeds = ScheduleDefinition(
		name="daily_dbt_build_excluding_seeds",
		cron_schedule="0 3 * * *",
		job=dbt_build_excluding_seeds_asset_job,
	)

	return Definitions(
		assets=[dbt_models],
		jobs=[dbt_build_job, dbt_build_excluding_seeds_asset_job],
		schedules=[daily_all_models, daily_build_excluding_seeds],
		resources={"dbt": dbt_resource},
	)
