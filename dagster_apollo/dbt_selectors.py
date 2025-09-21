from dagster import Definitions, job, op
from dagster_dbt import DbtCliResource


@op
def run_dbt_build_excluding_seeds(dbt: DbtCliResource):
	# Equivalent to: dbt build --exclude seeds/
	yield from dbt.cli(["build", "--exclude", "seeds/"]).stream()


@job
def dbt_build_exclude_seeds_job():
	run_dbt_build_excluding_seeds()  # resource bound at Definitions


def get_defs(dbt: DbtCliResource) -> Definitions:
	return Definitions(jobs=[dbt_build_exclude_seeds_job], resources={"dbt": dbt})
