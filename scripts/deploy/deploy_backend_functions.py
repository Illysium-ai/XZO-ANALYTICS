#!/usr/bin/env python3
"""
Deploy backend functions/UDFs to Snowflake using Snow CLI.

- Scans tenants/dbt_williamgrant/backend_functions recursively
- Excludes any files matching '*_ddl*.sql' (DDL and DDL chain files)
- Executes remaining .sql files via `snow sql -c <profile> -f <file>`
- Supports optional `--database` and `--schema` which are prepended as USE statements
- Supports `--dry-run` to preview actions

Connection profiles:
- apollo     → development (dev)
- apollo_wgs → production (prod)

Usage:
    python scripts/deploy/deploy_backend_functions.py --profile apollo --database APOLLO_DEVELOPMENT --schema FORECAST
    python scripts/deploy/deploy_backend_functions.py --profile apollo_wgs --schema FORECAST
    python scripts/deploy/deploy_backend_functions.py --profile apollo --dry-run
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
BACKEND_FUNCTIONS_DIR = REPO_ROOT / "tenants" / "dbt_williamgrant" / "backend_functions"


def find_sql_files() -> List[Path]:
    """Find all non-DDL SQL files under backend_functions."""
    if not BACKEND_FUNCTIONS_DIR.exists():
        logger.error(f"Backend functions directory not found: {BACKEND_FUNCTIONS_DIR}")
        return []

    all_sql = list(BACKEND_FUNCTIONS_DIR.rglob("*.sql"))

    def is_excluded(p: Path) -> bool:
        # Exclude any file with '_ddl' in the stem, e.g. *_ddl.sql, *_ddl_chains.sql
        if "_ddl" in p.stem.lower():
            return True
        # Exclude anything under testing_suite
        parts_lower = [part.lower() for part in p.parts]
        if "testing_suite" in parts_lower:
            return True
        return False

    filtered = [p for p in all_sql if not is_excluded(p)]
    excluded_count = len(all_sql) - len(filtered)
    logger.info(f"Discovered {len(filtered)} deployable SQL files (excluded {excluded_count} non-deploy files)")
    return sorted(filtered)


def build_temp_sql(original_sql_path: Path, database: Optional[str], schema: Optional[str]) -> Tuple[Path, bool]:
    """If database/schema provided, create a temp SQL that prepends USE statements; else return the original path.

    Returns: (path_to_execute, is_temporary)
    """
    if not database and not schema:
        return original_sql_path, False

    use_lines: List[str] = []
    if database:
        use_lines.append(f"USE DATABASE {database};")
    if schema:
        use_lines.append(f"USE SCHEMA {schema};")

    temp_dir = tempfile.mkdtemp(prefix="deploy_sql_")
    temp_path = Path(temp_dir) / original_sql_path.name

    original_content = original_sql_path.read_text()
    temp_content = "\n".join(use_lines) + "\n\n" + original_content
    temp_path.write_text(temp_content)

    return temp_path, True


def run_snow_sql(profile: str, sql_file: Path) -> int:
    """Execute the given SQL file using Snow CLI and the specified connection profile."""
    # Important: use only -f for files that may contain USE statements (per project guidance)
    cmd = [
        "snow",
        "sql",
        "-c",
        profile,
        "-f",
        str(sql_file)
    ]
    logger.debug("Executing: %s", " ".join(cmd))
    completed = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if completed.stdout:
        sys.stdout.write(completed.stdout)
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr)
    return completed.returncode


def deploy(profile: str, database: Optional[str], schema: Optional[str], dry_run: bool) -> bool:
    logger.info(f"Starting deployment (profile={profile}, database={database}, schema={schema}, dry_run={dry_run})")
    if profile not in {"apollo", "apollo_wgs"}:
        logger.error("Invalid profile. Use 'apollo' (dev) or 'apollo_wgs' (prod).")
        return False

    sql_files = find_sql_files()
    if not sql_files:
        logger.warning("No deployable SQL files found.")
        return True

    successes = 0
    failures = 0
    failed_files: List[str] = []

    for sql_path in sql_files:
        rel = sql_path.relative_to(REPO_ROOT)
        logger.info(f"Deploying: {rel}")

        if dry_run:
            logger.info("  🔍 DRY RUN: Would execute via Snow CLI")
            continue

        temp_path: Optional[Path] = None
        is_temp = False
        try:
            temp_path, is_temp = build_temp_sql(sql_path, database, schema)
            rc = run_snow_sql(profile=profile, sql_file=temp_path)
            if rc == 0:
                logger.info("  ✅ Success")
                successes += 1
            else:
                logger.error(f"  ❌ Failed with exit code {rc}")
                failures += 1
                failed_files.append(str(rel))
        finally:
            if is_temp and temp_path is not None:
                try:
                    tmpdir = temp_path.parent
                    temp_path.unlink(missing_ok=True)
                    # remove the temp directory if empty
                    try:
                        tmpdir.rmdir()
                    except OSError:
                        pass
                except Exception:
                    pass

    logger.info("\n📊 Deployment Summary:")
    logger.info(f"   ✅ Successful: {successes}")
    logger.info(f"   ❌ Failed: {failures}")
    logger.info(f"   📁 Total processed: {len(sql_files)}")
    if failures > 0:
        logger.error("   🔎 Failed files:")
        for f in failed_files:
            logger.error(f"     - {f}")

    return failures == 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy backend functions/UDFs to Snowflake via Snow CLI")
    parser.add_argument("--profile", required=True, choices=["apollo", "apollo_wgs"], help="Snow CLI connection profile to use")
    parser.add_argument("--database", required=False, help="Snowflake database to USE before executing each file")
    parser.add_argument("--schema", required=False, help="Snowflake schema to USE before executing each file")
    parser.add_argument("--dry-run", action="store_true", help="List actions without executing")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ok = deploy(profile=args.profile, database=args.database, schema=args.schema, dry_run=args.dry_run)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
