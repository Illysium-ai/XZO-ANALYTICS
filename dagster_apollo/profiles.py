import os
import tempfile
from typing import Dict, Any

# Load .env for local dev without overriding pre-set environment
try:
	from dotenv import load_dotenv  # type: ignore
	load_dotenv(override=False)
except Exception:
	pass

REQUIRED_ENV = [
	"DBT_PROFILE_NAME",
	"SNOWFLAKE_ACCOUNT",
	"SNOWFLAKE_USER",
	"SNOWFLAKE_ROLE",
	"SNOWFLAKE_DATABASE",
	"SNOWFLAKE_WAREHOUSE",
]

OPTIONAL_ENV = [
	"SNOWFLAKE_PRIVATE_KEY_PEM",
	"SNOWFLAKE_PRIVATE_KEY_B64",
	"SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
	"DBT_TARGET",
	"DBT_THREADS",
	"DBT_SCHEMA",
	"SNOWFLAKE_SCHEMA",
]


def _env(key: str, default: str | None = None) -> str | None:
	val = os.getenv(key)
	return val if val is not None else default


def _validate_env() -> None:
	missing = [k for k in REQUIRED_ENV if _env(k) is None]
	if _env("SNOWFLAKE_PRIVATE_KEY_PEM") is None and _env("SNOWFLAKE_PRIVATE_KEY_B64") is None:
		missing.append("SNOWFLAKE_PRIVATE_KEY_PEM or SNOWFLAKE_PRIVATE_KEY_B64")
	if missing:
		raise RuntimeError(
			"Missing required environment variables for dbt profiles: " + ", ".join(missing)
		)


def _build_profiles_dict() -> Dict[str, Any]:
	profile_name = _env("DBT_PROFILE_NAME", "apollo-snowflake")
	target = _env("DBT_TARGET", "dev")
	threads = int(_env("DBT_THREADS", "24"))

	private_key = _env("SNOWFLAKE_PRIVATE_KEY_PEM")
	if private_key is None:
		b64 = _env("SNOWFLAKE_PRIVATE_KEY_B64")
		if b64:
			import base64
			private_key = base64.b64decode(b64).decode("utf-8")

	base_target: Dict[str, Any] = {
		"type": "snowflake",
		"account": _env("SNOWFLAKE_ACCOUNT"),
		"user": _env("SNOWFLAKE_USER"),
		"role": _env("SNOWFLAKE_ROLE"),
		"database": _env("SNOWFLAKE_DATABASE"),
		"warehouse": _env("SNOWFLAKE_WAREHOUSE"),
		"threads": threads,
		"client_session_keep_alive": False,
		"reuse_connections": True,
		"disable_ocsp_checks": False,
	}

	# Policy: do not include schema by default; allow explicit opt-in.
	# Fallback to SNOWFLAKE_SCHEMA for dbt-core validation when present.
	schema_override = _env("DBT_SCHEMA") or _env("SNOWFLAKE_SCHEMA")
	if schema_override:
		base_target["schema"] = schema_override

	if private_key:
		# Ensure PEM has correct newlines
		if "-----BEGIN" in private_key and "\n" not in private_key:
			private_key = private_key.replace("\\n", "\n")
		base_target["private_key"] = private_key
		passphrase = _env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
		if passphrase:
			base_target["private_key_passphrase"] = passphrase

	profiles: Dict[str, Any] = {
		profile_name: {
			"target": target,
			"outputs": {
				"dev": base_target,
				"prod": base_target,
			},
		}
	}
	return profiles


def _dump_yaml(data: Dict[str, Any]) -> str:
	import yaml
	return yaml.safe_dump(data, sort_keys=False)


def ensure_profiles() -> str:
	"""
	Generate profiles.yml into a temp directory and set DBT_PROFILES_DIR.
	Returns the directory path.
	"""
	_validate_env()
	profiles = _build_profiles_dict()
	yaml_text = _dump_yaml(profiles)
	tmp = tempfile.mkdtemp(prefix="dbt_profiles_")
	path = os.path.join(tmp, "profiles.yml")
	with open(path, "w", encoding="utf-8") as f:
		f.write(yaml_text)
	os.environ["DBT_PROFILES_DIR"] = tmp
	return tmp


# Removed auto-generation at import to avoid side effects; call ensure_profiles() explicitly in code location. 
