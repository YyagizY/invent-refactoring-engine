"""
Legacy vs refactored PreETL comparison utilities.

This module is meant for local / ad-hoc validation that a structurally-refactored
PreETL step produces identical output to its legacy counterpart for multiple
Azure run ids (and derived run dates).

Before running this script, activate the customer virtualenv:

    source <customer-name>-venv/bin/activate

e.g. for aloyoga:  source aloyoga-venv/bin/activate
"""

from __future__ import annotations

import importlib
import json
import os
import re
import socket
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union

# Set Spark/Java env before invent_local_env starts the JVM
if not os.environ.get("JAVA_HOME"):
    os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home"
if not os.environ.get("SPARK_LOCAL_IP"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from invent_local_env import spark, display, F

# Script may live at repo/.invent-refactoring-engine/scripts/refin_comparison.py or repo/refin_comparison.py
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_SCRIPT_DIR)
if os.path.basename(_SCRIPT_DIR) == "scripts" and (
    "invent-refactoring-engine" in _parent or ".invent-refactoring-engine" in _parent
):
    # Two levels up: scripts -> .invent-refactoring-engine -> repo
    _REPO_ROOT = os.path.dirname(_parent)
else:
    # Script at repo root
    _REPO_ROOT = _SCRIPT_DIR
_DEFAULT_MAIN_YAML = os.path.join(_REPO_ROOT, "dags", "config", "main.yaml")

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

RUN_DATE_RE = re.compile(r"\b(?P<run_date>\d{4}-\d{2}-\d{2})\b")


def _get_pre_etl_class_from_module(module: Any) -> type:
    """
    Find the PreETL subclass defined in the given module.
    Prefers the class whose __module__ is this module (defined here, not imported).
    """
    try:
        from rocks.opal.pre_etl import PreETL
    except ImportError:
        raise ValueError(
            "Cannot discover PreETL class: rocks.opal.pre_etl.PreETL not available. "
            "Ensure rocks-opal (or equivalent) is installed."
        ) from None
    module_name = getattr(module, "__name__", "")
    defined_here: Optional[type] = None
    for name in dir(module):
        if name.startswith("_"):
            continue
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, PreETL)
            and obj is not PreETL
        ):
            if getattr(obj, "__module__", "") == module_name:
                return obj
            if defined_here is None:
                defined_here = obj
    if defined_here is not None:
        return defined_here
    raise ValueError(
        f"No PreETL subclass found in module {module_name!r}"
    )


def load_legacy_and_refactored_classes(script_name: str) -> Tuple[type, type]:
    """
    Load legacy and refactored step classes for a given script name.

    - Refactored: rocks_extension.opal.pre_etl.<script_name>
    - Legacy: rocks_extension.opal.pre_etl.legacy.<script_name>_legacy

    Discovers the PreETL subclass in each module dynamically.
    """
    refactored_module_name = f"rocks_extension.opal.pre_etl.{script_name}"
    legacy_module_name = f"rocks_extension.opal.pre_etl.legacy.{script_name}_legacy"
    refactored_module = importlib.import_module(refactored_module_name)
    legacy_module = importlib.import_module(legacy_module_name)
    refactored_cls = _get_pre_etl_class_from_module(refactored_module)
    legacy_cls = _get_pre_etl_class_from_module(legacy_module)
    return legacy_cls, refactored_cls


def _set_env_default(key: str, value: str) -> None:
    if value is None:
        return
    if key not in os.environ or os.environ[key] == "":
        os.environ[key] = value


def apply_local_env_defaults(
    *,
    config_prefix: Optional[str] = None,
    datastore_bucket_name: Optional[str] = None,
    pokedex_cloud_provider: Optional[str] = None,
    pokedex_environment: Optional[str] = None,
    shu_secret_name: Optional[str] = None,
    java_home: Optional[str] = None,
    spark_local_ip: Optional[str] = None,
) -> None:
    """
    Apply the essential environment variables from the first cell of
    `eggs/first_try.ipynb`, but only if they are not already set.

    Intentionally does NOT depend on python-dotenv.
    """

    # Spark/Java local ergonomics
    _set_env_default(
        "JAVA_HOME",
        java_home or "/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home",
    )
    _set_env_default("SPARK_LOCAL_IP", spark_local_ip or "127.0.0.1")

    # Rocks/opals config + storage
    if config_prefix:
        _set_env_default("CONFIG_PREFIX", config_prefix)
    if datastore_bucket_name:
        _set_env_default("DATASTORE_BUCKET_NAME", datastore_bucket_name)

    # Pokedex / secrets
    if pokedex_cloud_provider:
        _set_env_default("POKEDEX_CLOUD_PROVIDER", pokedex_cloud_provider)
    if pokedex_environment:
        _set_env_default("POKEDEX_ENVIRONMENT", pokedex_environment)
    if shu_secret_name:
        _set_env_default("SHU_SECRET_NAME", shu_secret_name)


def _load_yaml(path: str) -> Dict[str, Any]:
    """
    Load YAML with PyYAML if available; otherwise do a minimal parse sufficient
    for `dags/config/main.yaml` global_settings extraction.
    """

    try:
        import yaml  # type: ignore

        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ModuleNotFoundError:
        # Minimal fallback parser: we only need a couple of keys under global_settings.
        # This is not a general YAML parser, but works for simple `key: value` lines.
        global_settings: Dict[str, str] = {}
        in_global = False

        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if not line or line.lstrip().startswith("#"):
                    continue

                if re.match(r"^global_settings\s*:\s*$", line):
                    in_global = True
                    continue

                if in_global and re.match(r"^[a-zA-Z_].*:\s*$", line):
                    # Next top-level section
                    in_global = False

                if not in_global:
                    continue

                m = re.match(r"^\s{2}([a-zA-Z0-9_]+)\s*:\s*(.+?)\s*$", line)
                if not m:
                    continue
                key = m.group(1)
                value = m.group(2).strip().strip("'\"")
                global_settings[key] = value

        return {"global_settings": global_settings}


@dataclass(frozen=True)
class AzureRunIdSource:
    account_name: str
    container: str
    prefix: str = "feed/"


def get_input_bucket_from_main_yaml(
    main_yaml_path: Optional[str] = None,
) -> str:
    """
    Get input_bucket from main.yaml global_settings.input_bucket.
    Resolves ${oc.env:INPUT_BUCKET_NAME,"invent-<customer_name>-input"} style to env or default.
    """
    path = main_yaml_path or _DEFAULT_MAIN_YAML
    config = _load_yaml(path)
    gs = (config or {}).get("global_settings", {}) or {}
    customer_name = gs.get("customer_name")
    default_bucket = f"invent-{customer_name}-input" if customer_name else "invent-aloyoga-input"
    raw = gs.get("input_bucket", "")
    if not raw:
        return os.getenv("INPUT_BUCKET_NAME", default_bucket)
    # Handle ${oc.env:INPUT_BUCKET_NAME,"invent-<customer_name>-input"}
    if "INPUT_BUCKET_NAME" in raw:
        return os.getenv("INPUT_BUCKET_NAME", default_bucket)
    return raw.strip().strip("'\"")


def get_customer_name_from_main_yaml(main_yaml_path: str) -> Optional[str]:
    """Get customer_name from main.yaml global_settings. Returns None if missing or file unreadable."""
    try:
        config = _load_yaml(main_yaml_path)
        gs = (config or {}).get("global_settings", {}) or {}
        return gs.get("customer_name") or None
    except (OSError, ValueError):
        return None


def get_shared_azure_account_name_from_main_yaml(
    main_yaml_path: Optional[str] = None,
) -> str:
    """
    Get shared_azure_account_name from main.yaml global_settings.
    Input bucket (history/) lives in this account, not azure_account_name.
    """
    path = main_yaml_path or _DEFAULT_MAIN_YAML
    config = _load_yaml(path)
    gs = (config or {}).get("global_settings", {}) or {}
    raw = gs.get("shared_azure_account_name", "")
    if not raw:
        raise ValueError(
            f"Missing `global_settings.shared_azure_account_name` in {path!r}"
        )
    return raw.strip().strip("'\"")


def list_history_folders_for_run_date(
    account_name: str,
    container: str,
    run_date: str,
    *,
    auth_mode: str = "login",
) -> List[str]:
    """
    List folder names under container prefix 'history/' that start with run_date (YYYY-MM-DD).
    Example folder: 2026-02-04T092741.008Z -> path /mnt/<container>/history/2026-02-04T092741.008Z/
    Uses Azure CLI with prefix history/<run_date> to list only that day's folders.
    Caller must use shared_azure_account_name for the input bucket (not azure_account_name).
    """
    ensure_az_login()
    # List only blobs under history/2026-02-04* (e.g. history/2026-02-04T092741.008Z/store.csv)
    prefix = f"history/{run_date}"
    blobs = _run_az_json(
        [
            "az",
            "storage",
            "blob",
            "list",
            "--auth-mode",
            auth_mode,
            "--account-name",
            account_name,
            "--container-name",
            container,
            "--prefix",
            prefix,
            "--output",
            "json",
        ]
    )
    if blobs is None:
        blobs = []
    if not isinstance(blobs, list):
        raise TypeError(f"Unexpected Azure CLI output type: {type(blobs)}")

    base_prefix = "history/"
    folders: Set[str] = set()
    for blob in blobs:
        name = (blob or {}).get("name")
        if not name or not isinstance(name, str) or not name.startswith(base_prefix):
            continue
        relative = name[len(base_prefix) :].strip("/")
        if not relative:
            continue
        # First path segment is the folder name (e.g. 2026-02-04T092741.008Z)
        folder = relative.split("/")[0]
        if folder.startswith(run_date):
            folders.add(folder)
    out = sorted(folders)
    return out


def derive_azure_source_from_main_yaml(
    main_yaml_path: Optional[str] = None,
    *,
    prefix: str = "feed/",
) -> AzureRunIdSource:
    """
    Derive Azure storage account + container + prefix from `dags/config/main.yaml`.

    Per your rules:
    - account: `global_settings.azure_account_name`
    - customer: `global_settings.customer_name`
    - container: `invent-<customer_name>-datastore`
    - prefix: `/feed` (represented here as `feed/`)
    """
    path = main_yaml_path or _DEFAULT_MAIN_YAML
    config = _load_yaml(path)
    gs = (config or {}).get("global_settings", {}) or {}
    customer_name = gs.get("customer_name")
    account_name = gs.get("azure_account_name")

    if not customer_name:
        raise ValueError(f"Missing `global_settings.customer_name` in {path!r}")
    if not account_name:
        raise ValueError(f"Missing `global_settings.azure_account_name` in {path!r}")

    container = f"invent-{customer_name}-datastore"
    normalized_prefix = prefix.lstrip("/")
    if normalized_prefix and not normalized_prefix.endswith("/"):
        normalized_prefix += "/"

    return AzureRunIdSource(
        account_name=account_name,
        container=container,
        prefix=normalized_prefix or "feed/",
    )


def _run_az_json(command: Sequence[str]) -> Any:
    proc = subprocess.run(
        list(command),
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "Azure CLI command failed.\n"
            f"Command: {' '.join(command)}\n"
            f"Exit code: {proc.returncode}\n"
            f"STDERR:\n{proc.stderr}"
        )
    try:
        return json.loads(proc.stdout or "null")
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Azure CLI did not return valid JSON.\n"
            f"Command: {' '.join(command)}\n"
            f"STDOUT (first 2000 chars):\n{(proc.stdout or '')[:2000]}"
        ) from exc


def ensure_az_login() -> None:
    """
    Ensure the user is authenticated with Azure CLI.

    We intentionally do not attempt to run interactive login flows from code.
    Instead we fail fast with clear instructions.
    """

    proc = subprocess.run(
        ["az", "account", "show", "--output", "none"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode == 0:
        return

    stderr = (proc.stderr or "").strip()
    raise RuntimeError(
        "Azure CLI is not authenticated (or not configured).\n\n"
        "Run this first:\n"
        "  az login\n\n"
        "If you have multiple subscriptions, set the right one:\n"
        "  az account list -o table\n"
        "  az account set --subscription <SUBSCRIPTION_ID_OR_NAME>\n\n"
        f"Azure CLI error:\n{stderr}"
    )


def list_run_ids_from_azure(
    *,
    source: AzureRunIdSource,
    discovery_prefix: Optional[str] = None,
    auth_mode: str = "login",
    max_runs: Optional[int] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
) -> List[str]:
    """
    List unique run_id folder names under a given Azure container + prefix.

    Uses `az storage blob list` and extracts run_id= values from the path.
    Only returns run_ids that contain a `YYYY-MM-DD` date.

    By default uses `discovery_prefix="feed/product/"` so run_ids are discovered
    from the product feed folder. The same run_id list applies to all pre_etl
    steps (e.g. cluster, store) since run_ids are shared across feed tables.
    """

    ensure_az_login()

    prefix = (discovery_prefix if discovery_prefix is not None else source.prefix).strip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    if prefix.startswith("/"):
        prefix = prefix.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    blobs = _run_az_json(
        [
            "az",
            "storage",
            "blob",
            "list",
            "--auth-mode",
            auth_mode,
            "--account-name",
            source.account_name,
            "--container-name",
            source.container,
            "--prefix",
            prefix,
            "--output",
            "json",
        ]
    )
    if blobs is None:
        blobs = []
    if not isinstance(blobs, list):
        raise TypeError(f"Unexpected Azure CLI output type: {type(blobs)}")

    run_ids: Dict[str, date] = {}
    for blob in blobs:
        name = (blob or {}).get("name")
        if not name or not isinstance(name, str):
            continue
        if not name.startswith(prefix):
            continue
        relative = name[len(prefix) :]
        if not relative:
            continue
        # Most common layout we see in datastore:
        #   feed/<table_name>/run_id=<run_id>/...
        # Fall back to assuming:
        #   feed/<run_id>/...
        run_id = None
        parts = [p for p in relative.split("/") if p]
        for part in parts:
            if part.startswith("run_id=") and len(part) > len("run_id="):
                run_id = part[len("run_id=") :]
                break
        if not run_id:
            run_id = parts[0] if parts else ""
        if not run_id:
            continue

        m = RUN_DATE_RE.search(run_id)
        if not m:
            continue
        run_dt = datetime.strptime(m.group("run_date"), "%Y-%m-%d").date()
        run_ids.setdefault(run_id, run_dt)

    def _coerce_dt(v: Optional[Union[str, date]]) -> Optional[date]:
        if v is None:
            return None
        if isinstance(v, date):
            return v
        return datetime.strptime(v, "%Y-%m-%d").date()

    start_dt = _coerce_dt(start_date)
    end_dt = _coerce_dt(end_date)

    filtered: List[Tuple[str, date]] = []
    for rid, rdt in run_ids.items():
        if start_dt and rdt < start_dt:
            continue
        if end_dt and rdt > end_dt:
            continue
        filtered.append((rid, rdt))

    # Most recent first
    filtered.sort(key=lambda x: (x[1], x[0]), reverse=True)
    if max_runs is not None:
        filtered = filtered[: max_runs]

    return [rid for rid, _ in filtered]


def parse_run_date_from_run_id(run_id: str) -> str:
    m = RUN_DATE_RE.search(run_id)
    if not m:
        raise ValueError(f"run_id does not contain a YYYY-MM-DD date: {run_id!r}")
    return m.group("run_date")


def _df_is_empty(df) -> bool:
    # Spark 3.3+ often has DataFrame.isEmpty()
    if hasattr(df, "isEmpty") and callable(getattr(df, "isEmpty")):
        return bool(df.isEmpty())
    return df.limit(1).count() == 0


def _count_output_rows(output: Any) -> Any:
    """
    Return row counts for an output that may be a DataFrame or nested dict/list of DataFrames.
    DataFrame -> int; dict -> dict of counts; list -> list of counts.
    """
    if hasattr(output, "schema") and hasattr(output, "count"):
        return output.count()
    if isinstance(output, dict):
        return {k: _count_output_rows(v) for k, v in output.items()}
    if isinstance(output, (list, tuple)):
        return [_count_output_rows(v) for v in output]
    return None


def _output_diff_summary(left: Any, right: Any, *, path: str = "$") -> Any:
    """
    For each DataFrame pair in left/right, return left_count, right_count, only_in_left, only_in_right.
    Returns same structure as output (dict/list) with per-DF stats, or single dict for a single DF.
    """
    if hasattr(left, "schema") and hasattr(right, "schema"):
        left_count = left.count()
        right_count = right.count()
        only_in_left = left.exceptAll(right).count()
        only_in_right = right.exceptAll(left).count()
        return {
            "left_count": left_count,
            "right_count": right_count,
            "only_in_left": only_in_left,
            "only_in_right": only_in_right,
            "path": path,
        }
    if isinstance(left, dict) and isinstance(right, dict):
        return {k: _output_diff_summary(left[k], right[k], path=f"{path}.{k}") for k in left.keys() if k in right}
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return [_output_diff_summary(l, r, path=f"{path}[{i}]") for i, (l, r) in enumerate(zip(left, right))]
    return None


def _field_str(field: Any) -> str:
    """Format a StructField as 'name: type (nullable)'."""
    name = getattr(field, "name", "?")
    dtype = getattr(field, "dataType", None)
    type_str = dtype.simpleString() if dtype and hasattr(dtype, "simpleString") else "?"
    nullable = getattr(field, "nullable", None)
    if nullable is not None:
        return f"{name}: {type_str} (nullable={nullable})"
    return f"{name}: {type_str}"


def _schema_diff_two_dataframes(left: Any, right: Any, *, path: str = "$") -> Dict[str, Any]:
    """
    Compare schemas of two Spark DataFrames. Returns dict with:
    - match: bool
    - legacy_columns, refactored_columns: list of str (one per field, side-by-side order)
    - differences: list of str (human-readable diff lines)
    """
    result: Dict[str, Any] = {
        "match": True,
        "legacy_columns": [],
        "refactored_columns": [],
        "differences": [],
        "path": path,
    }
    if not (hasattr(left, "schema") and hasattr(right, "schema")):
        return result

    lfields = list(left.schema.fields)
    rfields = list(right.schema.fields)
    n = max(len(lfields), len(rfields))
    for i in range(n):
        result["legacy_columns"].append(
            _field_str(lfields[i]) if i < len(lfields) else "(missing)"
        )
        result["refactored_columns"].append(
            _field_str(rfields[i]) if i < len(rfields) else "(missing)"
        )

    lf = {f.name: (f.dataType.simpleString(), f.nullable) for f in lfields}
    rf = {f.name: (f.dataType.simpleString(), f.nullable) for f in rfields}
    only_legacy = set(lf) - set(rf)
    only_refactored = set(rf) - set(lf)
    for name in sorted(only_legacy):
        result["differences"].append(f"column '{name}' only in legacy")
        result["match"] = False
    for name in sorted(only_refactored):
        result["differences"].append(f"column '{name}' only in refactored")
        result["match"] = False
    for name in sorted(set(lf) & set(rf)):
        lt, ln = lf[name]
        rt, rn = rf[name]
        if lt != rt or ln != rn:
            result["match"] = False
            result["differences"].append(
                f"column '{name}': legacy ({lt}, nullable={ln}) vs refactored ({rt}, nullable={rn})"
            )
    if not result["differences"] and list(left.schema.fieldNames()) != list(right.schema.fieldNames()):
        result["match"] = False
        result["differences"].append("column order differs: legacy order vs refactored order")
    return result


def _output_schema_diff(left: Any, right: Any, *, path: str = "$") -> Any:
    """
    For each DataFrame pair in left/right, return schema comparison (match, legacy_schema,
    refactored_schema, differences). Same structure as output; single DF -> single dict.
    """
    if hasattr(left, "schema") and hasattr(right, "schema"):
        return _schema_diff_two_dataframes(left, right, path=path)
    if isinstance(left, dict) and isinstance(right, dict):
        return {
            k: _output_schema_diff(left[k], right[k], path=f"{path}.{k}")
            for k in left.keys()
            if k in right
        }
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return [
            _output_schema_diff(l, r, path=f"{path}[{i}]")
            for i, (l, r) in enumerate(zip(left, right))
        ]
    return None


def assert_df_equal_exact(left, right, *, diff_rows: int = 20) -> None:
    """
    Assert Spark DataFrames are exactly equal:
    - same schema (names, order, types, nullability)
    - same rows, including duplicates (bidirectional exceptAll)
    """

    if left is None and right is None:
        return
    if left is None or right is None:
        raise AssertionError(f"One DataFrame is None and the other is not: left={left is None}, right={right is None}")

    # Schema: strict equality (order + types)
    if left.schema != right.schema:
        raise AssertionError(
            "Schema mismatch.\n"
            f"LEFT schema:  {left.schema.simpleString()}\n"
            f"RIGHT schema: {right.schema.simpleString()}"
        )

    left_count = left.count()
    right_count = right.count()
    if left_count != right_count:
        raise AssertionError(f"Row count mismatch: left={left_count}, right={right_count}")

    left_minus_right = left.exceptAll(right)
    right_minus_left = right.exceptAll(left)

    if not _df_is_empty(left_minus_right):
        sample = left_minus_right.limit(diff_rows).toPandas().to_dict(orient="records")
        raise AssertionError(
            "Left contains rows not in right (including duplicates).\n"
            f"Sample (up to {diff_rows} rows): {sample}"
        )

    if not _df_is_empty(right_minus_left):
        sample = right_minus_left.limit(diff_rows).toPandas().to_dict(orient="records")
        raise AssertionError(
            "Right contains rows not in left (including duplicates).\n"
            f"Sample (up to {diff_rows} rows): {sample}"
        )


def _format_counts_for_report(counts: Any, indent: str = "  ") -> str:
    """Format row counts (int or nested dict/list) for report file."""
    if isinstance(counts, int):
        return str(counts)
    if isinstance(counts, dict):
        lines = []
        for k, v in counts.items():
            sub = _format_counts_for_report(v, indent + "  ")
            first_line = sub.split("\n")[0] if "\n" in sub else sub
            lines.append(f"{indent}{k}: {first_line}")
            if "\n" in sub:
                for rest in sub.split("\n")[1:]:
                    lines.append(rest)
        return "\n".join(lines) if lines else "{}"
    if isinstance(counts, (list, tuple)):
        return "[" + ", ".join(_format_counts_for_report(c, indent) for c in counts) + "]"
    return str(counts)


def _format_diff_for_report(diff: Any, indent: str = "  ") -> str:
    """Format diff summary for report file."""
    if isinstance(diff, dict):
        if "left_count" in diff and "right_count" in diff:
            return (
                f"left_count={diff['left_count']}, right_count={diff['right_count']}, "
                f"only_in_left={diff.get('only_in_left', '?')}, only_in_right={diff.get('only_in_right', '?')}"
            )
        lines = []
        for k, v in diff.items():
            if k == "path":
                continue
            lines.append(f"{indent}{k}: {_format_diff_for_report(v, indent + '  ')}")
        return "\n".join(lines) if lines else "{}"
    if isinstance(diff, (list, tuple)):
        return "[" + ", ".join(_format_diff_for_report(d, indent) for d in diff) + "]"
    return str(diff)


def _diff_one_line_summary(diff: Any) -> Optional[str]:
    """If diff is a single DF block, return a one-line summary; else None."""
    if isinstance(diff, dict) and "left_count" in diff and "right_count" in diff:
        only_l = diff.get("only_in_left", "?")
        only_r = diff.get("only_in_right", "?")
        try:
            total = int(only_l) + int(only_r)
            return f"rows only in legacy: {only_l}, only in refactored: {only_r}, total differing rows: {total}"
        except (TypeError, ValueError):
            return f"rows only in legacy: {only_l}, only in refactored: {only_r}"
    return None


def _format_schema_diff_for_report(schema_diff: Any, indent: str = "  ") -> str:
    """Format schema diff for report file: two-column table legacy | refactored."""
    if isinstance(schema_diff, dict):
        if "legacy_columns" in schema_diff and "refactored_columns" in schema_diff:
            leg_cols = schema_diff.get("legacy_columns") or []
            ref_cols = schema_diff.get("refactored_columns") or []
            col_width = 52
            lines = [
                f"match: {schema_diff.get('match', '?')}",
                "",
                f"  {'legacy':<{col_width}} | refactored",
                "  " + "-" * col_width + "-+-" + "-" * col_width,
            ]
            for i in range(max(len(leg_cols), len(ref_cols))):
                leg = leg_cols[i] if i < len(leg_cols) else ""
                ref = ref_cols[i] if i < len(ref_cols) else ""
                lines.append(f"  {leg:<{col_width}} | {ref}")
            diffs = schema_diff.get("differences") or []
            if diffs:
                lines.append("")
                lines.append("  differences:")
                for d in diffs:
                    lines.append(f"    - {d}")
            return "\n".join(lines)
        lines = []
        for k, v in schema_diff.items():
            if k == "path":
                continue
            sub = _format_schema_diff_for_report(v, indent + "  ")
            lines.append(f"{indent}{k}:")
            for line in sub.split("\n"):
                lines.append(indent + line)
        return "\n".join(lines) if lines else "{}"
    if isinstance(schema_diff, (list, tuple)):
        return "\n".join(_format_schema_diff_for_report(d, indent) for d in schema_diff)
    return str(schema_diff)


def _write_summary_to_report(report_file: Any, summary: Dict[str, Any]) -> None:
    """Append one run_id summary block to an open report file."""
    report_file.write("-" * 60 + "\n")
    report_file.write(f"run_id:     {summary['run_id']}\n")
    report_file.write(f"run_date:   {summary['run_date']}\n")
    report_file.write(f"raw_data:   {summary['raw_data_path']}\n")
    report_file.write(f"status:     {summary['status']}\n")
    report_file.write("legacy row counts:\n")
    report_file.write(_format_counts_for_report(summary["legacy_row_counts"]) + "\n")
    report_file.write("refactored row counts:\n")
    report_file.write(_format_counts_for_report(summary["refactored_row_counts"]) + "\n")
    diff = summary.get("diff_summary")
    if diff and diff != "(could not compute)":
        report_file.write("row difference counts (legacy vs refactored):\n")
        one_line = _diff_one_line_summary(diff)
        if one_line:
            report_file.write(f"  {one_line}\n")
        report_file.write("  details: " + _format_diff_for_report(diff).replace("\n", "\n  ") + "\n")
    elif diff == "(could not compute)":
        report_file.write("row difference counts: (could not compute)\n")
    schema_diff = summary.get("schema_diff")
    if schema_diff:
        report_file.write("schema (legacy vs refactored):\n")
        report_file.write(_format_schema_diff_for_report(schema_diff).replace("\n", "\n  ") + "\n")
    if summary.get("error"):
        report_file.write(f"error: {summary['error']}\n")
    report_file.write("\n")


def _assert_outputs_equal_exact(left: Any, right: Any, *, path: str = "$") -> None:
    """
    Compare outputs that may be a DataFrame or nested structure of DataFrames.
    """

    # DataFrame: duck-type by schema attribute
    if hasattr(left, "schema") and hasattr(right, "schema"):
        try:
            assert_df_equal_exact(left, right)
            return
        except AssertionError as exc:
            raise AssertionError(f"Mismatch at {path}: {exc}") from exc

    if isinstance(left, dict) and isinstance(right, dict):
        if set(left.keys()) != set(right.keys()):
            raise AssertionError(f"Key mismatch at {path}: left={sorted(left.keys())}, right={sorted(right.keys())}")
        for k in left.keys():
            _assert_outputs_equal_exact(left[k], right[k], path=f"{path}.{k}")
        return

    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        if len(left) != len(right):
            raise AssertionError(f"Length mismatch at {path}: left={len(left)}, right={len(right)}")
        for i, (l, r) in enumerate(zip(left, right)):
            _assert_outputs_equal_exact(l, r, path=f"{path}[{i}]")
        return

    # Fallback: strict python equality
    if left != right:
        raise AssertionError(f"Value mismatch at {path}: left={left!r}, right={right!r}")


class PreETLRefinComparator:
    """
    Compare a legacy and refactored PreETL step for multiple run ids.

    run_id, run_date, and raw_data_path are not in config — they are obtained
    via Azure CLI (discover_run_ids, parse_run_date_from_run_id,
    _raw_data_path_for_run_id).

    Flow:
    - Get run_ids from Azure (feed/product/); for each run_id get run_date and
      raw_data_path via Azure CLI helpers.
    - Initialize both legacy and refactored classes with (run_id, run_date,
      raw_data_path).
    - Call read() on one instance; use its output as input_data.
    - Call main(input_data) on both; compare their main() outputs exactly.
    """

    def __init__(
        self,
        *,
        legacy_step_cls: type,
        refactored_step_cls: type,
        config_section: str,
        csv_delimiter: str = ",",
        raw_data_path_template: Optional[str] = None,
        step_kwargs: Optional[Dict[str, Any]] = None,
    ):
        self.legacy_step_cls = legacy_step_cls
        self.refactored_step_cls = refactored_step_cls
        self.config_section = config_section
        self.csv_delimiter = csv_delimiter
        self.raw_data_path_template = raw_data_path_template
        self.step_kwargs = step_kwargs or {}

    def _raw_data_path_for_run_id(
        self,
        run_id: str,
        *,
        main_yaml_path: Optional[str] = None,
        auth_mode: str = "login",
    ) -> str:
        """
        Get raw_data_path for a run_id. Not in config — resolved via Azure CLI.

        Precedence:
        1. RAW_DATA_PATH env: single path for all runs (override for testing).
        2. raw_data_path_template: e.g. "/mnt/invent-aloyoga-input/history/{run_id}/".
        3. Azure CLI: input_bucket from main.yaml, list history/ prefix in shared
           account, pick most recent folder for run_date; return /mnt/<bucket>/history/<folder>/.
        """
        main_yaml = main_yaml_path or _DEFAULT_MAIN_YAML
        single = os.getenv("RAW_DATA_PATH")
        if single:
            single = single.rstrip("/") + "/"
            return single

        if self.raw_data_path_template:
            path = self.raw_data_path_template.format(run_id=run_id)
            return path.rstrip("/") + "/"

        # Input bucket lives in shared_azure_account_name (see main.yaml manifest).
        ensure_az_login()
        input_bucket = get_input_bucket_from_main_yaml(main_yaml)
        shared_account = get_shared_azure_account_name_from_main_yaml(main_yaml)
        run_date = parse_run_date_from_run_id(run_id)
        folders = list_history_folders_for_run_date(
            shared_account,
            input_bucket,
            run_date,
            auth_mode=auth_mode,
        )
        if not folders:
            raise ValueError(
                f"No history folder found for run_date={run_date!r} under "
                f"account {shared_account!r} container {input_bucket!r} prefix history/. "
                "Run 'az login' and ensure you have access to the shared storage account."
            )
        # Pick most recent folder that day (last in sort order, e.g. 2026-02-04T092741.008Z)
        folder = folders[-1]
        return f"/mnt/{input_bucket}/history/{folder}/"

    def discover_run_ids(
        self,
        *,
        main_yaml_path: Optional[str] = None,
        discovery_prefix: str = "feed/product/",
        max_runs: Optional[int] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        auth_mode: str = "login",
    ) -> List[str]:
        """
        Discover run_ids by listing under feed/product/ (or discovery_prefix).

        The same run_id list is used for all pre_etl comparisons (cluster, store, etc.)
        since run_ids are shared across feed tables.
        """
        main_yaml = main_yaml_path or _DEFAULT_MAIN_YAML
        source = derive_azure_source_from_main_yaml(main_yaml, prefix="feed/")
        return list_run_ids_from_azure(
            source=source,
            discovery_prefix=discovery_prefix,
            auth_mode=auth_mode,
            max_runs=max_runs,
            start_date=start_date,
            end_date=end_date,
        )

    def _instantiate_step(
        self,
        step_cls: type,
        *,
        run_id: str,
        run_date: str,
        raw_data_path: str,
    ):
        # PreETL step: config_section, raw_data_path, csv_delimiter, run_id, run_date, **step_kwargs.
        return step_cls(
            config_section=self.config_section,
            raw_data_path=raw_data_path,
            csv_delimiter=self.csv_delimiter,
            run_id=run_id,
            run_date=run_date,
            **self.step_kwargs,
        )

    def compare_for_run_id(
        self,
        run_id: str,
        *,
        main_yaml_path: Optional[str] = None,
        auth_mode: str = "login",
    ) -> Dict[str, Any]:
        """
        Compare legacy vs refactored for one run_id. Returns a summary dict.
        On AssertionError (output mismatch) returns summary with status="FAIL" and does not raise.
        Other exceptions (e.g. read failure) are not caught.
        """
        run_date = parse_run_date_from_run_id(run_id)
        raw_data_path = self._raw_data_path_for_run_id(
            run_id,
            main_yaml_path=main_yaml_path,
            auth_mode=auth_mode,
        )

        legacy = self._instantiate_step(
            self.legacy_step_cls,
            run_id=run_id,
            run_date=run_date,
            raw_data_path=raw_data_path,
        )
        refactored = self._instantiate_step(
            self.refactored_step_cls,
            run_id=run_id,
            run_date=run_date,
            raw_data_path=raw_data_path,
        )

        input_data = legacy.read()
        legacy_out = legacy.main(input_data=input_data)
        refactored_out = refactored.main(input_data=input_data)

        legacy_counts = _count_output_rows(legacy_out)
        refactored_counts = _count_output_rows(refactored_out)

        try:
            diff_summary = _output_diff_summary(legacy_out, refactored_out)
        except Exception:  # pylint: disable=broad-except
            diff_summary = "(could not compute)"

        try:
            schema_diff = _output_schema_diff(legacy_out, refactored_out)
        except Exception:  # pylint: disable=broad-except
            schema_diff = "(could not compute)"

        summary: Dict[str, Any] = {
            "run_id": run_id,
            "run_date": run_date,
            "raw_data_path": raw_data_path,
            "status": "PASS",
            "legacy_row_counts": legacy_counts,
            "refactored_row_counts": refactored_counts,
            "diff_summary": diff_summary,
            "schema_diff": schema_diff,
            "error": None,
        }

        try:
            _assert_outputs_equal_exact(legacy_out, refactored_out)
        except AssertionError as exc:
            summary["status"] = "FAIL"
            summary["error"] = str(exc)
        return summary

    def compare_all(
        self,
        *,
        run_ids: Optional[Sequence[str]] = None,
        max_runs: Optional[int] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        main_yaml_path: Optional[str] = None,
        auth_mode: str = "login",
        report_path: Optional[str] = None,
    ) -> List[str]:
        """
        Compare for many run_ids. Returns the list of run_ids that passed.
        If report_path is set, writes a text summary to that file after each run_id.
        Raises on first failure (after writing that run's summary to the report).
        """

        main_yaml = main_yaml_path or _DEFAULT_MAIN_YAML
        if run_ids is None:
            run_ids = self.discover_run_ids(
                main_yaml_path=main_yaml,
                max_runs=max_runs,
                start_date=start_date,
                end_date=end_date,
                auth_mode=auth_mode,
            )

        report_file = None
        if report_path:
            report_dir = os.path.dirname(report_path)
            if report_dir:
                os.makedirs(report_dir, exist_ok=True)
            report_file = open(report_path, "w", encoding="utf-8")
            report_file.write("PreETL comparison report\n")
            report_file.write("=" * 60 + "\n")
            report_file.write(f"run_ids: {len(run_ids)}\n\n")

        passed: List[str] = []
        try:
            for rid in run_ids:
                summary = self.compare_for_run_id(
                    rid,
                    main_yaml_path=main_yaml,
                    auth_mode=auth_mode,
                )
                if report_file:
                    _write_summary_to_report(report_file, summary)
                if summary["status"] == "FAIL":
                    raise AssertionError(summary["error"])
                passed.append(rid)
        finally:
            if report_file:
                report_file.write("\n" + "=" * 60 + "\n")
                report_file.write(f"Passed: {len(passed)} / {len(run_ids)}\n")
                report_file.close()
        return passed


def _is_likely_secret(key: str) -> bool:
    """True if the env var name suggests a secret (value will be redacted)."""
    k = key.upper()
    return any(
        x in k
        for x in ("SECRET", "PASSWORD", "TOKEN", "KEY", "CREDENTIAL", "AUTH", "PRIVATE")
    )


def _print_environment_variables() -> None:
    """Print all environment variables, sorted by key. Redacts values for likely secrets."""
    print("environment variables:")
    for key in sorted(os.environ.keys()):
        value = os.environ[key]
        if _is_likely_secret(key):
            value = "(redacted)" if value else "(not set)"
        print(f"  {key}={value}")

    print("")


def print_environment_diagnostics() -> None:
    """
    Print machine and Spark info so you can compare with the notebook.
    Run this in the script and the same checks in the notebook to see if
    they run on the same machine / same Spark.
    """
    print("=== Environment diagnostics (compare with notebook) ===")
    print(f"hostname: {socket.gethostname()}")
    print(f"cwd: {os.getcwd()}")
    print(f"python: {sys.executable}")
    print(f"VIRTUAL_ENV: {os.environ.get('VIRTUAL_ENV', '(not set)')}")
    customer_name = get_customer_name_from_main_yaml(_DEFAULT_MAIN_YAML)
    input_bucket = f"invent-{customer_name}-input" if customer_name else None
    if input_bucket:
        mount_path = f"/mnt/{input_bucket}"
        print(f"{mount_path} exists: {os.path.exists(mount_path)}")
        print(f"{mount_path}/history exists: {os.path.exists(mount_path + '/history')}")
    else:
        print("customer_name not in config, skipping input path checks")

    # List all environment variables (redact likely secrets)
    _print_environment_variables()

    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            print("spark: (active session)")
            print(f"  appName: {spark.sparkContext.appName}")
            print(f"  version: {spark.version}")
            print(f"  master: {spark.sparkContext.master}")
            try:
                print(f"  applicationId: {spark.sparkContext.applicationId}")
            except Exception:  # pylint: disable=broad-except
                pass
        else:
            print("spark: (no active session yet)")
    except ImportError:
        print("spark: (pyspark not imported yet)")
    print("=== end diagnostics ===")


def _compare_pre_etl_example(
    script_name: str,
    max_runs: Optional[int] = None,
    timeout_minutes: Optional[int] = 20,
    report_path: Optional[str] = None,
) -> None:
    """
    Compare legacy vs refactored PreETL step for a given script name.

    script_name: e.g. "goods_in_transit" or "cluster".
    - Refactored: rocks_extension.opal.pre_etl.<script_name>
    - Legacy: rocks_extension.opal.pre_etl.legacy.<script_name>_legacy
    - config_section: pre_etl_<script_name>
    """
    # ensure_delta_spark_session()
    print_environment_diagnostics()

    # Defaults: config prefix = repo root (script lives in .invent-refactoring-engine/scripts/).
    main_yaml_path = _DEFAULT_MAIN_YAML
    customer_name = get_customer_name_from_main_yaml(main_yaml_path)
    apply_local_env_defaults(
        config_prefix=_REPO_ROOT + os.sep,
        datastore_bucket_name=f"invent-{customer_name}-datastore" if customer_name else None,
        pokedex_cloud_provider="azure",
        pokedex_environment="prod",
        shu_secret_name=f"cheetah-shu-{customer_name}" if customer_name else None,
    )

    legacy_cls, refactored_cls = load_legacy_and_refactored_classes(script_name)
    config_section = f"pre_etl_{script_name}"

    # run_id, run_date, raw_data_path from Azure CLI. Optional: RAW_DATA_PATH or RAW_DATA_PATH_TEMPLATE.
    comparator = PreETLRefinComparator(
        legacy_step_cls=legacy_cls,
        refactored_step_cls=refactored_cls,
        config_section=config_section,
        csv_delimiter="|",
        raw_data_path_template=os.getenv("RAW_DATA_PATH_TEMPLATE"),
        step_kwargs={
            "customer_config_path": "scripts/opal/config.yaml",
            "pipeline_name": "pre_etl",
        },
    )

    result: List[Optional[List[str]]] = [None]
    exc: List[Optional[BaseException]] = [None]

    def run() -> None:
        try:
            result[0] = comparator.compare_all(
                max_runs=max_runs,
                report_path=report_path,
            )
        except BaseException as e:
            exc[0] = e

    timeout_seconds = (timeout_minutes or 0) * 60 if timeout_minutes else 0
    if timeout_seconds > 0:
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=timeout_seconds)
        if thread.is_alive():
            print(f"Timed out after {timeout_minutes} minutes.", file=sys.stderr)
            sys.exit(1)
        if exc[0] is not None:
            raise exc[0]
    else:
        run()
        if exc[0] is not None:
            raise exc[0]


def _parse_args():
    import argparse

    p = argparse.ArgumentParser(
        description="Compare legacy vs refactored PreETL step. "
        "Give script name (e.g. goods_in_transit, cluster); refactored is pre_etl.<name>, legacy is pre_etl.legacy.<name>_legacy."
    )
    p.add_argument(
        "script",
        type=str,
        nargs="?",
        default=None,
        metavar="SCRIPT",
        help="PreETL script name (e.g. goods_in_transit, cluster). Refactored: pre_etl.<SCRIPT>; legacy: pre_etl.legacy.<SCRIPT>_legacy. Omit when using --diagnostics-only.",
    )
    p.add_argument(
        "--diagnostics-only",
        action="store_true",
        help="Only print environment diagnostics (host, env vars, paths) and exit. Does not start Spark.",
    )
    p.add_argument(
        "--max-runs",
        type=int,
        default=None,
        metavar="N",
        help="Max number of run_ids to compare (default: no limit).",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=20,
        metavar="MINUTES",
        help="Abort after this many minutes (default: 20).",
    )
    p.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        metavar="FILE",
        help="Write a text report to FILE (default: eggs/comparison_report_<SCRIPT>.txt).",
    )
    return p.parse_args()


def _default_report_path(script_name: str) -> str:
    """Default report path under repo eggs/ including the compared script name."""
    return os.path.join(_REPO_ROOT, "eggs", f"comparison_report_{script_name}.txt")


if __name__ == "__main__":
    args = _parse_args()
    if args.diagnostics_only:
        print_environment_diagnostics()
        sys.exit(0)
    if not args.script:
        print("error: the following arguments are required: SCRIPT", file=sys.stderr)
        sys.exit(2)
    report_path = args.output if args.output else _default_report_path(args.script)
    _compare_pre_etl_example(
        script_name=args.script,
        max_runs=args.max_runs,
        timeout_minutes=args.timeout,
        report_path=report_path,
    )
