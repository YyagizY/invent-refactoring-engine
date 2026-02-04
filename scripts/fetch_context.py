#!/usr/bin/env python3
"""
Phase B context fetcher. Run when contextual_refactor <script_path> is invoked.
Resolves customer_name from dags/config/main.yaml, Confluence page from config,
fetches Confluence + optional Invent repo, writes .cursor/context/<script_stem>.md.

Usage (from customer-pipeline project root):
  python .invent-refactoring-engine/scripts/fetch_context.py path/to/pre_etl/scope.py

Requires: PyYAML, requests (for Confluence).
Env: CONFLUENCE_API_TOKEN (email:api_key for Basic auth, or Bearer token).
     Optional GITHUB_TOKEN for private Invent repo (or set phase_b.github.token in config).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Phase B context for a pre_etl script.")
    parser.add_argument("script_path", help="Path to pre_etl script, e.g. rocks_extension/opal/pre_etl/scope.py")
    parser.add_argument("--project-root", default=os.getcwd(), help="Project root (default: cwd)")
    parser.add_argument("--engine", default=None, help="Refactoring engine dir (default: .invent-refactoring-engine under project root)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print log for Confluence/Invent fetch and section resolution")
    args = parser.parse_args()
    verbose = getattr(args, "verbose", False)

    project_root = Path(args.project_root).resolve()
    script_path = Path(args.script_path)
    if not script_path.is_absolute():
        script_path = project_root / script_path

    engine_dir = args.engine
    if not engine_dir:
        engine_dir = project_root / ".invent-refactoring-engine"
    else:
        engine_dir = Path(engine_dir).resolve()
    if not engine_dir.is_dir():
        print(f"Error: Engine directory not found: {engine_dir}", file=sys.stderr)
        return 1

    # Script stem: scope.py -> scope, pre_etl_scope.py -> pre_etl_scope (we want scope)
    stem = script_path.stem
    if stem.startswith("pre_etl_"):
        pre_etl_name = stem[len("pre_etl_"):]
    else:
        pre_etl_name = stem

    config_path = engine_dir / "config" / "external-sources.json"
    if not config_path.is_file():
        print(f"Error: Config not found: {config_path}", file=sys.stderr)
        return 1

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    phase_b = config.get("phase_b", {})
    confluence_cfg = phase_b.get("confluence", {})
    github_cfg = phase_b.get("github", {})

    # 1) customer_name from dags/config/main.yaml
    main_yaml = project_root / "dags" / "config" / "main.yaml"
    if not main_yaml.is_file():
        print(f"Error: main.yaml not found: {main_yaml}", file=sys.stderr)
        return 1

    try:
        import yaml
    except ImportError:
        print("Error: PyYAML required. pip install pyyaml", file=sys.stderr)
        return 1

    with open(main_yaml, encoding="utf-8") as f:
        main_data = yaml.safe_load(f) or {}
    global_settings = main_data.get("global_settings") or {}
    customer_name = global_settings.get("customer_name")
    if not customer_name:
        print("Error: global_settings.customer_name not found in main.yaml", file=sys.stderr)
        return 1

    # 2) Confluence page URL/id from config (clients or pages key)
    clients = confluence_cfg.get("clients") or confluence_cfg.get("pages") or {}
    page_ref = clients.get(customer_name)
    if not page_ref:
        print(f"Error: No Confluence page for customer_name={customer_name} in config", file=sys.stderr)
        return 1

    base_url = (confluence_cfg.get("base_url") or "").rstrip("/")
    if not base_url:
        print("Error: phase_b.confluence.base_url not set in config", file=sys.stderr)
        return 1

    page_id = _resolve_confluence_page_id(page_ref, base_url)
    if not page_id:
        print(f"Error: Could not resolve Confluence page id from: {page_ref}", file=sys.stderr)
        return 1

    # 3) Input table names from rocks_extension/opal/config.yml
    opal_config_path = project_root / "rocks_extension" / "opal" / "config.yml"
    if not opal_config_path.is_file():
        print(f"Warning: config.yml not found: {opal_config_path}", file=sys.stderr)
        input_table_names = []
    else:
        with open(opal_config_path, encoding="utf-8") as f:
            opal_config = yaml.safe_load(f) or {}
        pre_etl_key = f"pre_etl_{pre_etl_name}"
        pre_etl_block = opal_config.get(pre_etl_key) or {}
        sources = pre_etl_block.get("sources") or {}
        input_table_names = []
        for _key, src in sources.items():
            if isinstance(src, dict) and "table_name" in src:
                input_table_names.append(src["table_name"])

    output_table_name = f"{pre_etl_name}_feed"

    # 4) Fetch Confluence page (token: from config api_token, or env var named by api_token_env)
    token = confluence_cfg.get("api_token") or os.environ.get(confluence_cfg.get("api_token_env", "CONFLUENCE_API_TOKEN"))
    if not token:
        print("Error: Set phase_b.confluence.api_token (in config) or set the env var named by api_token_env", file=sys.stderr)
        return 1

    try:
        confluence_body = _fetch_confluence_page(base_url, page_id, token)
    except Exception as e:
        print(f"Error fetching Confluence page: {e}", file=sys.stderr)
        return 1

    # 5) Parse Confluence: Input Data Explanations per table, Pre-ETL Business Logics / pre_etl_name
    input_sections = _parse_confluence_input_explanations(confluence_body, input_table_names + [output_table_name])
    business_logic = _parse_confluence_business_logic(confluence_body, pre_etl_name)

    # 6) Optional: Invent repo search for output and input tables
    invent_repo = github_cfg.get("repo")
    requirements_path = github_cfg.get("requirements_path")
    github_token = github_cfg.get("token") or os.environ.get(github_cfg.get("token_env", "GITHUB_TOKEN"))
    invent_sections = {}
    if invent_repo:
        invent_sections = _fetch_invent_sections(
            invent_repo, output_table_name, input_table_names, requirements_path, github_token, verbose
        )

    # 7) Build context markdown
    context_md = _build_context_md(
        pre_etl_name=pre_etl_name,
        output_table_name=output_table_name,
        input_table_names=input_table_names,
        invent_sections=invent_sections,
        input_sections=input_sections,
        business_logic=business_logic,
    )

    # 8) Write .cursor/context/<stem>.md
    context_dir = project_root / ".cursor" / "context"
    context_dir.mkdir(parents=True, exist_ok=True)
    context_file = context_dir / f"{pre_etl_name}.md"
    context_file.write_text(context_md, encoding="utf-8")
    print(f"Wrote context: {context_file}")
    return 0


def _resolve_confluence_page_id(page_ref: str | dict, base_url: str) -> str | None:
    if isinstance(page_ref, dict):
        return page_ref.get("page_id") or page_ref.get("id")
    s = str(page_ref).strip()
    # Full URL: .../pages/123456/...
    m = re.search(r"/pages/(\d+)(?:/|$)", s)
    if m:
        return m.group(1)
    # Numeric id only
    if s.isdigit():
        return s
    return None


def _fetch_confluence_page(base_url: str, page_id: str, token: str) -> str:
    try:
        import requests
    except ImportError:
        raise RuntimeError("requests required. pip install requests")

    url = f"{base_url}/rest/api/content/{page_id}"
    params = {"expand": "body.storage"}
    # Confluence Cloud: Basic auth (email:api_token) or Bearer
    if token.startswith("Bearer"):
        headers = {"Authorization": token}
        auth = None
    elif ":" in token:
        email, api_key = token.split(":", 1)
        auth = (email, api_key)
        headers = {}
    else:
        headers = {"Authorization": f"Bearer {token}"}
        auth = None

    resp = requests.get(url, params=params, headers=headers, auth=auth, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    body = data.get("body") or {}
    storage = body.get("storage") or {}
    return storage.get("value") or ""


def _storage_to_text(html: str) -> str:
    """Rough conversion of Confluence storage (XML/HTML) to plain text."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|h[1-6]|li|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _parse_confluence_input_explanations(html: str, table_names: list[str]) -> dict[str, str]:
    """Extract per-table blocks from Input Data Explanations. Returns {table_name: content}."""
    text = _storage_to_text(html)
    out = {}
    # Find "Input Data Explanations" section (case-insensitive), then look for each table name as heading or block
    input_start = re.search(r"Input\s+Data\s+Explanations", text, re.IGNORECASE)
    if not input_start:
        return out
    rest = text[input_start.end():]
    # Pre-ETL Business Logics marks end of Input Data Explanations if present
    logic_start = re.search(r"Pre-ETL\s+Business\s+Logics", rest, re.IGNORECASE)
    if logic_start:
        rest = rest[: logic_start.start()]
    for name in table_names:
        # Look for table name as a heading (alone on line or followed by colon/newline)
        pat = re.compile(r"(?:\n|^)\s*" + re.escape(name) + r"\s*[:\n]", re.IGNORECASE)
        m = pat.search(rest)
        if m:
            start = m.start()
            # End at next known table name or end of rest
            next_m = None
            for other in table_names:
                if other == name:
                    continue
                nm = re.compile(r"(?:\n|^)\s*" + re.escape(other) + r"\s*[:\n]", re.IGNORECASE).search(rest, start + 1)
                if nm and (next_m is None or nm.start() < next_m.start()):
                    next_m = nm
            end = next_m.start() if next_m else len(rest)
            block = rest[start:end].strip()
            if block:
                out[name] = block
    return out


def _parse_confluence_business_logic(html: str, pre_etl_name: str) -> str:
    """Extract Pre-ETL Business Logics section for this pre_etl name."""
    text = _storage_to_text(html)
    logic_start = re.search(r"Pre-ETL\s+Business\s+Logics", text, re.IGNORECASE)
    if not logic_start:
        return ""
    rest = text[logic_start.end():]
    # Section for this name: ****\nscope:\n... or heading "scope" or "scope:"
    pat = re.compile(r"(?:\n|^)\s*\*{4}\s*\n\s*" + re.escape(pre_etl_name) + r"\s*:\s*\n", re.IGNORECASE)
    m = pat.search(rest)
    if m:
        after = rest[m.end():]
        end = re.search(r"\n\s*\*{4}\s*\n", after)
        return after[: end.start()].strip() if end else after.strip()
    # Fallback: heading with pre_etl_name
    pat2 = re.compile(r"(?:\n|^)\s*#" + r"\s*" + re.escape(pre_etl_name) + r"\s*[:\n]", re.IGNORECASE)
    m2 = pat2.search(rest)
    if m2:
        after = rest[m2.end():]
        end = re.search(r"\n\s*#+\s", after)
        return after[: end.start()].strip() if end else after.strip()
    return ""


def _fetch_invent_sections(
    repo_url: str,
    output_table: str,
    input_tables: list[str],
    requirements_path: str | None = None,
    token: str | None = None,
    verbose: bool = False,
) -> dict[str, str]:
    """Search Invent repo for table sections (table_name:). requirements_path = path inside repo. token = from config phase_b.github.token or env (token_env). Returns {table_name: content}."""
    def log(msg: str) -> None:
        if verbose:
            print(f"[Invent] {msg}", file=sys.stderr)

    tables = [output_table] + [t for t in input_tables if t != output_table]
    out = {}
    with tempfile.TemporaryDirectory(prefix="invent_repo_") as tmp:
        try:
            log(f"Cloning {repo_url} ...")
            if token and "github.com" in repo_url:
                # Use token for private repos
                from urllib.parse import urlparse
                parsed = urlparse(repo_url)
                auth_url = f"{parsed.scheme}://x-access-token:{token}@{parsed.netloc}{parsed.path}"
                r = subprocess.run(["git", "clone", "--depth", "1", "--single-branch", auth_url, tmp], capture_output=True, text=True, timeout=60)
            else:
                r = subprocess.run(["git", "clone", "--depth", "1", "--single-branch", repo_url, tmp], capture_output=True, text=True, timeout=60)
            if r.returncode != 0:
                log(f"Clone failed: {r.stderr or r.stdout or 'unknown'}")
                return out
            log("Clone OK")
        except subprocess.TimeoutExpired:
            log("Clone timed out")
            return out
        except subprocess.CalledProcessError as e:
            log(f"Clone error: {e}")
            return out
        search_root = Path(tmp) / requirements_path.strip("/") if requirements_path else Path(tmp)
        if not search_root.exists():
            log(f"requirements_path {requirements_path} not found in clone, using repo root")
            search_root = Path(tmp)
        for table in tables:
            pattern = f"{table}:"
            try:
                r = subprocess.run(
                    ["grep", "-r", "-l", pattern, str(search_root)],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if r.returncode != 0 or not r.stdout:
                    log(f"  {table}: not found (grep exit {r.returncode})")
                    continue
                # Get first file and extract section (lines containing pattern and following lines until next similar)
                first_line = r.stdout.strip().split("\n")[0].strip()
                file_path = first_line.split(":")[0].strip()
                path = Path(file_path) if Path(file_path).is_file() else search_root / file_path if (search_root / file_path).is_file() else Path(tmp) / file_path
                if not path.is_file():
                    log(f"  {table}: matched but file not found at {path}")
                    continue
                content = path.read_text(encoding="utf-8", errors="replace")
                idx = content.find(pattern)
                if idx != -1:
                    start = content.rfind("\n", 0, idx) + 1
                    rest = content[idx + len(pattern):]
                    end_match = re.search(r"\n\s*[\w_]+\s*:\s*\n", rest)
                    end_idx = idx + len(pattern) + (end_match.start() if end_match else len(rest))
                    block = content[start:end_idx].strip()
                    if block:
                        out[table] = block
                        log(f"  {table}: found in {path.name} ({len(block)} chars)")
                    else:
                        log(f"  {table}: found in {path.name} but extracted block empty")
                else:
                    log(f"  {table}: pattern in file but find() failed")
            except Exception as e:
                log(f"  {table}: error {e}")
                continue
    if verbose:
        print(f"[Invent] Resolved {len(out)} table(s) from repo", file=sys.stderr)
    return out


def _build_context_md(
    pre_etl_name: str,
    output_table_name: str,
    input_table_names: list[str],
    invent_sections: dict[str, str],
    input_sections: dict[str, str],
    business_logic: str,
) -> str:
    parts = []

    parts.append("# Pre-ETL Business Logic")
    parts.append(f"\n## {pre_etl_name}\n")
    parts.append(business_logic or "(No business logic section found.)")

    parts.append("\n\n# Output Table")
    parts.append(f"\n## {output_table_name}\n")
    parts.append(invent_sections.get(output_table_name) or input_sections.get(output_table_name) or "(No definition found.)")

    parts.append("\n\n# Input Tables\n")
    for t in input_table_names:
        if t == output_table_name:
            continue
        parts.append(f"## {t}\n\n")
        parts.append(invent_sections.get(t) or input_sections.get(t) or "(No definition found.)")
        parts.append("\n\n")

    return "".join(parts)


if __name__ == "__main__":
    sys.exit(main())
