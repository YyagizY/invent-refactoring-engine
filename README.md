# Automated Pre-ETL Refactoring Engine (APRE)

Automated pipeline to standardize, document, and validate client Pre-ETL PySpark scripts. LLM-based **agents** transform legacy technical debt into clean, maintainable, business-aligned code without changing functional output. Integrates with Cursor IDE.

---

## The three agents

| Agent | Command | Purpose |
|-------|---------|--------|
| **Structural Refactor (SRA)** | `structural_refactor path/to/pre_etl_script.py` | Linearize flow, remove dead code, enforce style. No business context needed. |
| **Contextual Refactor (CRA)** | `contextual_refactor path/to/pre_etl_script.py` | Add business-semantic comments only. Fetches context from Confluence + Invent Data Requirements; **does not modify executable code**. |
| **Validator (VA)** | `refactor_validator <script_name> [run_count]` | Run legacy vs refactored script on real data; row/schema parity check and pass/fail report. |

---

## Quick start

### Install

From your pipeline project root (e.g. customer-pipeline repo):

```bash
git submodule add https://github.com/YyagizY/invent-refactoring-engine.git .invent-refactoring-engine
./.invent-refactoring-engine/install.sh
```

If you get "Permission denied": `chmod +x .invent-refactoring-engine/install.sh`

### Use in Cursor

- **Confluence:** Uses [atlassian-cli](https://github.com/omar16100/atlassian-cli) only. Install it (e.g. `brew install omar16100/atlassian-cli/atlassian-cli`) and run `atlassian-cli auth login` once: `atlassian-cli auth login --profile <name> --base-url https://invent.atlassian.net --email <your@email> --token <api_token> --default`. No Confluence token env var is needed.
- **GITHUB_TOKEN:** Optional. Used for private Invent repo when `phase_b.github.token` is not set. If you prefer not to use a token, install [GitHub CLI](https://cli.github.com/) and run `gh auth login` once; the script will then clone via `gh repo clone` and no token is needed.

---

---

## Structural Refactor Agent (SRA)

from the **project root**. Dependencies for the script: `pip install -r .invent-refactoring-engine/scripts/requirements.txt` (PyYAML).

---

## Contextual Refactor Agent (CRA) setup

CRA adds comments by pulling context from Confluence and the Invent Data Requirements repo. A **fetcher script** runs before the agent: it writes aggregated context to `.cursor/context/<script_stem>.md`, which the agent uses. CRA may only add or update comments; it does not change executable code.

### Config

In the engine (or your project’s `.invent-refactoring-engine/config/external-sources.json`):

- **phase_b.confluence.base_url** — Confluence base URL (e.g. `https://invent.atlassian.net/wiki`).
- **phase_b.confluence.clients** — Map `customer_name` (from `dags/config/main.yaml` → `global_settings.customer_name`) to Confluence page URL or page id.
- **phase_b.confluence.email** (or **CONFLUENCE_EMAIL**) — Your Atlassian email; required for Confluence Cloud when using an API token (ATATT...).
- **phase_b.github.repo** — Invent Data Requirements repo URL.
- **phase_b.github.token** / **phase_b.github.token_env** — GitHub token for private repo; default env name is `GITHUB_TOKEN`.

### Environment

- **CONFLUENCE_API_TOKEN** — Use `email:api_key` (Atlassian API token) or Bearer. With ATATT tokens, also set **CONFLUENCE_EMAIL** (or `phase_b.confluence.email`).
- **GITHUB_TOKEN** — Optional; for private Invent repo. Alternatively use `gh auth login` and the script can clone without a token.

### What the fetcher does

When you run `contextual_refactor path/to/scope.py`, the agent runs:

```bash
python .invent-refactoring-engine/scripts/fetch_context.py path/to/scope.py
```

from the **project root**. Dependencies: `pip install -r .invent-refactoring-engine/scripts/requirements.txt` (PyYAML, requests).

The fetcher: resolves `customer_name` from `main.yaml` → gets Confluence page from config → reads `rocks_extension/opal/config.yml` for table names → fetches Confluence + (if configured) Invent repo table sections → writes **.cursor/context/<script_stem>.md**. The agent then reads that file and adds comments only.

## Validator Agent (VA)

Runs **legacy** and **refactored** scripts in parallel over a recent window of real client data and compares output (row count + schema/values). Generates a pass/fail summary; any discrepancy is a hard failure.

### Invoke

```
refactor_validator <script_name> [run_count]
```

- **script_name** — Pre-ETL name (e.g. `cluster`, `scope`). Same as the script stem in `pre_etl/`.
- **run_count** — Optional; default **20** (number of recent run ids to compare).

Examples: `refactor_validator cluster`, `refactor_validator cluster 10`, `refactor_validator goods_in_transit 5`.

### What the agent does

1. Runs the comparison from project root (customer venv + `refin_comparison.py` with `--max-runs <N>`), which **writes** `eggs/comparison_report_<script_name>.txt`.
2. **Reads** that report file.
3. Returns a short summary: total runs, Passed/Failed, and any failed run ids (with run_date/error).

### Prerequisites

- Customer virtualenv (e.g. `aloyoga-venv`) activatable from project root.
- **refin_comparison.py** and its dependencies; Azure/Spark access and config as required by the script.
- Legacy script at `path/to/legacy/<script>_legacy.py` (created by SRA); refactored script at the original path.

### Troubleshooting

- **"Azure CLI is not authenticated"** — run `az login` (and `az account set` if needed).
- **Missing venv or import errors** — ensure the customer venv is installed; the agent will report and stop.
- **Report file missing** — agent will not invent a summary; confirm the comparison script wrote `eggs/comparison_report_<script_name>.txt`.

---

## Update the engine

```bash
git submodule update --remote .invent-refactoring-engine
./.invent-refactoring-engine/install.sh
```

Or: `./.invent-refactoring-engine/update.sh`
