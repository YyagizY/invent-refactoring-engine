# Invent Refactoring Engine

Automated refactoring for PySpark pre-ETL scripts, integrated with Cursor IDE.

## Quick Start

### 1. Add and install

```bash
# From your project root (e.g. customer-pipeline repo)
git submodule add https://github.com/YyagizY/invent-refactoring-engine.git .invent-refactoring-engine
./.invent-refactoring-engine/install.sh
```

If you get "Permission denied": `chmod +x .invent-refactoring-engine/install.sh`

### 2. Use in Cursor

- **Phase A (structural):** `structural_refactor path/to/pre_etl_script.py`
- **Phase B (comments):** `contextual_refactor path/to/pre_etl_script.py`

---

## Phase B setup

Phase B adds semantic comments using Confluence and (optionally) the Invent Data Requirements repo. The agent runs the context fetch when you invoke `contextual_refactor`.

### Config

Edit **config/external-sources.json** in this repo:

- **phase_b.confluence.base_url:** Confluence base URL (e.g. `https://invent.atlassian.net/wiki`).
- **phase_b.confluence.clients:** Map `customer_name` (from the pipeline’s `dags/config/main.yaml` → `global_settings/customer_name`) to the Confluence page URL or page id, e.g. `"academyv2": "https://.../pages/123456/..."` or `"academyv2": "123456"`.
- **phase_b.github.repo:** (Optional) Invent Data Requirements repo URL for table/column definitions.
- **phase_b.github.token:** (Optional) GitHub token for private Invent repo. If set, used for clone; otherwise the env var named by `token_env` is used.
- **phase_b.github.token_env:** Env var name for GitHub token when `token` is not set (default: `GITHUB_TOKEN`).

### Environment

- **CONFLUENCE_API_TOKEN:** Required for Confluence. Use either:
  - Confluence Cloud Basic: `email:api_key` (e.g. from Atlassian API tokens), or
  - Bearer token if your instance supports it.
- **GITHUB_TOKEN:** Used for private Invent repo when `phase_b.github.token` is not set in config.

### Context fetcher (run by the agent)

When you run `contextual_refactor path/to/scope.py`, the agent runs:

```bash
python .invent-refactoring-engine/scripts/fetch_context.py path/to/scope.py
```

from the **project root**. Dependencies for the script: `pip install -r .invent-refactoring-engine/scripts/requirements.txt` (PyYAML, requests).

The script:

1. Reads `dags/config/main.yaml` → `global_settings/customer_name`.
2. Resolves the Confluence page from **config/external-sources.json** (`phase_b.confluence.clients[customer_name]`).
3. Reads `rocks_extension/opal/config.yml` for `pre_etl_<name>.sources.*.table_name`.
4. Fetches the Confluence page and (if configured) clones the Invent repo and searches for table sections.
5. Writes **.cursor/context/<script_stem>.md** (e.g. `.cursor/context/scope.md`).

The agent then reads that file and adds comments only (no code changes).

---

## Phase A (structural)

- Original script is moved to `path/to/legacy/<script>_legacy.py`; refactored script is written to the original path.
- Only the `main` method is modified; output table must remain identical.

## Phase B (contextual)

- Original script is moved to `path/to/contextual_refactor/<script_stem>_legacy.py`; commented script is written to the original path.
- Only comments are added or changed; code must stay identical.

---

## Update

```bash
git submodule update --remote .invent-refactoring-engine
./.invent-refactoring-engine/install.sh
```

Or: `./.invent-refactoring-engine/update.sh`

---

## Tutorial: Using the Refactor Validator

After you refactor a PreETL script (e.g. with `structural_refactor`), you can validate that the refactored code produces the same output as the legacy version over recent pipeline runs. The **refactor_validator** Cursor rule automates this: it runs the comparison for you and summarizes pass/fail and any failed run ids.

### When to use it

- After Phase A (structural refactor) to confirm the new script matches legacy output.
- When you want a quick “did my refactor break anything?” check over the last N run ids.

### How to invoke it

In Cursor, ask the agent to run the validator with:

```
refactor_validator <script_name> [run_count]
```

- **script_name**: The PreETL script name (e.g. `cluster`, `goods_in_transit`, `asn`, `product`). Use the same name as in your `pre_etl/` path (e.g. `cluster.py` → `cluster`).
- **run_count** (optional): How many recent run ids to compare. Default is **20** if you omit it.

**Examples:**

- `refactor_validator cluster` — compare last 20 run ids for `cluster`.
- `refactor_validator cluster 10` — compare last 10 run ids.
- `refactor_validator goods_in_transit 5` — compare last 5 run ids for `goods_in_transit`.

### What the agent does

The agent (following the rule in `.cursor/rules/refactor_validator.mdc`) will:

1. **Run the comparison** from your project root: it activates your customer virtualenv (e.g. `aloyoga-venv`) and runs `refin_comparison.py` with your script name and `--max-runs <N>`.
2. **Read the report** written to `eggs/comparison_report_<script_name>.txt`.
3. **Return a short validation summary**: total runs, Passed/Failed, and a list of any failed run ids (with run_date and error if present).

You get a concise “all passed” or “X run(s) failed” verdict without pasting the full report unless you ask for it.

### Prerequisites

- **Customer virtualenv** must exist and be activatable (e.g. `source aloyoga-venv/bin/activate`). The comparison script runs inside this environment.
- **refin_comparison.py** and its dependencies (see `.invent-refactoring-engine/scripts/`) must be available; the script needs Azure/Spark access and config as described in the comparison script’s own docs.
- **Legacy script** should be at `path/to/legacy/<script>_legacy.py` (as created by Phase A). Refactored script at the original path.

### If something goes wrong

- **“Azure CLI is not authenticated”** — run `az login` (and optionally `az account set`) before re-running.
- **Missing venv or import errors** — ensure the customer venv is installed and activated; the agent will report the error and stop.
- **Report file missing** — the agent will tell you and will not invent a summary; check that the comparison script completed and wrote to `eggs/comparison_report_<script_name>.txt`.
