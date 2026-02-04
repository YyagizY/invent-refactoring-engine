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

### Environment

- **CONFLUENCE_API_TOKEN:** Required for Confluence. Use either:
  - Confluence Cloud Basic: `email:api_key` (e.g. from Atlassian API tokens), or
  - Bearer token if your instance supports it.
- **GITHUB_TOKEN:** Optional; set for private Invent repo clone.

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
