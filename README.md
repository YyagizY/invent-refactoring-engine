# Invent Refactoring Engine

Automated refactoring engine for PySpark pre-ETL scripts, integrated with Cursor IDE.

## Quick Start (3 Steps)

### 1. Add Refactoring Engine

```bash
# In your project root
git submodule add https://github.com/YyagizY/invent-refactoring-engine.git .invent-refactoring-engine
```

### 2. Install

```bash
# From project root (no cd needed!)
./.invent-refactoring-engine/install.sh
```

If you get "Permission denied", run: `chmod +x .invent-refactoring-engine/install.sh`

### 3. Use in Cursor IDE

Open your project in Cursor and in chat type:

```
structural_refactor path/to/pre_etl_scope.py
```

That's it!

**What happens:** The original file is moved to `path/to/legacy/pre_etl_scope_legacy.py` and the refactored version is written to the original path.

---

## Update to New Version

When a new version is available, update the submodule and reinstall:

```bash
# Step 1: Update submodule to latest version (from project root)
git submodule update --remote .invent-refactoring-engine

# Step 2: Reinstall to update Cursor rules
./.invent-refactoring-engine/install.sh
```

**Alternative (using update script):**
```bash
# From project root
./.invent-refactoring-engine/update.sh
```

The update script will update the repo and reinstall the Cursor rule.

**Note:** Using `git submodule update --remote` gives you more control and ensures you get the latest from the remote.

---

## Installation Methods

### Method 1: Git Submodule (Recommended)

```bash
# Add as submodule
git submodule add https://github.com/YyagizY/invent-refactoring-engine.git .invent-refactoring-engine

# Install (from project root)
./.invent-refactoring-engine/install.sh
```

**Update to latest:**
```bash
git submodule update --remote .invent-refactoring-engine
./.invent-refactoring-engine/install.sh
```

### Method 2: Direct Clone

```bash
# Clone into your project
git clone https://github.com/YyagizY/invent-refactoring-engine.git .invent-refactoring-engine

# Install (from project root)
./.invent-refactoring-engine/install.sh
```

**Update:**
```bash
./.invent-refactoring-engine/update.sh
```

---

## Commands (Cursor Chat)

| Command | Phase | Description |
|---------|-------|-------------|
| `structural_refactor path/to/script.py` | A | Structural & stylistic refactoring |
| `contextual_refactor path/to/script.py` | B | Planned: business documentation |
| `validate refactored.py legacy.py` | C | Planned: output comparison |

---

## Project Structure After Integration

```
your-project/
├── .cursor/
│   └── rules/
│       └── structural_refactor.mdc     # Installed by install.sh
├── .invent-refactoring-engine/         # Submodule
│   ├── .cursor/
│   │   └── rules/
│   │       └── structural_refactor.mdc
│   ├── config/
│   │   └── external-sources.json
│   ├── docs/
│   │   └── examples/
│   ├── install.sh
│   ├── update.sh
│   └── README.md
└── rocks_extension/
    └── opal/
        └── pre_etl/
            ├── pre_etl_scope.py
            └── legacy/
                └── pre_etl_scope_legacy.py
```

---

## Phase A: Transformation Rules

- **Scope**: Only modifies the `main` method
- **Data Ingestion**: All input reads at top of `main`
- **Variable Declaration**: Static variables after reads
- **Dead Code Removal**: Removes unused DataFrames
- **Redundancy Pruning**: Removes unused columns
- **Alias Cleanup**: Simplifies joins (no unnecessary aliases)
- **Broadcast Preservation**: Keeps existing broadcast hints
- **Visual Formatting**: Blank lines between different DataFrames
- **Linearity**: Define DataFrames close to where they're used
- **Output**: Refactored `main` returns the same output table as legacy

Style: [Invent Analytics PySpark Style Guide](https://github.com/inventanalytics/pyspark-style-guide). All chained expressions are broken (one operation per line).

---

## Future Phases

| Phase | Command | Description |
|-------|---------|-------------|
| B | `contextual_refactor` | Business documentation (Confluence, GitHub) |
| C | `validate` | Compare legacy vs refactored output |

---

## Troubleshooting

### Rule not working in Cursor

1. Ensure `.cursor/rules/structural_refactor.mdc` exists in your project root
2. Run install again from project root:
   ```bash
   ./.invent-refactoring-engine/install.sh
   ```
3. Restart Cursor IDE

### Update not working

1. From project root, check submodule status: `git submodule status`
2. Update manually: `git submodule update --remote .invent-refactoring-engine`
3. Reinstall: `./.invent-refactoring-engine/install.sh`

---

## Deploying This Repo to GitHub

From this project root:

```bash
git init
git add .
git commit -m "Initial commit: Phase A structural refactoring rule and docs"
git branch -M main
git remote add origin https://github.com/YyagizY/invent-refactoring-engine.git
git push -u origin main
```

Create the repo on GitHub first at [github.com/new](https://github.com/new) (name: `invent-refactoring-engine`, leave empty).

---

## License

Proprietary - Invent Analytics
