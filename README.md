# ADME ACZ Silver Layer

Use the Azure Data Manager for Energy (ADME) Analytics Consumption Zone (ACZ) Silver Layer notebook to turn nested OSDU records into reusable Delta tables for analytics, reporting, and downstream data engineering.

The notebook reads bronze OSDU records from ACZ, unwraps Storage record envelopes in the bronze `data` payload, resolves schemas from the ADME schema service, flattens nested JSON, and writes Silver Layer Delta outputs into Microsoft Fabric or OneLake.

## What the notebook creates

| Output | Description | Example |
| --- | --- | --- |
| Parent table | One row per OSDU record with scalar and flattened object columns. | `osdu_wks_welllog` |
| Child tables | One table per array field, linked to the parent by `id` and ordered with `ordinal` when available. | `osdu_wks_welllog___curves` |
| Reassembled table | One wide table per kind when `OUTPUT_MODE = "wide"`. | `osdu_wks_welllog` |
| Relationship bridge | When enabled, one table per schema-declared scalar relationship path and target type, including 1:1 and 1:N links. | `relationship__<source-kind>__<field-path>__<target-type>` |
| Run metadata and quality issues | Processing status, row counts, timing, output manifest, run commit status, data-quality issues, and optional generated documentation. | `silver_run_info`, `silver_run_status`, `silver_data_quality_issues` |

Parent tables include the normalized kind authority and source, followed by the entity name, for example `osdu_wks_welllog` or `data_wks_file_generic`. Child tables use `{parent_table}___{array_field_path}` with the normalized full array field path, which keeps nested array outputs distinct. If `TABLE_PREFIX` is set, it is applied to generated parent, child, and relationship bridge table names.

## Architecture

```text
Azure Data Manager for Energy
        |
        v
Analytics Consumption Zone bronze Delta table
        |
        v
ADME ACZ Silver Layer notebook
        |
        +--> ADME schema service lookup
        +--> Type inference and flattening
        +--> Decomposition or reassembly
        |
        v
Silver Layer Delta tables in Fabric or OneLake
        |
        v
Analytics, reporting, and downstream data products
```

## Prerequisites

Before running the notebook, confirm that the customer environment has:

- A running Azure Data Manager for Energy instance.
- A configured Analytics Consumption Zone. For setup guidance, see [Enable Analytics Consumption Zone](https://learn.microsoft.com/en-us/azure/energy-data-services/how-to-enable-analytics-consumption-zone?tabs=bash).
- A Microsoft Fabric workspace and lakehouse that can access the ACZ bronze Delta table.
- Permission to read the bronze table and write Silver Layer Delta tables.
- An identity that can call the ADME schema service, including `GET /api/schema-service/v1/schema?latestVersion=False&limit=1`.
- For direct notebook runs, a service principal with its client secret stored in Key Vault. Device-code authentication is supported for interactive testing.
- A Fabric notebook runtime with a Synapse PySpark kernel.
- Network access from the runtime to the ADME endpoint, for example `https://contoso.energy.azure.com`.

## Quick start

1. Download [`ADME ACZ Silver Layer.ipynb`](ADME%20ACZ%20Silver%20Layer.ipynb) and import it into Microsoft Fabric.
2. Attach the lakehouse that contains the ACZ bronze table, or set `WORKSPACE_ID` and `LAKEHOUSE_ID`.
3. Edit only the customer settings in the dedicated Configuration cell, then run the adjacent configuration-code cell.
4. Keep `RUN_PROFILE = "inspect"` and run the Setup checklist and Smoke test sections.
5. Set `RUN_PROFILE = "dry_run"` and run the Run pipeline section to preview planned outputs without writing Silver tables.
6. Set `RUN_PROFILE = "execute"` when the dry run looks correct. `RUN_PROFILE` controls whether the pipeline runs; `WRITE_MODE` independently controls whether it uses `upsert` or `full_refresh`. Set `ALLOW_OVERWRITE = True` only when replacing existing output tables is intended.
7. Review the generated Silver tables, including relationship-specific bridge tables, and the run records in `silver_run_info`, `silver_run_manifest`, and `silver_run_status`.

For an opt-in table-group concurrency trial across Well, Wellbore, WellLog, WellboreMarkerSet, WellboreTrajectory, and CoordinateReferenceSystem kinds, import [ADME ACZ Silver Parallelism Experiment.ipynb](ADME%20ACZ%20Silver%20Parallelism%20Experiment.ipynb). Compare `GROUP_PROCESSING_PARALLELISM = 1` with `2`, keeping inputs and resources fixed and using a distinct `TABLE_PREFIX` for each run. Full refresh, schema preflight, batched metadata and `OUTPUT_WRITE_PARALLELISM = 1` are required. Run trials sequentially in a dedicated test lakehouse: output prefixes do not isolate metadata tables or schema caches. The experiment is not a production scheduling recommendation.

## Customer configuration

Most customers only need the settings in this section. The remaining notebook settings are advanced defaults for incremental refreshes, metadata tables, schema caching, and performance tuning.

| Decision | Settings | Guidance |
| --- | --- | --- |
| Fabric target | `WORKSPACE_ID`, `LAKEHOUSE_ID` | Leave both blank when the Fabric lakehouse is attached. Set them only for scheduled or detached runs that need explicit OneLake resolution. |
| Bronze source | `BRONZE_TABLE` | ACZ bronze Delta table containing OSDU records. Default is `osducatalog`. Inactive rows are excluded by default using the bronze `isActive` column. |
| ADME schema service | `ADME_ENDPOINT`, `ADME_DATA_PARTITION_ID` | Required for schema lookup. Use the customer ADME endpoint and OSDU data partition id. |
| Authentication | `ADME_AUTH_METHOD`, `ADME_TENANT_ID`, `ADME_SP_CLIENT_ID`, `ADME_SP_SECRET_KV_NAME`, `ADME_SP_SECRET_NAME`, `ADME_MANAGED_IDENTITY_CLIENT_ID` | Use `MI` for production orchestration with the runner's managed identity. Use `SP` for direct notebook runs with a Key Vault stored client secret. Use `DC` only for interactive validation. |
| Scope | `KINDS`, `EXCLUDED_KINDS`, `LIMIT`, `KIND_LIMITS` | Start with one or two explicit kinds and a small limit. Widen to wildcard or all-kinds selections after the dry run is clean, and use exclusions for kind families that should never be processed. |
| Output shape | `OUTPUT_MODE` | Use `normalized` for parent and child tables. Use `wide` when consumers need one denormalized table per kind. |
| Relationship bridges | `WRITE_RELATIONSHIP_BRIDGES`, `ADME_WRITE_RELATIONSHIP_BRIDGES` | Keep `True` to resolve and write schema-declared scalar relationship bridges. Set `False` to skip bridge planning, resolution, and writes; raw relationship references remain on parent rows and existing bridge tables are left untouched. Rebuild bridges with an authorized full refresh before relying on them after disabled upsert runs. This setting works independently of `OUTPUT_MODE` and `VERSION_STRATEGY`. |
| Table safety | `TABLE_PREFIX`, `ALLOW_OVERWRITE` | Use a test prefix for onboarding. Keep overwrite disabled until the planned tables have been reviewed. |
| Run stage | `RUN_PROFILE` | Use `inspect` to print effective settings and next steps, `dry_run` to validate and preview writes, and `execute` to run the configured pipeline. |

Recommended first-run shape:

```python
WORKSPACE_ID = ""              # leave blank when a lakehouse is attached
LAKEHOUSE_ID = ""              # leave blank when a lakehouse is attached
BRONZE_TABLE = "osducatalog"

ADME_ENDPOINT = "https://contoso.energy.azure.com"
ADME_DATA_PARTITION_ID = "opendes"
ADME_AUTH_METHOD = "SP"
ADME_TENANT_ID = "<tenant-id>"
ADME_SP_CLIENT_ID = "<application-client-id>"
ADME_SP_SECRET_KV_NAME = "<key-vault-name-or-url>"
ADME_SP_SECRET_NAME = "<secret-name>"
ADME_MANAGED_IDENTITY_CLIENT_ID = ""  # optional; set only for user-assigned MI

RUN_PROFILE = "inspect"
KINDS = ["osdu:wks:work-product-component--WellLog:1.4.0"]
EXCLUDED_KINDS = []
LIMIT = 10
KIND_LIMITS = {}
OUTPUT_MODE = "normalized"
WRITE_RELATIONSHIP_BRIDGES = True
TABLE_PREFIX = "test_"
ALLOW_OVERWRITE = False
```

Environment variables can override the same settings for scheduled execution. Use environment overrides for automation; edit the notebook values for first-time interactive onboarding.

By default, Silver Layer processing transforms only bronze rows where `isActive == true`; rows where `isActive` is `false` or `null` are excluded. Set the advanced `INCLUDE_INACTIVE_RECORDS = True` control, or `ADME_INCLUDE_INACTIVE_RECORDS=true`, only when inactive records should also be transformed.

## When to use advanced settings

Leave advanced settings at their defaults unless one of these needs applies.

| Need | Settings | Default guidance |
| --- | --- | --- |
| Include inactive bronze records | `INCLUDE_INACTIVE_RECORDS` | Keep `False` so only `isActive == true` records are transformed. Set `True` only when inactive records should be included. |
| Scheduled incremental refresh | `WRITE_MODE`, `MERGE_KEY_COLUMNS`, `INCREMENTAL_WATERMARK_COLUMN`, `INCREMENTAL_WATERMARK_MODE`, `INCREMENTAL_STATE_TABLE` | Start with `WRITE_MODE = "full_refresh"`. For ACZ `osducatalog`, `ingestTime` is the default source-change watermark for `WRITE_MODE = "upsert"`. |
| Multiple schema versions | `VERSION_STRATEGY` | Keep `versioned_tables` for physical separation by schema version. Use `merge` only when consumers want one logical table across versions. |
| Missing private schemas | `MISSING_SCHEMA_MODE` | Keep `skip` for schema-correct outputs that continue past unresolved kinds. Use `infer` only when best-effort output is preferred for unresolved private schemas. Use `fail` when missing schemas should stop the run. |
| Stable child-table contracts | `CREATE_EMPTY_CHILD_TABLES` | Keep enabled when downstream consumers expect schema-defined child tables even when the current batch has no rows. |
| Geometry columns | `DROP_WKT` | Leave disabled unless downstream consumers want WKT geometry columns removed. |
| Schema repeatability | `PERSIST_SCHEMA_CACHE`, `SCHEMA_CACHE_TABLE` | Keep enabled so full runs persist resolved schemas and later runs can tolerate transient schema-service misses. |
| Metadata, quality, and documentation | `RUN_MANIFEST_TABLE`, `RUN_STATUS_TABLE`, `DATA_QUALITY_CHECKS`, `DATA_QUALITY_ISSUES_TABLE`, `DATA_QUALITY_MAX_EXAMPLES`, `WRITE_OUTPUT_DOCS`, `OUTPUT_DOCS_MODE`, `OUTPUT_DOCS_TABLE` | Keep manifest, run status, and data-quality checks enabled. Use `OUTPUT_DOCS_MODE = "summary"` for broad runs and `full` only when column-level generated documentation is required. |
| Large wildcard runs | `CACHE_BRONZE`, `PREFLIGHT_KIND_COUNTS`, `BATCH_METADATA_WRITES`, `METADATA_FLUSH_INTERVAL`, `SCHEMA_PREFLIGHT`, `SCHEMA_FETCH_PARALLELISM`, `OUTPUT_WRITE_PARALLELISM`, `WIDE_MAX_CARDINALITY_CAP` | Keep defaults for broad runs to reduce repeated bronze scans, parallelize bounded schema lookups, batch metadata/data-quality commits, limit small Delta commits, and cap wide-mode array expansion. `OUTPUT_WRITE_PARALLELISM = "auto"` uses the Fabric workspace SKU when available and falls back to `1`; it bounds concurrent parent and child writes and full-refresh relationship-bridge writes. Incremental bridge reconciliation remains sequential. Set an integer only after validating that the Fabric Spark pool can run concurrent independent Delta writes. Limited runs using `LIMIT` or `KIND_LIMITS` automatically bypass bronze caching and count preflight so small samples do not materialize the full bronze slice. |
| Transient ADME schema failures | `ADME_SCHEMA_TIMEOUT_SECONDS`, `ADME_SCHEMA_RETRY_TOTAL`, `ADME_SCHEMA_RETRY_BACKOFF_SECONDS`, `ADME_SCHEMA_RETRY_STATUS_CODES` | Keep retries enabled. Tune only for scheduled runs that process many kinds or encounter throttling. |

`ADME_TOKEN_SCOPE = "https://management.core.windows.net/.default"` and `ADME_DEVICE_CODE_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"` are static authentication constants in the notebook, not customer-specific tenant settings.

`ADME_INCREMENTAL` and `ADME_REASSEMBLE` are still accepted as compatibility aliases for older scheduled runs. Prefer `ADME_WRITE_MODE` and `ADME_OUTPUT_MODE` for new runs.

### Migrating existing automation

Use `inspect`, `dry_run`, or `execute` for `RUN_PROFILE` and `ADME_RUN_PROFILE`; replace legacy `interactive` with `inspect` and `full` with `execute`. Set `WRITE_MODE` explicitly: `execute` does not select full refresh. Update sample-file references to `samples/config/inspect_sp.json` and `samples/config/scheduled_execute_mi.json`.

## Selecting kinds

`KINDS` can include explicit OSDU kind URNs or bronze-driven wildcard selectors. Wildcards are expanded from distinct `kind` values in the configured bronze table:

```python
KINDS = ["osdu:wks:work-product-component--WellLog:1.4.0"]
KINDS = ["*:*:*:*"]                                # all kinds in bronze
KINDS = ["all"]                                    # all kinds in bronze
KINDS = ["osdu:wks:*:*"]                           # all wks kinds in bronze
KINDS = ["*:wks:work-product-component--Well*:1.*"]  # matching Well* WPC kinds
```

Use `EXCLUDED_KINDS` to remove exact kinds or wildcard matches after `KINDS` is expanded. For scheduled runs, set `ADME_EXCLUDED_KINDS` to the same comma-separated selector format:

```python
KINDS = ["*:*:*:*"]
EXCLUDED_KINDS = ["osdu:wks:reference*:*"]
```

All-kinds and wildcard runs can create many tables. Use the Setup checklist, `RUN_PROFILE = "dry_run"`, `TABLE_PREFIX`, `LIMIT`, `KIND_LIMITS`, and `ALLOW_OVERWRITE` intentionally before running a large wildcard selection.

## Choose an output mode

| Mode | Set | Creates | Use when |
| --- | --- | --- | --- |
| Normalized | `OUTPUT_MODE = "normalized"` | Parent table plus child tables for arrays. | You want a relational Silver Layer model with separate tables for repeated fields. |
| Wide | `OUTPUT_MODE = "wide"` | One wide table per kind. | You need a single table per OSDU kind for BI, export, simplified SQL, or tools that prefer denormalized data. |

Normalized output preserves repeated structures as related tables. Wide output expands struct arrays into indexed columns, pivots tags, and concatenates primitive arrays.

## Handle multiple schema versions

The default `VERSION_STRATEGY = "versioned_tables"` keeps schema versions physically separate. Versioned mode writes tables such as `osdu_wks_organisation__v1_2_0`.

Use `VERSION_STRATEGY = "merge"` when consumers prefer one logical table across schema versions. Merge mode groups concrete kinds by authority, source, and entity, then unions schema versions into one table with nullable columns for fields that only exist in some versions. `schema_version` and `osdu_kind` metadata columns preserve the source version.

When schemas cannot be resolved from the configured ADME schema service, `MISSING_SCHEMA_MODE = "skip"` records a schema-missing result and continues processing later kinds. Set `MISSING_SCHEMA_MODE = "infer"` only when best-effort output is acceptable: the notebook infers a temporary schema from the selected bronze payload, writes `schema_mode = "inferred"` in the manifest, and continues without claiming schema-certified output. Set `MISSING_SCHEMA_MODE = "fail"` when unresolved schemas should stop the run.

The notebook does not fall back to the public OSDU data-definitions repository.

Schema parsing supports common OSDU and private-schema variants, including `definitions`, `$defs`, local references through either form, nullable type arrays such as `["null", "string"]`, and nullable `anyOf`/`oneOf` branches.

## Authentication guidance

For production scheduling, set `ADME_AUTH_METHOD = "MI"` and use a managed identity through a supported Fabric pipeline notebook activity connection, workspace identity, or equivalent orchestrator. Leave `ADME_MANAGED_IDENTITY_CLIENT_ID` blank for the system-assigned identity; set it only when the runner should use a specific user-assigned managed identity. Grant that identity only the ADME/OSDU entitlement groups and data-plane permissions required to read schemas and source data.

For local runs, set `ADME_AUTH_METHOD = "CLI"` to reuse an existing Azure CLI login (`az login`; honors `AZURE_CONFIG_DIR`). `ADME_TENANT_ID` is not required.

Direct notebook execution keeps `ADME_AUTH_METHOD = "SP"` as the default because managed identity token acquisition is not assumed inside every interactive Fabric notebook runtime. Store the service principal secret in Key Vault, rotate it regularly, and grant the notebook only secret read access. Use `ADME_AUTH_METHOD = "DC"` only for interactive validation.

Device-code authentication reuses its signed-in account and MSAL token cache within the current notebook session, including when helper cells are rerun. Valid access tokens are reused; renewal first attempts silent acquisition or refresh. A device-code prompt is shown only when no usable session exists or Microsoft Entra ID requires interaction. Authentication caches are isolated by method, tenant/authority, client or managed identity, and token scope; tokens are kept in memory, not written to lakehouse files. Restarting the notebook session clears the cache and requires a fresh sign-in.

## Scalar relationship bridges

For scalar top-level `data` fields declared with `x-osdu-relationship`, the parent retains its raw reference field and does not add duplicate `<field>__fk_id` or `<field>__fk_version` columns. With `WRITE_RELATIONSHIP_BRIDGES = True`, the pipeline writes one bridge table per source kind family, relationship path and declared target type, so consumers do not need to filter a shared bridge table to isolate a relationship.

With `WRITE_RELATIONSHIP_BRIDGES = False` (or `ADME_WRITE_RELATIONSHIP_BRIDGES=false`), bridge planning, resolution, and writes are skipped while raw references remain on parent rows; existing bridge tables are not deleted or refreshed. A later upsert cannot reconcile changes made while bridge writes were disabled, so run an authorized full refresh before relying on re-enabled bridges.

Bridge output is independent of `OUTPUT_MODE` and supports both `VERSION_STRATEGY` values. With `VERSION_STRATEGY = "merge"`, all schema versions in a family share that bridge table; for example, the `1.0.0` Well relationship shown as `relationship__osdu_wks_master_data_well_1_0_0__data_existencekind__reference_data_existencekind` in `versioned_tables` is named `relationship__osdu_wks_master_data_well__data_existencekind__reference_data_existencekind` in `merge`. With `versioned_tables`, the bridge name includes the source schema version, matching its versioned parent table.

Each resolved link has source and target identities, source table, property path, raw reference, target active state, status, and run id. This includes scalar 1:1 and 1:N relationships; consumers can use rows with `status = "resolved"` as Fabric ontology mapping data. Explicit versions resolve exactly and unversioned references resolve to the latest numeric record version for the declared target type, including inactive latest records. References without a matching target are retained in the parent raw field but do not become bridge rows. Array-valued relationships and relationships nested within array elements remain out of scope.

Resolution reflects the target snapshot when a referring source row is processed. Incremental target-only updates or tombstones do not revisit unchanged referring sources. Run an authorized full refresh of affected referring kinds when those target changes must be reflected immediately. A matched target with unknown/null `isActive` can have `status = "resolved"`; consumers requiring confirmed-active targets must also filter `target_is_active = true`.

## Upsert mode, watermarks, and merge keys

The default `MERGE_KEY_COLUMNS = ["id", "version"]` treats the bronze `version` column as the OSDU record version. This allows multiple versions of the same record `id` to coexist in Silver Layer tables and makes repeated upsert runs idempotent for the same `id + version` pair.

Do not confuse the bronze record `version` column with `schema_version`, which is derived from the kind URN. In `WRITE_MODE = "upsert"`, parent or wide rows are merged by `MERGE_KEY_COLUMNS`, and child rows are replaced using the same key columns.

With bridge upserts enabled, merge keys must be drawn from `id`, `version`, and `kind`. Bridge replacement maps those effective keys to their `source_` columns, including ID-only parent replacement. Payload-derived merge keys require bridges to be disabled. Watermark advancement is deferred until bridge publication succeeds so a bridge-write failure remains retryable; this is not a transaction across tables, and failed runs can still leave partial outputs.

By default, `INCREMENTAL_WATERMARK_COLUMN = "ingestTime"` uses the ACZ bronze update timestamp to prune incremental upsert runs to affected concrete kinds before schema preflight and group processing. Active changed rows are transformed and upserted; rows explicitly marked `isActive = false` hard-delete matching Silver parent/wide and child rows by `MERGE_KEY_COLUMNS` so the default Silver outputs remain active-only. If `INCLUDE_INACTIVE_RECORDS = True`, inactive rows are included in the transformed Silver outputs instead of being hard-deleted. Use `INCREMENTAL_WATERMARK_MODE = "required"` when a scheduled job must fail rather than process all selected rows if the watermark column is missing.

When watermark filtering is active, do not set `LIMIT` or `KIND_LIMITS`; the notebook rejects that combination because advancing a persistent watermark after a limited batch can skip unprocessed records. Watermark filtering intentionally includes rows at the previous maximum watermark value so late-arriving records with the same watermark are reprocessed safely through idempotent upserts and deletes.

## Run the pipeline

The notebook is organized into these executable sections:

| Section | Purpose |
| --- | --- |
| Spark runtime configuration | Reuses the Fabric Spark session or creates one outside Fabric, then applies Spark and Delta settings. |
| Configuration | Resolves customer settings, environment overrides, workspace, lakehouse, bronze table, ADME schema service settings, and selected kinds. |
| Pipeline constants | Defines service endpoints, storage scopes, run metadata schema, and result types. |
| Helper functions | Provides Fabric, OneLake, Delta write, upsert, and bronze-read helpers. |
| Core decomposition and reassembly logic | Resolves schemas, identifies column shapes, decomposes records, builds child tables, and reassembles wide outputs. |
| Pipeline functions | Processes one or more kinds and records run metadata. |
| Setup checklist | Validates tenant configuration, bronze access, ADME schema access, output table names, and planned output tables without writing Silver tables. |
| Smoke test bronze access | Reads at most one row for the first configured kind without writing Silver tables. |
| Run pipeline | Prints settings and next steps when `inspect`, previews writes when `dry_run`, or executes when `execute`. |
| Results summary | Displays per-kind status, separate child/bridge counts, and a deduplicated bridge inventory without additional data scans. |

## Development layout

### Packaged schema-driven full rebuild

The optional `schema_contract.py`, `silver.py`, and `silver_publish.py` modules provide a separate full-rebuild API for application developers. They are not called by the customer notebook and do not change its active-only, upsert, or bridge contracts. There is no additional reference notebook to import. Callers provide exact schema documents and a Spark session, build candidate outputs, and explicitly invoke publication into a separate destination.

The calling application supplies `spark`, `source_path`, `schema_directory`, `table_root`, and `journal_root`. Exported schema filenames replace kind colons with underscores. Use a separate output root and a fresh run ID:

```python
from uuid import uuid4

from adme_acz_silverlayer.schema_contract import load_schema_directory
from adme_acz_silverlayer.silver import build_silver, release_silver
from adme_acz_silverlayer.silver_publish import publish_silver, read_pinned_source

source, snapshot = read_pinned_source(spark, source_path)
kinds = [row.kind for row in source.select("kind").distinct().collect()]
schemas = load_schema_directory(schema_directory, kinds)
candidate = build_silver(source, schemas, run_id=uuid4().hex)
try:
    manifest = publish_silver(candidate, snapshot, table_root, journal_root)
finally:
    release_silver(candidate)
```

| Contract | Packaged behavior |
| --- | --- |
| Snapshot | Pin one source Delta version before discovering kinds or reading records. |
| Root grain | Preserve all source rows and columns, including `isActive`; identity is `(id, version)`. |
| Latest | `_silver_is_latest` compares exact decimal versions across schema-version tables for each full ID. It is independent of deletion state. |
| Projections | Resolve local schema references and compositions; add declared typed fields. Mixed-type alternatives and map-like objects retain JSON text; arrays inside opaque JSON do not create further child tables. |
| Arrays | Generate children with full ancestry ordinals and explicit null-element state; duplicate/null occurrences are retained. Typed arrays are not repeated on normalized parents; the raw payload retains them for replay. |
| Relationships | Use only `x-osdu-relationship`, declared patterns and target types. Explicit versions resolve exactly; unversioned references select latest available targets. |
| Diagnostics | Preserve raw references and distinguish resolved, deleted, unavailable, missing-version, invalid and absent targets. Local measurement pointers are not inferred as record references. |
| Refresh | Full rebuild of the available source snapshot. It cannot reconstruct versions never exported by ACZ. |
| Publication | Create new run-specific Delta tables, record commits, verify pinned read-backs and expose one successful run manifest. Existing outputs are never overwritten. |

The source requires string `id`/`kind`, JSON-string `data`, Boolean `isActive`, and string or exact integer `version`. Payloads may be entity data or Storage envelopes with matching identity; wrapper metadata is projected where available, while raw source columns stay unchanged. Native ACZ timestamps are represented as UTC ISO strings in schema projections without changing the original timestamp columns. Schema integers use Spark's signed 64-bit type; record versions retain their original exact representation. Empty snapshots, ambiguous numeric versions/payload wrappers, missing exact schemas, invalid declared value types, malformed JSON, external/cyclic schema references, tuple arrays, references inside opaque `additionalProperties` maps, and table/case-insensitive field collisions stop the run before publication. This representation contract is **not full JSON Schema validation**: compatible alternatives combine fields/relationship targets, while required fields, branch assertions, enums, bounds, formats and conditional schemas are not enforced. Preserved ACL/legal metadata does not enforce ADME entitlements in Fabric; govern destination access separately.

Outputs use the existing kind/child naming helpers and have a default `gen_silver_` prefix. Publication appends `__run_<run_id>` to each physical table folder. The relationship bridge stores logical table keys; the run manifest maps those keys to physical paths. Consumers must select all outputs and `versionAsOf` values from **one** successful run manifest under the caller's manifest root, not from mixed table-name tips. Failed runs retain their partial inventory and propagate the error. Multi-table publication is manifest-gated, not a Delta transaction; there is no automatic rollback. Keep the previous successful manifest for recovery, rebuild with a new ID, and retain pinned Delta versions/files while consumers need them. SQL endpoint discovery is asynchronous and is not part of the publish guarantee.

`test_schema_contract.py` covers pure projection/reference rules; `test_silver.py` exercises synthetic Spark transformations and publication failures. Optional `test_delta_silver.py` exercises real local Delta, including pinned input after a later commit and immutable output publication:

```shell
python -m pip install -e ".[delta]"
python -m unittest discover -s tests -p test_delta_silver.py -q
```

The optional Delta package must match the installed Spark/Python/Java runtime; its launcher may fetch matching Maven artifacts on first use. Fabric supplies Delta itself and does not require that package for the self-contained notebook. Before scheduling a packaged rebuild, preview the source, review diagnostics, publish to a separate destination, and smoke-test the successful manifest's pinned outputs and filesystem permissions. Source/manifest retention and obsolete-run cleanup are explicit operational responsibilities.

### Self-contained notebook helpers

The customer-facing artifact remains `ADME ACZ Silver Layer.ipynb`. Customer runs in Microsoft Fabric should not need any helper `.py` files deployed beside the notebook.

Reusable helpers live under `src/adme_acz_silverlayer/` so configuration parsing, naming, JSON Schema compatibility behavior, Fabric/OneLake boundary behavior, ADME schema URL/auth helpers, bronze filter decisions, and notebook hygiene can be tested directly outside Fabric. The optional Spark modules provide schema-to-Spark conversion and normalization primitives. The committed notebook stays self-contained for import into Fabric and does not require installing this package.

| Spark module | Responsibility |
| --- | --- |
| `spark_schema.py` | OSDU envelope/data schema parsing, JSON Schema to Spark type conversion, and case-insensitive nested type merging. These helpers require PySpark types, but not a running Spark session. |
| `normalization.py` | Safe column aliases, recursive struct flattening, and typed-array expansion with explicit record keys and occurrence ordinals. JSON and native-array notebook builders share the same array expansion helper. |

These modules preserve the notebook's existing compatibility behavior. They do not provide a new synchronization pipeline or change active-record filtering, merge keys, version retention, schema-version grouping, or write modes. The schema converter retains string fallbacks for unknown types/references and first-supported-branch selection for alternatives; it is not a complete JSON Schema validator. Registry lookup, JSON inference, higher-level decomposition/reassembly, and publication orchestration remain in the notebook.

Use the notebook sync command before committing notebook changes:

```powershell
python scripts/sync_notebook.py --check --summary
python scripts/sync_notebook.py
python -m unittest discover -s tests -q
```

`--check` validates the notebook format, required section order, absence of code-cell outputs, the self-contained contract, and synchronization of the shared Spark helper definitions. Running without `--check` embeds those definitions from the package source and removes execution artifacts. Edit the shared functions in `spark_schema.py` or `normalization.py`, then run the sync command; do not maintain separate copies manually. The `SHARED_SPARK_HELPERS` inventory in `notebook_sync.py` declares the synchronized functions and their type/naming constants. Synchronization reads Python source without importing PySpark or executing notebook cells.

The group-concurrency experiment is generated from the main notebook. Its generator changes trial settings and group dispatch while reusing the production transformation, authentication, publication and summary code. After changing the main notebook, regenerate and check the experiment:

```shell
python scripts/sync_parallelism_experiment.py
python scripts/sync_parallelism_experiment.py --check
```

### Notebook integration tests

`tests/test_notebook_integration.py` executes every code cell of the committed notebook on local Spark and Delta (`tests/notebook_runner.py` overrides the customer settings). `OfflineNotebookRunTests` use synthetic bronze rows and a stubbed schema service and token, and verify active-record filtering, child tables, relationship bridges and run metadata. `tests/test_notebook_tno_integration.py` runs the notebook on real Spark and Delta with public OSDU TNO records (`tests/tno_bronze.py`, pinned to a commit of the `osdu/platform/data-flow/data-loading/open-test-data` project, Well and Wellbore master data) against a fake ADME schema service. `tests/fake_adme.py` is a local HTTPS server (self-signed certificate, OS-assigned port) that serves the public OSDU `data-definitions` schemas, bundled with their abstract schemas inlined, and checks the bearer token and `data-partition-id`. Local Spark sessions use their default port allocation. No tenant data, Azure identity or live ADME instance is involved; the first run needs network access to `community.opengroup.org` (downloads are cached in the system temporary directory). `ADME_ACZ_TNO_RECORDS` (default 10) sets the number of wells and wellbores.

`LiveNotebookRunTests` are opt-in and call a real ADME schema service with `ADME_AUTH_METHOD = "CLI"`, comparing published row counts with the active bronze records. They regenerate bronze from the same TNO data, or read a local bronze Delta copy from `ADME_ACZ_LIVE_BRONZE_PATH` (tenant data; never commit it). `ADME_ACZ_LIVE_TNO_RECORDS` (default 25) sets the record count.

```shell
az login
ADME_ACZ_LIVE_ENDPOINT="https://<instance>.energy.azure.com" ADME_ACZ_LIVE_PARTITION="<partition>" \
  python -m unittest discover -s tests -p test_notebook_integration.py -q
python tests/tno_bronze.py --output /tmp/tno-bronze --wells 5 --wellbores 5   # write the bronze table only
```

From a clean checkout (Python 3.13 and a Java 21 runtime on `PATH` or `JAVA_HOME`; the first run downloads the Delta jar from Maven Central):

```shell
python -m venv .venv && source .venv/bin/activate
python -m pip install -e ".[integration]"
python -m unittest discover -s tests -p test_notebook_integration.py -q
```

The offline and fake-ADME tests need nothing else. For the live tests, run `az login` first (set `AZURE_CONFIG_DIR` to use a non-default profile) and export the two `ADME_ACZ_LIVE_*` variables above. A local bronze copy exported from a tenant contains tenant data and must not be committed.

`.github/workflows/tests.yml` runs the unit, offline and fake-ADME integration tests on pull requests and pushes to `main`. It does not call a live ADME instance; the live tests are skipped there and are run manually.

The `integration` extra constrains PySpark to `>=4.1,<4.2` and delta-spark to `>=4.2,<4.3`. Python 3.13 and Java 21 are the tested local/CI runtime; package extras do not install or pin Python or Java. Match the selected Fabric runtime when comparing performance, and use a compatible Spark/Delta release pair rather than independently upgrading either package.

`tests/test_notebook_modes.py` exercises upsert with the incremental watermark and inactive-record deletes, wide output with versioned tables and data-quality issues, schema inference for kinds missing from the schema service, the dry-run profile, output-shape helpers, and the SP, DC, MI and CLI authentication branches. These tests cover selected scenarios, not every input shape, cloud identity setup or Fabric runtime behavior.

`tests/test_parallelism_experiment.py` checks deterministic generation, shared-implementation parity, safe settings, actual schema-preflight success, isolated concurrent metadata buffers, and result ordering. Its optional real Delta scenario runs the generated notebook with group parallelism 1 and 2 and compares output schemas and rows, excluding generated run/time values and prefix-dependent source-table names.

### Optional local Spark tests

The baseline unit command includes the Spark test files. Only tests requiring an unavailable optional dependency or Java runtime are skipped. Install PySpark and a Java runtime compatible with the selected Spark version to run the transformation tests:

```shell
python -m pip install -e ".[spark]"
python -m unittest discover -s tests -q
```

Set `JAVA_HOME` if the runtime is not discoverable through `java`. The Spark test harness uses the current Python executable for workers, so driver and worker minor versions match. An installed but broken Spark runtime fails the tests rather than being silently skipped.

`test_spark_schema.py` exercises type conversion without starting Spark. `test_normalization.py` uses real local Spark with synthetic inputs to verify record-version preservation, duplicate and null array elements, ordinal identity, empty child schemas, escaped field names, and parity between JSON-array and native-array builders. These tests do not call ADME, access Fabric, write Delta tables, or validate the complete ingestion/publication pipeline.

## Onboarding assets

The `samples/` folder contains generic, placeholder-based assets for customer onboarding:

| Asset | Purpose |
| --- | --- |
| `samples/config/inspect_sp.json` | First-run settings inspection using service principal authentication and a small limit. |
| `samples/config/dry_run_validation.json` | Write-free validation profile for expanding kind coverage. |
| `samples/config/scheduled_execute_mi.json` | Production-style scheduled execution using managed identity and upsert mode. |
| `samples/fabric_pipeline_parameters.json` | Generic Fabric pipeline notebook activity parameter values. |
| `samples/synthetic_bronze_records.json` | Tiny synthetic bronze-like records for local shape review or sample table creation. |

Replace every placeholder before running in a customer environment. Do not add tenant ids, secrets, or customer-specific values to committed sample files.

## Review results

After a successful run, review:

- The parent or reassembled table for each selected kind.
- Generated child tables when `OUTPUT_MODE = "normalized"`.
- `silver_run_info` for run status, record counts, failures, schema access details, watermark settings, and stage timings.
- `silver_run_manifest` for produced table names, output mode, write mode, schema versions, row counts, config hash, active-record filter behavior, relationship-bridge enablement, and status.
- `silver_run_status` for run-level publish state. Treat only the latest `committed` status for a `run_id` as a completed multi-table publish; `started` means the run began writing, and `failed` means outputs may be partial.
- `silver_data_quality_issues` for capped examples of non-blocking quality findings such as missing/null/duplicate merge keys, malformed JSON-looking values, and columns not present in the resolved schema. `quality_status` and `quality_issue_count` are also written to run metadata.
- `silver_schema_cache` when persisted schema caching is enabled.
- `silver_incremental_state` when watermark-based source filtering is enabled.
- `silver_output_documentation` for generated table and column documentation.
- The Results summary cell for per-kind status, rows, parent table, separate array-child and relationship-bridge counts, validation, and errors. Its bridge inventory deduplicates shared table names and lists source kinds, parents, and processing statuses. It uses existing results without scanning Delta: it does not report bridge row counts or independently certify publication.

If a kind has no matching bronze records, the run returns `skipped` for that kind.

## Monitoring examples

```sql
SELECT status, COUNT(*) AS kinds, SUM(records_processed) AS rows
FROM silver_run_info
GROUP BY status;

SELECT kind, status, error_type, error_message
FROM silver_run_info
WHERE status <> 'success';

SELECT kind, parent_table, child_table_count, write_mode, watermark_column, watermark_mode
FROM silver_run_manifest
WHERE run_id = '<run-id>';

SELECT run_id, status, status_time, table_names, error_message
FROM silver_run_status
WHERE run_id = '<run-id>'
ORDER BY status_time DESC;

SELECT kind, check_name, severity, COUNT(*) AS examples
FROM silver_data_quality_issues
WHERE run_id = '<run-id>'
GROUP BY kind, check_name, severity;
```

Review high `duration_seconds` values and `stage_timings_json` to identify expensive phases such as bronze scans, schema preflight, group processing, metadata flush, or output documentation flush.

## Sizing, cost, and performance guidance

For smoke tests and first tenant validation, use `RUN_PROFILE = "dry_run"`, a small `LIMIT`, narrow `KINDS`, a test `TABLE_PREFIX`, and `OUTPUT_DOCS_MODE = "summary"`.

For medium onboarding runs, process one domain or entity family at a time with `KIND_LIMITS`, keep broad-run performance defaults enabled, and review `silver_run_manifest` before widening selection.

For large wildcard or all-kinds runs, expect cost and runtime to be driven by bronze scans, schema lookups, array-heavy child table expansion, wide-table column growth, Delta commits, and output documentation volume. Prefer normalized output for array-heavy entities unless downstream consumers require a wide table.

Schema preflight fetches unresolved schemas with bounded parallelism (`SCHEMA_FETCH_PARALLELISM`, default `4`) and reuses retry-enabled HTTP sessions. Increase it only when the ADME schema service and network can tolerate more concurrent requests; reduce it if throttling occurs.

Relationship resolution expands all declared scalar references into a narrow row stream and joins it to eligible Bronze identities once per source kind. Paths sharing a target-type set share the latest-version ranking; overlapping target-type sets retain separate eligibility rules. The pipeline materializes the resolved bridge rows once before publishing individual relationship tables. This keeps the number of joins independent of the number of relationship fields without changing physical bridge-table names or reference semantics.

The final timing summary separates `relationship_materialize` (executing and caching the relationship plans) from `relationship_write` (publishing bridge tables and preparing their documentation). Compare runs with identical kinds, output mode, write parallelism, Spark resources, and storage. Input row count alone does not describe the workload: each parent, child, and bridge table requires a separate Delta commit, including schema-required empty tables.

Wide output expands struct-array children up to `WIDE_MAX_CARDINALITY_CAP` positions per parent record. Keep the default unless consumers explicitly need more repeated elements in a single wide table; normalized output is safer for high-cardinality arrays.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Bronze table not found | Confirm the lakehouse is attached, or set `WORKSPACE_ID`, `LAKEHOUSE_ID`, and `BRONZE_TABLE`. |
| Active-record filter fails | Confirm the bronze table has an `isActive` boolean column, or set `INCLUDE_INACTIVE_RECORDS = True` only if inactive records should be transformed. |
| No records processed | Confirm the selected `KINDS` values match active `kind` values in the bronze table. If only inactive rows exist for a kind, set `INCLUDE_INACTIVE_RECORDS = True` only when those rows should be transformed. |
| ADME schema access fails | Confirm `ADME_ENDPOINT`, `ADME_DATA_PARTITION_ID`, and `ADME_AUTH_METHOD` are set. For `MI`, confirm the runner exposes a managed identity and `ADME_MANAGED_IDENTITY_CLIENT_ID` is set only when using a user-assigned identity. For `SP`, confirm `ADME_TENANT_ID`, `ADME_SP_CLIENT_ID`, `ADME_SP_SECRET_KV_NAME`, and `ADME_SP_SECRET_NAME` are set and the notebook can read the Key Vault secret. |
| Schema load fails | Confirm the runtime can reach the ADME endpoint, MSAL can get a token, and the kind URN exists in the ADME schema service. Review retry settings and schema access details in `silver_run_info`. |
| Schema calls are throttled | Increase retry total or backoff, reduce the number of selected kinds, or keep persisted schema cache enabled. |
| Schema preflight is too slow or throttled | Tune `SCHEMA_FETCH_PARALLELISM`. Lower it for throttling or constrained networks; raise it cautiously for broad runs with many schemas. |
| Private/custom schemas are skipped | Confirm the schema is registered in the ADME data partition and the configured identity is authorized to read it, or use `MISSING_SCHEMA_MODE = "infer"` for best-effort output marked as `schema_mode = "inferred"` in `silver_run_manifest`. |
| Multiple versions collide | Keep `VERSION_STRATEGY = "versioned_tables"` for physical separation, or use `merge` only when one table across versions is intended. |
| Setup checklist fails | Fix the failed checklist item before running with `RUN_PROFILE = "execute"`. |
| Full refresh is blocked by existing tables | Review the listed tables and set `ALLOW_OVERWRITE = True` only if replacing them is intended. |
| Upsert merge fails | Fix the Delta merge error and rerun. The notebook does not fall back to overwrite when an existing Delta target fails to merge. |
| Watermark filtering is not active | Confirm `WRITE_MODE = "upsert"`, verify `ingestTime` exists in bronze or override `INCREMENTAL_WATERMARK_COLUMN`, and use `INCREMENTAL_WATERMARK_MODE = "required"` if fallback processing should fail. |
| Watermark upsert refuses to run with limits | Remove `LIMIT` and `KIND_LIMITS`, or set `INCREMENTAL_WATERMARK_MODE = "off"` for bounded test runs that should not advance watermark state. |
| Outputs are not marked committed | Check `silver_run_status` for the latest row for the run. If the final status is `failed` or no `committed` row exists, treat output tables from that run as partial and review `silver_run_info` for the failing kind or metadata write. |
| Data-quality issues are reported | Review `silver_data_quality_issues` and the `quality_status` / `quality_issue_count` fields in run metadata. Findings are non-blocking examples capped by `DATA_QUALITY_MAX_EXAMPLES`; fix source data or merge-key configuration when severity is `error`. |
| Output table shape is too normalized | Use `OUTPUT_MODE = "wide"` to create one wide table per selected kind. |
| Wide output has too many or too few repeated columns | Tune `WIDE_MAX_CARDINALITY_CAP`, or switch to normalized output for array-heavy entities. |
| Output tables are overwritten unexpectedly | Check `RUN_PROFILE`, `WRITE_MODE`, environment overrides, and `ALLOW_OVERWRITE` before running with `RUN_PROFILE = "execute"`. |

## Security

Do not report security vulnerabilities through public GitHub issues. For security reporting guidance, see [SECURITY.md](SECURITY.md).

## License

This project is licensed under the [MIT License](LICENSE.md).
