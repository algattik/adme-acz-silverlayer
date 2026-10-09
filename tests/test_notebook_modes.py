"""Notebook runs on local Spark/Delta for the modes the default run does not reach.

Covers upsert with the incremental watermark and inactive-record deletes, wide output with versioned tables and
data-quality issues, schema inference for kinds missing from the schema service, the dry-run profile, and the
authentication and Fabric-context helpers. Bronze rows are synthetic and the schema service is stubbed.
"""

import json
import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

import requests

from notebook_runner import run_notebook
from test_notebook_integration import WELL_KIND, WELL_SCHEMA, NotebookIntegrationBase
from tno_bronze import BRONZE_DDL

WELLBORE_KIND = "osdu:wks:master-data--Wellbore:1.0.0"
EARLIER = datetime(2026, 1, 1, tzinfo=timezone.utc)
LATER = datetime(2026, 2, 1, tzinfo=timezone.utc)
LATEST = datetime(2026, 3, 1, tzinfo=timezone.utc)


def row(record_id, version, payload, kind=WELL_KIND, active=True, ingest=EARLIER):
    envelope = json.dumps({"data": payload, "meta": None, "modifyUser": "buildagent", "modifyTime": 1760000000000})
    return (envelope, None, record_id, version, kind, None, None, None, "buildagent", None, None, None,
            ingest, None, None, None, active)


def write_bronze(spark, rows, mode="overwrite"):
    writer = spark.createDataFrame(rows, BRONZE_DDL).write.format("delta").mode(mode)
    writer.saveAsTable("osducatalog")


def schema_stub(missing_kinds=()):
    def serve(url, context, timeout=None, session=None):
        if "latestVersion" in url:
            return []
        if any(kind in url for kind in missing_kinds):
            response = requests.Response()
            response.status_code = 404
            raise requests.HTTPError(f"HTTP 404 for {context}", response=response)
        return {"schema": WELL_SCHEMA}
    return serve


def run(spark, settings, missing_kinds=(), capture=None):
    def stub_services(namespace):
        if capture is not None:
            capture["get_adme_access_token"] = namespace["get_adme_access_token"]
        namespace["get_adme_access_token"] = lambda: "unit-test-token"
        namespace["_adme_schema_get_json"] = schema_stub(missing_kinds)
        namespace["_table_path_uri"] = lambda table: f"file:///nonexistent-delta-path/{table}"

    return run_notebook(spark, {
        "ADME_ENDPOINT": "https://adme.example.test",
        "ADME_DATA_PARTITION_ID": "test-partition",
        "ADME_AUTH_METHOD": "CLI",
        "ALLOW_OVERWRITE": True,
        **settings,
    }, before_pipeline=stub_services)


def keys(frame):
    return {(r["id"], r["version"]) for r in frame.select("id", "version").distinct().collect()}


class UpsertWatermarkTests(NotebookIntegrationBase):
    SETTINGS = {
        "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "up_",
        "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
    }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [
            row("test:well:1", "1", {"FacilityName": "Alpha", "NameAliases": [{"AliasName": "A1"}]}),
            row("test:well:2", "1", {"FacilityName": "Beta", "NameAliases": [{"AliasName": "B1"}]}),
            row("test:well:3", "1", {"FacilityName": "Gamma", "NameAliases": []}),
        ])
        run(cls.spark, cls.SETTINGS)
        cls.first_keys = keys(cls.spark.table("up_osdu_wks_well"))
        cls.spark.createDataFrame([
            row("test:well:1", "2", {"FacilityName": "Alpha renamed", "NameAliases": [{"AliasName": "A2"}]}, ingest=LATER),
            row("test:well:4", "1", {"FacilityName": "Delta", "NameAliases": []}, ingest=LATER),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        cls.spark.sql(f"UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '{LATER:%Y-%m-%d %H:%M:%S}' "
                      "WHERE id = 'test:well:2'")
        cls.namespace = run(cls.spark, cls.SETTINGS)
        cls.second_keys = keys(cls.read("up_osdu_wks_well"))
        cls.spark.createDataFrame([
            row("test:well:5", "1", {"FacilityName": "Epsilon", "NameAliases": []}, ingest=LATER),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        run(cls.spark, cls.SETTINGS)
        cls.third_keys = keys(cls.read("up_osdu_wks_well"))

    def test_first_run_publishes_every_active_record(self):
        self.assertEqual({("test:well:1", "1"), ("test:well:2", "1"), ("test:well:3", "1")}, self.first_keys)

    def test_second_run_merges_new_versions_and_records(self):
        self.assertEqual(
            {("test:well:1", "1"), ("test:well:1", "2"), ("test:well:3", "1"), ("test:well:4", "1")},
            self.second_keys)

    def test_rows_at_the_watermark_boundary_are_reprocessed(self):
        self.assertIn(("test:well:5", "1"), self.third_keys)

    def test_deactivated_record_is_deleted_from_parent_and_child_tables(self):
        self.assertNotIn("test:well:2", {r["id"] for r in self.read("up_osdu_wks_well").collect()})
        aliases = {r["AliasName"] if "AliasName" in r.asDict() else r["data__AliasName"]
                   for r in self.read("up_osdu_wks_well___namealiases").collect()}
        self.assertNotIn("B1", aliases)
        self.assertIn("A2", aliases)

    def test_watermark_state_records_the_latest_ingest_time(self):
        state = self.read("silver_incremental_state").orderBy("updated_at").collect()
        self.assertEqual(
            ["2026-01-01 00:00:00", "2026-02-01 00:00:00", "2026-02-01 00:00:00"],
            [r["watermark_value"] for r in state],
        )


class WatermarkedRelationshipBridgeTests(NotebookIntegrationBase):
    KIND = "osdu:wks:master-data--Wellbore:1.0.0"
    TARGET_KIND = "osdu:wks:reference-data--FacilityType:1.0.0"
    TARGET_ID = "test:reference-data--FacilityType:well"
    OTHER_TARGET_ID = "test:reference-data--FacilityType:bore"
    SETTINGS = {
        "RUN_PROFILE": "execute", "KINDS": [KIND], "TABLE_PREFIX": "wmbridge_",
        "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
    }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [
            row("test:wellbore:1", "1", {"FacilityTypeID": cls.TARGET_ID + ":"}, kind=cls.KIND),
            row(cls.TARGET_ID, "1", {"FacilityName": "Well"}, kind=cls.TARGET_KIND),
            row(cls.OTHER_TARGET_ID, "1", {"FacilityName": "Bore"}, kind=cls.TARGET_KIND),
        ])
        run(cls.spark, cls.SETTINGS)
        cls.bridge_table = next(
            name for name in (table.name for table in cls.spark.catalog.listTables())
            if name.startswith("wmbridge_relationship__") and name.endswith("facilitytype")
        )
        cls.spark.createDataFrame([
            row("test:wellbore:1", "1", {"FacilityTypeID": cls.OTHER_TARGET_ID + ":"},
                kind=cls.KIND, ingest=LATER),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        run(cls.spark, cls.SETTINGS)
        cls.bridge_after_source_update = cls.spark.table(cls.bridge_table).collect()
        cls.spark.createDataFrame([
            row("test:wellbore:1", "1", {"FacilityTypeID": cls.OTHER_TARGET_ID + ":"},
                kind=cls.KIND, active=False, ingest=LATEST),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        run(cls.spark, cls.SETTINGS)

    def test_watermarked_source_change_replaces_its_bridge_row(self):
        rows = self.bridge_after_source_update
        self.assertEqual(1, len(rows))
        self.assertEqual(
            (self.OTHER_TARGET_ID, self.OTHER_TARGET_ID + ":", "resolved"),
            (rows[0]["target_id"], rows[0]["raw_reference"], rows[0]["status"]),
        )
        self.assertEqual("test:wellbore:1", rows[0]["source_id"])

    def test_watermarked_inactive_source_deletes_its_bridge_row(self):
        self.assertEqual(0, self.read(self.bridge_table).count())


class WatermarkedRelationshipBridgeDisabledTests(NotebookIntegrationBase):
    KIND = "osdu:wks:master-data--Wellbore:2.0.0"
    TARGET_KIND = "osdu:wks:reference-data--FacilityType:1.0.0"
    TARGET_ID = "test:reference-data--FacilityType:disabled"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.settings = {
            "RUN_PROFILE": "execute", "KINDS": [cls.KIND], "TABLE_PREFIX": "wmnobridge_",
            "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
            "WRITE_RELATIONSHIP_BRIDGES": True,
        }
        write_bronze(cls.spark, [
            row("test:wellbore:disabled", "1", {"FacilityTypeID": cls.TARGET_ID + ":"}, kind=cls.KIND),
            row(cls.TARGET_ID, "1", {"FacilityName": "Disabled"}, kind=cls.TARGET_KIND),
        ])
        run(cls.spark, cls.settings)
        cls.bridge_table = next(
            name for name in (table.name for table in cls.spark.catalog.listTables())
            if name.startswith("wmnobridge_relationship__") and name.endswith("facilitytype")
        )
        cls.settings["WRITE_RELATIONSHIP_BRIDGES"] = False
        cls.spark.createDataFrame([
            row("test:wellbore:disabled", "2", {"FacilityTypeID": "test:reference-data--FacilityType:other:"},
                kind=cls.KIND, ingest=LATER),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        run(cls.spark, cls.settings)

    def test_disabling_bridges_keeps_parent_updates_and_does_not_delete_existing_bridge_tables(self):
        parent = self.read("wmnobridge_osdu_wks_wellbore")
        current = parent.where("version = '2'").first()
        self.assertEqual("test:reference-data--FacilityType:other:", current["data__FacilityTypeID"])
        bridge_rows = self.read(self.bridge_table).collect()
        self.assertEqual(1, len(bridge_rows))
        self.assertEqual(self.TARGET_ID + ":", bridge_rows[0]["raw_reference"])


class WideVersionedOutputTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [
            row("test:well:1", "1", {"FacilityName": "Alpha", "Unmodelled": "x",
                                     "NameAliases": [{"AliasName": "A1"}, {"AliasName": "A2"}]}),
            row("test:well:2", "1", {"FacilityName": "Beta", "NameAliases": []}),
            row("test:well:2", "1", {"FacilityName": "Beta", "NameAliases": []}),
        ])
        cls.namespace = run(cls.spark, {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "wide_", "OUTPUT_MODE": "wide",
            "WRITE_MODE": "full_refresh", "VERSION_STRATEGY": "versioned_tables",
        })

    def test_versioned_wide_table_holds_one_row_per_record(self):
        table = self.read("wide_osdu_wks_well__v1_0_0")
        self.assertEqual({"test:well:1", "test:well:2"}, {r["id"] for r in table.collect()})

    def test_child_arrays_are_reassembled_into_the_wide_row(self):
        table = self.read("wide_osdu_wks_well__v1_0_0")
        alias_columns = [c for c in table.columns if "namealiases" in c.lower()]
        self.assertTrue(alias_columns, table.columns)
        first = table.filter("id = 'test:well:1'").collect()[0][alias_columns[0]]
        self.assertEqual(2, len(first))

    def test_data_quality_issue_is_recorded_for_duplicate_merge_key(self):
        issues = self.read("silver_data_quality_issues").filter("check_name = 'duplicate_merge_key'").collect()
        self.assertEqual(["test:well:2"], [r["source_id"] for r in issues])
        self.assertIn("appears 2 times", issues[0]["issue_detail"])


class SchemaInferenceTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [
            row("test:wellbore:1", "1", {"FacilityName": "Bore", "Depth": 12.5}, kind=WELLBORE_KIND),
            row("test:wellbore:2", "1", {"FacilityName": "Bore 2", "Depth": 7.0}, kind=WELLBORE_KIND),
        ])
        base = {"RUN_PROFILE": "execute", "KINDS": [WELLBORE_KIND], "WRITE_MODE": "full_refresh",
                "VERSION_STRATEGY": "merge"}
        cls.inferred = run(cls.spark, {**base, "TABLE_PREFIX": "inf_", "MISSING_SCHEMA_MODE": "infer"},
                           missing_kinds=[WELLBORE_KIND])
        cls.skipped = run(cls.spark, {**base, "TABLE_PREFIX": "skp_", "MISSING_SCHEMA_MODE": "skip"},
                          missing_kinds=[WELLBORE_KIND])

    def test_infer_mode_publishes_rows_with_inferred_column_types(self):
        table = self.read("inf_osdu_wks_wellbore")
        self.assertEqual(2, table.count())
        self.assertEqual("double", dict(table.dtypes)["data__Depth"])

    def test_skip_mode_reports_the_kind_as_schema_missing(self):
        self.assertEqual(["schema_missing"], [r.status for r in self.skipped["results"]])
        self.assertFalse(self.spark.catalog.tableExists("skp_osdu_wks_wellbore"))


class DryRunTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [row("test:well:1", "1", {"FacilityName": "Alpha"})])
        cls.namespace = run(cls.spark, {
            "RUN_PROFILE": "dry_run", "KINDS": [WELL_KIND], "TABLE_PREFIX": "dry_", "WRITE_MODE": "full_refresh",
            "VERSION_STRATEGY": "merge",
        })

    def test_dry_run_creates_no_output_tables(self):
        names = {t.name for t in self.spark.catalog.listTables()}
        self.assertFalse([n for n in names if n.startswith("dry_")], names)


class AuthenticationAndContextTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [row("test:well:1", "1", {"FacilityName": "Alpha"})])
        cls.original = {}
        cls.namespace = run(cls.spark, {
            "RUN_PROFILE": "inspect", "KINDS": [WELL_KIND], "TABLE_PREFIX": "auth_", "WRITE_MODE": "full_refresh",
            "VERSION_STRATEGY": "merge",
        }, capture=cls.original)

    def patched(self, **values):
        lowered = {name.lower(): value for name, value in values.items()
                   if name.lower() == name.upper().lower() and name.lower() in self.namespace and name.isupper()}
        return mock.patch.dict(self.namespace, {**values, **lowered})

    @staticmethod
    def token(value, expires_on=4102444800):
        return SimpleNamespace(token=value, expires_on=expires_on)

    def test_cli_token_is_acquired_and_cached(self):
        calls = []
        credential = SimpleNamespace(get_token=lambda scope: calls.append(scope) or self.token("cli-token"))
        self.namespace["_ADME_AUTH_STATE"]["tokens"].clear()
        with self.patched(ADME_AUTH_METHOD="CLI", AzureCliCredential=lambda: credential):
            self.assertEqual("cli-token", self.original["get_adme_access_token"]())
            self.assertEqual("cli-token", self.original["get_adme_access_token"]())
        self.assertEqual(1, len(calls))

    def test_cli_failure_explains_how_to_sign_in(self):
        def fail():
            raise RuntimeError("no login")
        with self.patched(ADME_AUTH_METHOD="CLI", AzureCliCredential=fail):
            with self.assertRaisesRegex(RuntimeError, "az login"):
                self.namespace["_acquire_adme_access_token"]()

    def test_managed_identity_uses_the_optional_user_assigned_client_id(self):
        created = []
        def factory(**kwargs):
            created.append(kwargs)
            return SimpleNamespace(get_token=lambda scope: self.token("mi-token"))
        with self.patched(ADME_AUTH_METHOD="MI", ADME_MANAGED_IDENTITY_CLIENT_ID="client-1", ManagedIdentityCredential=factory):
            self.assertEqual("mi-token", self.namespace["_acquire_adme_access_token"]()[0])
            self.assertEqual("client-1", self.namespace["_adme_token_cache_key"]()[2])
        with self.patched(ADME_AUTH_METHOD="MI", ADME_MANAGED_IDENTITY_CLIENT_ID="", ManagedIdentityCredential=factory):
            self.namespace["_acquire_adme_access_token"]()
        self.assertEqual([{"client_id": "client-1"}, {}], created)

    def test_service_principal_reads_its_secret_from_key_vault(self):
        secrets = []
        applications = []
        notebookutils = SimpleNamespace(credentials=SimpleNamespace(
            getSecret=lambda url, name: secrets.append((url, name)) or "sp-secret"))

        def application(**kwargs):
            applications.append(kwargs)
            return SimpleNamespace(acquire_token_for_client=lambda scopes: {"access_token": "sp-token", "expires_in": 3600})
        with self.patched(ADME_AUTH_METHOD="SP", ADME_TENANT_ID="tenant", ADME_SP_CLIENT_ID="client",
                          ADME_SP_SECRET_KV_NAME="vault", ADME_SP_SECRET_NAME="secret-name",
                          notebookutils=notebookutils, ConfidentialClientApplication=application):
            token, expires_on = self.namespace["_acquire_adme_access_token"]()
        self.assertEqual("sp-token", token)
        self.assertGreater(expires_on, 0)
        self.assertEqual([("https://vault.vault.azure.net/", "secret-name")], secrets)
        self.assertEqual("https://login.microsoftonline.com/tenant", applications[0]["authority"])
        self.assertEqual("sp-secret", applications[0]["client_credential"])

    def test_service_principal_rejects_an_error_result(self):
        application = lambda **kwargs: SimpleNamespace(
            acquire_token_for_client=lambda scopes: {"error": "invalid_client", "error_description": "bad"})
        notebookutils = SimpleNamespace(credentials=SimpleNamespace(getSecret=lambda url, name: "s"))
        with self.patched(ADME_AUTH_METHOD="SP", ADME_TENANT_ID="tenant", ADME_SP_CLIENT_ID="client",
                          ADME_SP_SECRET_KV_NAME="https://vault.example/", ADME_SP_SECRET_NAME="n",
                          notebookutils=notebookutils, ConfidentialClientApplication=application):
            with self.assertRaisesRegex(RuntimeError, "invalid_client"):
                self.namespace["_acquire_adme_access_token"]()

    def test_device_code_flow_prints_the_prompt_and_selects_the_signed_in_account(self):
        account = {"local_account_id": "oid-1", "realm": "tenant-1"}
        state = {"accounts": []}

        class Application:
            def __init__(self, **kwargs):
                pass

            def get_accounts(self):
                return state["accounts"]

            def initiate_device_flow(self, scopes):
                return {"message": "Open the device login page and enter the code."}

            def acquire_token_by_device_flow(self, flow):
                state["accounts"] = [account]
                return {"access_token": "dc-token", "expires_in": 3600,
                        "id_token_claims": {"oid": "oid-1", "tid": "tenant-1"}}

            def acquire_token_silent_with_error(self, scopes, account):
                return {"access_token": "dc-silent", "expires_in": 3600}

        self.namespace["_ADME_AUTH_STATE"]["device_clients"].clear()
        with self.patched(ADME_AUTH_METHOD="DC", ADME_TENANT_ID="tenant-1", ADME_DEVICE_CODE_CLIENT_ID="client",
                          PublicClientApplication=Application):
            self.assertEqual("dc-token", self.namespace["_acquire_adme_access_token"]()[0])
            self.assertEqual("dc-silent", self.namespace["_acquire_adme_access_token"]()[0])

    def test_http_error_detail_is_truncated(self):
        response = SimpleNamespace(text="x" * 1500)
        self.assertEqual(1003, len(self.namespace["_adme_response_detail"](response)))

    def test_workspace_and_lakehouse_ids_resolve_from_explicit_value_then_spark_conf(self):
        namespace = self.namespace
        self.assertEqual("explicit", namespace["_resolve_workspace_id"]("explicit"))
        self.assertEqual("explicit", namespace["_resolve_lakehouse_id"]("explicit"))
        self.spark.conf.set("trident.workspace.id", "workspace-from-conf")
        self.spark.conf.set("trident.lakehouse.id", "lakehouse-from-conf")
        try:
            self.assertEqual("workspace-from-conf", namespace["_resolve_workspace_id"]())
            self.assertEqual("lakehouse-from-conf", namespace["_resolve_lakehouse_id"]())
        finally:
            self.spark.conf.unset("trident.workspace.id")
            self.spark.conf.unset("trident.lakehouse.id")
        with self.assertRaises(ValueError):
            namespace["_resolve_workspace_id"]()
        with self.assertRaises(ValueError):
            namespace["_resolve_lakehouse_id"]()


class OutputShapeHelperTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [row("test:well:1", "1", {"FacilityName": "Alpha"})])
        cls.namespace = run(cls.spark, {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "docs_", "OUTPUT_DOCS_MODE": "full",
            "WRITE_MODE": "full_refresh", "VERSION_STRATEGY": "merge",
        })

    def test_full_documentation_lists_every_column_of_the_output_table(self):
        documented = {r["column_name"] for r in self.read("silver_output_documentation")
                      .filter("table_name = 'docs_osdu_wks_well'").collect()}
        self.assertIn("data__FacilityName", documented)

    def test_tag_children_are_pivoted_into_prefixed_parent_columns(self):
        parent = self.spark.createDataFrame([("a", "1"), ("b", "1")], "id string, version string")
        tags = self.spark.createDataFrame(
            [("a", "1", "env", "dev"), ("a", "1", "team", "x")], "id string, version string, tag_key string, tag_value string")
        row_a = self.namespace["_reassemble_tags"](parent, tags).filter("id = 'a'").collect()[0]
        self.assertEqual(("dev", "x"), (row_a["tag_env"], row_a["tag_team"]))

    def test_primitive_children_are_joined_into_one_delimited_column(self):
        parent = self.spark.createDataFrame([("a", "1")], "id string, version string")
        values = self.spark.createDataFrame(
            [("a", "1", 0, "x"), ("a", "1", 1, "y")], "id string, version string, ordinal int, value string")
        result = self.namespace["_reassemble_primitive"](parent, values, "codes").collect()[0]
        self.assertEqual({"x", "y"}, set(result["codes"].split(";")))

    def test_merge_key_fields_keep_the_source_type_and_default_to_string(self):
        frame = self.spark.createDataFrame([("a", 1)], "id string, version int")
        fields = {f.name: f.dataType.simpleString() for f in self.namespace["_merge_key_fields"](frame)}
        self.assertEqual({"id": "string", "version": "int"}, fields)

    def test_quality_issues_are_written_and_appended(self):
        namespace = self.namespace
        frame = namespace["_single_quality_issue_df"](
            self.spark, "run-1", WELL_KIND, "missing_merge_key_column", "error", "id", "detail")
        self.assertEqual(1, namespace["write_data_quality_issues"](self.spark, frame, "ws", "lh"))
        self.assertEqual(1, namespace["write_data_quality_issues"](self.spark, frame, "ws", "lh"))
        self.assertEqual(0, namespace["write_data_quality_issues"](self.spark, None, "ws", "lh"))
        issues = self.read("silver_data_quality_issues").filter("check_name = 'missing_merge_key_column'")
        self.assertEqual(2, issues.count())


if __name__ == "__main__":
    unittest.main()
