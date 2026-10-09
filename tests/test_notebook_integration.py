"""End-to-end notebook runs on local Spark/Delta.

The offline test uses synthetic bronze rows and a stubbed ADME schema service and token.
The live test is opt-in: it reads a local copy of an ACZ bronze Delta table and calls a real
ADME schema service with the Azure CLI login (ADME_AUTH_METHOD = "CLI").
"""

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fake_adme import free_port
from notebook_runner import run_notebook
from test_normalization import ROOT, T, java_is_available
from tno_bronze import BRONZE_DDL, write_bronze

if T is not None:
    from pyspark.sql import SparkSession

WELL_KIND = "osdu:wks:master-data--Well:1.0.0"
WELL_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Well",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "kind": {"type": "string"},
        "data": {
            "allOf": [{
                "type": "object",
                "properties": {
                    "FacilityName": {"type": "string"},
                    "FacilityTypeID": {
                        "type": "string",
                        "x-osdu-relationship": [{"GroupType": "reference-data", "EntityType": "FacilityType"}],
                    },
                    "NameAliases": {
                        "type": "array",
                        "items": {"type": "object", "properties": {"AliasName": {"type": "string"}}},
                    },
                },
            }],
        },
    },
}

def bronze_row(record_id, version, payload, active=True, kind=WELL_KIND):
    return (
        json.dumps({"data": payload, "meta": None, "modifyUser": "buildagent", "modifyTime": 1760000000000}), None, record_id, version, kind, None, None, None,
        "buildagent", None, None, None, None, None, None, None, active,
    )


def build_session(warehouse: str):
    from delta import configure_spark_with_delta_pip

    driver_port = free_port(3)
    builder = (
        SparkSession.builder.master("local[2]").appName("notebook-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.port", str(driver_port))
        .config("spark.driver.blockManager.port", str(driver_port + 1))
        .config("spark.blockManager.port", str(driver_port + 2))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@unittest.skipIf(T is None or importlib.util.find_spec("delta") is None,
                 "Install optional [spark] and [delta] dependencies for notebook integration tests.")
class NotebookIntegrationBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not java_is_available():
            raise unittest.SkipTest("A working Java runtime is required for notebook integration tests.")
        cls.temporary = tempfile.TemporaryDirectory(prefix="notebook-integration-")
        cls.addClassCleanup(cls.temporary.cleanup)
        environment = mock.patch.dict(os.environ, {
            "PYSPARK_PYTHON": sys.executable,
            "SPARK_LOCAL_IP": "127.0.0.1",
            "ADME_SPARK_SHUFFLE_PARTITIONS": "2",
            "ADME_WORKSPACE_ID": "00000000-0000-0000-0000-000000000001",
            "ADME_LAKEHOUSE_ID": "00000000-0000-0000-0000-000000000002",
        })
        environment.start()
        cls.addClassCleanup(environment.stop)
        cls.spark = build_session(cls.temporary.name + "/warehouse")
        cls.addClassCleanup(cls.spark.stop)

    def read(self, table):
        return self.spark.table(table)


class OfflineNotebookRunTests(NotebookIntegrationBase):
    PREFIX = "it_"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.createDataFrame([
            bronze_row("test:well:1", "1", {"FacilityName": "Alpha", "FacilityTypeID": "test:reference-data--FacilityType:Well:",
                                            "NameAliases": [{"AliasName": "A1"}, {"AliasName": "A2"}]}),
            bronze_row("test:well:2", "1", {"FacilityName": "Beta", "NameAliases": []}),
            bronze_row("test:reference-data--FacilityType:Well", "1", {"Name": "Well"},
                       kind="osdu:wks:reference-data--FacilityType:1.0.0"),
            bronze_row("test:well:3", "1", {"FacilityName": "Retired"}, active=False),
        ], BRONZE_DDL).write.format("delta").saveAsTable("osducatalog")

        schema_urls = []

        def serve_schema(url, context, timeout=None, session=None):
            schema_urls.append(url)
            return [] if "latestVersion" in url else {"schema": WELL_SCHEMA}

        def stub_services(namespace):
            namespace["get_adme_access_token"] = lambda: "unit-test-token"
            namespace["_adme_schema_get_json"] = serve_schema

        cls.displayed = []
        cls.schema_urls = schema_urls
        cls.namespace = run_notebook(cls.spark, {
            "ADME_ENDPOINT": "https://adme.example.test",
            "ADME_DATA_PARTITION_ID": "test-partition",
            "ADME_AUTH_METHOD": "CLI",
            "RUN_PROFILE": "execute",
            "KINDS": [WELL_KIND],
            "TABLE_PREFIX": cls.PREFIX,
            "ALLOW_OVERWRITE": True,
            "WRITE_MODE": "full_refresh",
            "VERSION_STRATEGY": "merge",
        }, before_pipeline=stub_services, displayed=cls.displayed)

    def test_schema_service_was_queried_for_the_selected_kind(self):
        self.assertTrue(any(url.endswith("/" + WELL_KIND) for url in self.schema_urls))

    def test_parent_table_contains_only_active_records(self):
        rows = {row["id"]: row for row in self.read("it_osdu_wks_well").collect()}
        self.assertEqual({"test:well:1", "test:well:2"}, set(rows))
        self.assertEqual("Alpha", rows["test:well:1"]["data__FacilityName"])

    def test_wrapper_epoch_milliseconds_are_normalized_to_timestamp(self):
        normalized = self.namespace["_normalize_bronze_record_wrapper"](self.read("osducatalog"))
        row = normalized.where("id = 'test:well:1'").selectExpr("unix_timestamp(modifyTime) AS epoch_seconds").first()
        self.assertEqual(1760000000, row["epoch_seconds"])

    def test_array_elements_become_child_rows_keyed_by_parent(self):
        aliases = self.read("it_osdu_wks_well___namealiases").collect()
        self.assertEqual({"A1", "A2"}, {row["AliasName"] for row in aliases})
        self.assertEqual({"test:well:1"}, {row["id"] for row in aliases})

    def test_declared_relationship_is_published_to_a_bridge_table(self):
        bridge = [name for name in (t.name for t in self.spark.catalog.listTables())
                  if name.startswith("it_relationship__") and name.endswith("facilitytype")]
        self.assertEqual(1, len(bridge), bridge)
        row = self.read(bridge[0]).collect()[0]
        self.assertEqual(("test:well:1", "resolved", "test:reference-data--FacilityType:Well"),
                         (row["source_id"], row["status"], row["target_id"]))

    def test_run_metadata_reports_success(self):
        statuses = {row["status"] for row in self.read("silver_run_status").collect()}
        self.assertEqual({"started", "committed"}, {s.lower() for s in statuses})
        self.assertEqual(1, len([r for r in self.namespace["results"] if r]))
        self.assertGreaterEqual(self.read("silver_run_manifest").count(), 1)

    def test_results_summary_is_displayed(self):
        self.assertTrue(self.displayed)


LIVE_BRONZE = os.environ.get("ADME_ACZ_LIVE_BRONZE_PATH")
TNO_RECORDS = int(os.environ.get("ADME_ACZ_LIVE_TNO_RECORDS", "25"))


@unittest.skipUnless(os.environ.get("ADME_ACZ_LIVE_ENDPOINT") and os.environ.get("ADME_ACZ_LIVE_PARTITION"),
                     "Set ADME_ACZ_LIVE_ENDPOINT and ADME_ACZ_LIVE_PARTITION and sign in with the Azure CLI.")
class LiveNotebookRunTests(NotebookIntegrationBase):
    """Bronze comes from ADME_ACZ_LIVE_BRONZE_PATH, or is regenerated from the public OSDU TNO test data."""

    def test_well_and_wellbore_kinds_are_published_from_bronze(self):
        bronze_path = LIVE_BRONZE
        if not bronze_path:
            bronze_path = self.temporary.name + "/tno-osducatalog"
            write_bronze(self.spark, bronze_path, TNO_RECORDS, TNO_RECORDS, os.environ["ADME_ACZ_LIVE_PARTITION"],
                         Path(self.temporary.name) / "tno-cache")
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS osducatalog USING delta LOCATION '{Path(bronze_path).as_posix()}'")
        namespace = run_notebook(self.spark, {
            "ADME_ENDPOINT": os.environ["ADME_ACZ_LIVE_ENDPOINT"],
            "ADME_DATA_PARTITION_ID": os.environ["ADME_ACZ_LIVE_PARTITION"],
            "ADME_AUTH_METHOD": "CLI",
            "RUN_PROFILE": "execute",
            "KINDS": ["osdu:wks:master-data--Well:*", "osdu:wks:master-data--Wellbore:*"],
            "TABLE_PREFIX": "live_",
            "ALLOW_OVERWRITE": True,
            "WRITE_MODE": "full_refresh",
            "VERSION_STRATEGY": "merge",
        })
        self.assertEqual(2, len(namespace["results"]))
        active = self.spark.table("osducatalog").filter("isActive = true")
        for kind_fragment, table in (("master-data--Well:", "live_osdu_wks_well"),
                                     ("master-data--Wellbore:", "live_osdu_wks_wellbore")):
            expected = active.filter(f"kind like '%{kind_fragment}%'").select("id", "version").distinct().count()
            self.assertGreater(expected, 0)
            self.assertEqual(expected, self.spark.table(table).select("id", "version").distinct().count())


if __name__ == "__main__":
    unittest.main()
