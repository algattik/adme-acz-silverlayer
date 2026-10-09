"""Synthetic Spark coverage for scalar ADME relationship projection."""

import ast
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_normalization import ROOT, T, java_is_available
from test_notebook_simplification import load_notebook

if T is not None:
    from pyspark.sql import SparkSession, Window, functions as F
    from adme_acz_silverlayer.normalization import make_delta_column_alias


@unittest.skipIf(T is None, "Install the optional [spark] dependency for relationship bridge integration tests.")
class RelationshipBridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not java_is_available():
            raise unittest.SkipTest("A working Java runtime is required for local Spark tests.")
        cls.temporary = tempfile.TemporaryDirectory(prefix="relationship-bridge-tests-")
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.environment = mock.patch.dict(os.environ, {
            "PYSPARK_PYTHON": sys.executable,
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYTHONPATH": str(ROOT / "src") + os.pathsep + os.environ.get("PYTHONPATH", ""),
        })
        cls.environment.start()
        cls.addClassCleanup(cls.environment.stop)
        cls.spark = SparkSession.builder.master("local[2]").appName("relationship-bridge-tests").config(
            "spark.ui.enabled", "false"
        ).config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        ).config("spark.sql.shuffle.partitions", "2").config(
            "spark.sql.warehouse.dir", cls.temporary.name
        ).getOrCreate()
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")

        notebook = load_notebook()
        nodes = [
            node for cell in notebook["cells"] if cell["cell_type"] == "code"
            for node in ast.parse("".join(cell["source"])).body
            if isinstance(node, ast.FunctionDef) and node.name in {
                "resolve_direct_relationships",
                "relationship_bridge_table_name",
            }
        ]
        module = ast.Module(body=nodes, type_ignores=[])
        ast.fix_missing_locations(module)
        namespace = {
            "F": F, "Window": Window, "DataFrame": object, "SchemaRegistry": object,
            "re": __import__("re"),
            "validate_table_names": lambda names: None,
            "_quoted_top_level_col": lambda name: F.col("`" + name.replace("`", "``") + "`"),
            "make_delta_column_alias": make_delta_column_alias,
        }
        exec(compile(module, "<notebook-direct-fk>", "exec"), namespace)
        cls.resolve_relationships = staticmethod(namespace["resolve_direct_relationships"])
        cls.bridge_table_name = staticmethod(namespace["relationship_bridge_table_name"])

    def test_merge_bridge_names_omit_source_schema_version(self):
        self.assertEqual(
            self.bridge_table_name(
                "osdu:wks:master-data--Well:1.0.0",
                "data__ExistenceKind",
                "reference-data--ExistenceKind",
                version_strategy="merge",
            ),
            "relationship__osdu_wks_master_data_well__data_existencekind__reference_data_existencekind",
        )
        self.assertEqual(
            self.bridge_table_name(
                "osdu:wks:master-data--Well:1.0.0",
                "data__ExistenceKind",
                "reference-data--ExistenceKind",
            ),
            "relationship__osdu_wks_master_data_well_1_0_0__data_existencekind__reference_data_existencekind",
        )
        self.assertEqual(
            self.bridge_table_name(
                "osdu:wks:master-data--Well:2.0.0",
                "data__ExistenceKind",
                "reference-data--ExistenceKind",
                version_strategy="merge",
            ),
            "relationship__osdu_wks_master_data_well__data_existencekind__reference_data_existencekind",
        )

    def test_scalar_relationship_projects_joinable_id_and_exact_or_latest_version(self):
        from pyspark.sql import Row

        well_kind = "osdu:wks:master-data--Well:1.0.0"
        well_kind_v2 = "osdu:wks:master-data--Well:2.0.0"
        wellbore_kind = "osdu:wks:master-data--Wellbore:1.0.0"
        well_id = "osdu:master-data--Well:well-a"
        other_id = "osdu:master-data--Well:well-b"
        source = self.spark.createDataFrame([
            (well_id, "0008", well_kind, True),
            (well_id, "9", well_kind_v2, False),
            (other_id, "2", well_kind, True),
            ("osdu:master-data--Wellbore:wellbore-a", "1", wellbore_kind, True),
        ], "id string, version string, kind string, isActive boolean")
        parent = self.spark.createDataFrame([
            ("osdu:master-data--Wellbore:wb-1", "1", wellbore_kind, well_id + ":"),
            ("osdu:master-data--Wellbore:wb-2", "1", wellbore_kind, well_id + ":0008"),
            ("osdu:master-data--Wellbore:wb-3", "1", wellbore_kind, other_id + ":"),
            ("osdu:master-data--Wellbore:wb-4", "1", wellbore_kind, "invalid-reference"),
            ("osdu:master-data--Wellbore:wb-5", "1", wellbore_kind, well_id + ":88"),
            ("osdu:master-data--Wellbore:wb-6", "1", wellbore_kind, None),
        ], "id string, version string, kind string, data__WellID string")

        class Registry:
            def direct_relationship_fields(self, kind):
                if kind != wellbore_kind:
                    return []
                return [{"field": "WellID", "targets": [
                    {"GroupType": "master-data", "EntityType": "Well"}
                ]}]

        result, bridge, bridge_tables = self.resolve_relationships(
            parent, source, Registry(), wellbore_kind, "osdu_wks_wellbore", "fixture-run",
            version_strategy="merge",
        )
        rows = {row.id: row for row in result.collect()}
        self.assertNotIn("data__WellID__fk_id", result.columns)
        self.assertNotIn("data__WellID__fk_version", result.columns)
        self.assertEqual(rows["osdu:master-data--Wellbore:wb-1"]["data__WellID"], well_id + ":")
        self.assertEqual(rows["osdu:master-data--Wellbore:wb-2"]["data__WellID"], well_id + ":0008")
        self.assertEqual(result.count(), parent.count())
        self.assertEqual(len(bridge_tables), 1)
        self.assertEqual(
            bridge_tables,
            ["relationship__osdu_wks_master_data_wellbore__data_wellid__master_data_well"],
        )

        bridge_rows = {row.source_id: row for row in bridge.collect()}
        self.assertEqual(set(bridge_rows), {
            "osdu:master-data--Wellbore:wb-1",
            "osdu:master-data--Wellbore:wb-2",
            "osdu:master-data--Wellbore:wb-3",
        })
        self.assertEqual(
            bridge_rows["osdu:master-data--Wellbore:wb-1"].status,
            "target_deleted",
        )
        self.assertEqual(
            bridge_rows["osdu:master-data--Wellbore:wb-2"].target_version,
            "0008",
        )
        self.assertEqual(
            bridge_rows["osdu:master-data--Wellbore:wb-2"].status,
            "resolved",
        )
        self.assertEqual(
            bridge_rows["osdu:master-data--Wellbore:wb-1"].target_id,
            bridge_rows["osdu:master-data--Wellbore:wb-2"].target_id,
        )
        self.assertTrue(all(row.run_id == "fixture-run" for row in bridge_rows.values()))

    def test_distinct_relationship_paths_get_distinct_bridge_tables(self):
        well_id = "osdu:master-data--Well:well-a"
        kind = "osdu:wks:master-data--Wellbore:1.0.0"
        source = self.spark.createDataFrame(
            [(well_id, "1", "osdu:wks:master-data--Well:1.0.0", True)],
            "id string, version string, kind string, isActive boolean",
        )
        parent = self.spark.createDataFrame(
            [("wb", "1", kind, well_id + ":", well_id + ":")],
            "id string, version string, kind string, data__WellID string, data__ParentWellID string",
        )

        class Registry:
            def direct_relationship_fields(self, kind):
                return [
                    {"field": field, "targets": [{"GroupType": "master-data", "EntityType": "Well"}]}
                    for field in ("WellID", "ParentWellID")
                ]

        result, bridge, bridge_tables = self.resolve_relationships(
            parent, source, Registry(), kind, "osdu_wks_wellbore", "fixture-run"
        )
        self.assertEqual(result.columns, parent.columns)
        self.assertEqual(len(bridge_tables), 2)
        self.assertEqual({row.relationship_path for row in bridge.collect()}, {
            "data__WellID", "data__ParentWellID",
        })
        self.assertEqual(
            {row._relationship_bridge_table for row in bridge.collect()},
            set(bridge_tables),
        )
        analyzed_plan = bridge._jdf.queryExecution().analyzed()

        def join_count(plan):
            children = plan.children()
            return int(plan.nodeName() == "Join") + sum(
                join_count(children.apply(index))
                for index in range(children.size())
            )

        self.assertEqual(join_count(analyzed_plan), 1)

    def test_overlapping_target_sets_preserve_exact_and_latest_resolution(self):
        kind = "osdu:wks:master-data--Wellbore:1.0.0"
        well_id = "osdu:master-data--Well:well-a"
        bore_id = "osdu:master-data--Wellbore:bore-a"
        well_kind = "osdu:wks:master-data--Well:1.0.0"
        huge_version = "1" + "0" * 45
        source = self.spark.createDataFrame([
            (well_id, "000", well_kind, True),
            (well_id, "9" * 44, well_kind, True),
            (well_id, huge_version, well_kind, False),
            (well_id, "not-a-version", well_kind, True),
            (bore_id, "2", kind, None),
            ("osdu:master-data--Other:other-a", "1", "osdu:wks:master-data--Other:1.0.0", True),
        ], "id string, version string, kind string, isActive boolean")
        parent = self.spark.createDataFrame([
            ("source-a", "1", kind, well_id + ":", well_id + ":0000", bore_id + ":"),
            ("source-b", "2", kind, bore_id + ":", "invalid", well_id + ":" + huge_version),
            ("source-c", "1", kind, None, well_id + ":7", "osdu:master-data--Other:other-a:"),
        ], "id string, version string, kind string, data__WellID string, data__ExactID string, data__EitherID string")

        class Registry:
            def direct_relationship_fields(self, kind):
                well = {"GroupType": "master-data", "EntityType": "Well"}
                bore = {"GroupType": "master-data", "EntityType": "Wellbore"}
                return [
                    {"field": "WellID", "targets": [well]},
                    {"field": "ExactID", "targets": [well]},
                    {"field": "EitherID", "targets": [bore, well]},
                ]

        result, bridge, tables = self.resolve_relationships(parent, source, Registry(), kind)
        self.assertIs(result, parent)
        self.assertEqual(len(tables), 4)
        rows = bridge.collect()
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            {(r.source_id, r.relationship_path, r.target_id, r.target_version, r.status) for r in rows},
            {
                ("source-a", "data__WellID", well_id, huge_version, "target_deleted"),
                ("source-a", "data__ExactID", well_id, "000", "resolved"),
                ("source-a", "data__EitherID", bore_id, "2", "resolved"),
                ("source-b", "data__EitherID", well_id, huge_version, "target_deleted"),
            },
        )
        for row in rows:
            target_type = row.target_kind.split(":")[2]
            self.assertEqual(
                row._relationship_bridge_table,
                self.bridge_table_name(kind, row.relationship_path, target_type),
            )

    def test_empty_references_retain_planned_tables_and_bridge_schema(self):
        kind = "osdu:wks:master-data--Wellbore:1.0.0"
        source = self.spark.createDataFrame([], "id string, version string, kind string")
        parent = self.spark.createDataFrame(
            [("source", "1", kind, None)],
            "id string, version string, kind string, data__WellID string",
        )

        class Registry:
            def direct_relationship_fields(self, kind):
                return [{"field": "WellID", "targets": [
                    {"GroupType": "master-data", "EntityType": "Well"},
                ]}]

        result, bridge, tables = self.resolve_relationships(parent, source, Registry(), kind)
        self.assertIs(result, parent)
        self.assertEqual(len(tables), 1)
        self.assertEqual(bridge.count(), 0)
        self.assertEqual(bridge.schema["target_is_active"].dataType, T.BooleanType())

    def test_duplicate_target_identity_is_rejected(self):
        target_kind = "osdu:wks:master-data--Well:1.0.0"
        target_id = "osdu:master-data--Well:well-a"
        source = self.spark.createDataFrame([
            (target_id, "1", target_kind),
            (target_id, "01", target_kind),
        ], "id string, version string, kind string")
        parent = self.spark.createDataFrame(
            [("record", "osdu:master-data--Well:well-a:1")], "id string, data__WellID string"
        )

        class Registry:
            def direct_relationship_fields(self, kind):
                return [{"field": "WellID", "targets": [
                    {"GroupType": "master-data", "EntityType": "Well"}
                ]}]

        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.resolve_relationships(parent, source, Registry(), "osdu:wks:master-data--Wellbore:1.0.0")


if __name__ == "__main__":
    unittest.main()
