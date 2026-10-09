from __future__ import annotations

import ast
import importlib.util
import json
from pathlib import Path
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("experiment_sync", ROOT / "scripts/sync_parallelism_experiment.py")
experiment_sync = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(experiment_sync)


GROUP_FIXTURE = """
def run_silver_build(groups, process, incremental=False):
    timings = {}
    schema_registry = globals().get("prefetched_registry", object())
    metadata_batching = True
    output_docs_rows = []
    data_quality_issue_rows = []
    relationship_frames = []
    relationship_changed_key_frames = []
    relationship_bridge_tables = []
    results = []
    for group_index, group in enumerate(groups, 1):
        group_kinds = list(group["kinds"])
        start_time = perf_counter()
        error_message = None
        error_type = None
        group_results = []
        t_group = perf_counter()
        try:
            group_results = process(group, output_docs_rows, data_quality_issue_rows,
                relationship_frames, relationship_changed_key_frames, relationship_bridge_tables)
        except ValueError as exc:
            error_message = str(exc)
            error_type = type(exc).__name__
            group_results = [("failed", group["kinds"][0])]
        timings["group_processing"] = timings.get("group_processing", 0.0) + (perf_counter() - t_group)
        end_time = perf_counter()
        for result in group_results:
            results.append(result)
    return results, output_docs_rows, data_quality_issue_rows, relationship_frames, relationship_changed_key_frames, relationship_bridge_tables, timings
"""


class ParallelismExperimentTests(unittest.TestCase):
    def test_committed_experiment_is_generated_and_clean(self):
        main = json.loads(experiment_sync.MAIN.read_text(encoding="utf-8"))
        generated = experiment_sync.build_experiment(main)
        committed = json.loads(experiment_sync.EXPERIMENT.read_text(encoding="utf-8"))
        self.assertEqual(generated, committed)
        self.assertEqual(generated, experiment_sync.build_experiment(main))
        for cell in generated["cells"]:
            if cell["cell_type"] == "code":
                self.assertEqual(cell["outputs"], [])
                self.assertIsNone(cell["execution_count"])
                compile("".join(cell["source"]), "<experiment>", "exec")

    def test_common_implementation_is_identical(self):
        main = json.loads(experiment_sync.MAIN.read_text(encoding="utf-8"))
        generated = experiment_sync.build_experiment(main)
        for original, experiment in zip(main["cells"], generated["cells"]):
            if original["cell_type"] != "code":
                continue
            source = "".join(original["source"])
            if "CUSTOMER SETTINGS" in source or "FABRIC RUNTIME RESOLUTION" in source:
                continue
            left = ast.parse(source)
            right = ast.parse("".join(experiment["source"]))
            left.body = [node for node in left.body if not isinstance(node, ast.FunctionDef) or node.name != "run_silver_build"]
            right.body = [node for node in right.body if not isinstance(node, ast.FunctionDef) or node.name != "run_silver_build"]
            self.assertEqual(ast.dump(left), ast.dump(right))

    def fixture(self, parallelism=2, **overrides):
        namespace = {
            "perf_counter": perf_counter,
            "ThreadPoolExecutor": ThreadPoolExecutor,
            "group_processing_parallelism": parallelism,
            "batch_metadata_writes": True,
            "schema_preflight": True,
            "output_write_parallelism": 1,
            **overrides,
        }
        exec(compile(experiment_sync.concurrent_build(GROUP_FIXTURE), "<fixture>", "exec"), namespace)
        return namespace["run_silver_build"]

    def test_concurrent_groups_keep_buffers_isolated_and_results_ordered(self):
        rendezvous = threading.Barrier(2)

        def process(group, *buffers):
            rendezvous.wait(timeout=10)
            name = group["kinds"][0]
            for buffer in buffers:
                self.assertEqual(buffer, [])
                buffer.append(name)
            return [name]

        result = self.fixture()([{"kinds": ["alpha"]}, {"kinds": ["beta"]}], process)
        for values in result[:-1]:
            self.assertEqual(values, ["alpha", "beta"])
        self.assertIn("group_processing_wall_seconds", result[-1])

    def test_serial_baseline_and_group_failures_preserve_results(self):
        def process(group, *buffers):
            name = group["kinds"][0]
            if name == "beta":
                raise ValueError("synthetic failure")
            for buffer in buffers:
                buffer.append(name)
            return [name]

        result = self.fixture(parallelism=1)([{"kinds": ["alpha"]}, {"kinds": ["beta"]}], process)
        self.assertEqual(result[0], ["alpha", ("failed", "beta")])
        self.assertEqual(result[1], ["alpha"])

    def test_invalid_trial_settings_fail_before_processing(self):
        def must_not_process(*args):
            self.fail("invalid experiment must fail before processing")

        cases = [
            ({"batch_metadata_writes": False}, False, "BATCH_METADATA"),
            ({"schema_preflight": False}, False, "SCHEMA_PREFLIGHT"),
            ({"output_write_parallelism": 2}, False, "OUTPUT_WRITE"),
            ({}, True, "full_refresh"),
        ]
        for overrides, incremental, message in cases:
            with self.subTest(overrides=overrides, incremental=incremental):
                with self.assertRaisesRegex(ValueError, message):
                    self.fixture(**overrides)([{"kinds": ["alpha"]}], must_not_process, incremental=incremental)

    def test_generator_rejects_unknown_group_loop_shape(self):
        with self.assertRaisesRegex(ValueError, "group-processing loop"):
            experiment_sync.concurrent_build(GROUP_FIXTURE.replace("group_index, group", "index, group"))

    def test_buffer_localization_preserves_keyword_names_and_text(self):
        source = 'process("é", output_docs_rows=output_docs_rows, label="output_docs_rows")\n'
        self.assertEqual(
            experiment_sync._localize_buffers(source),
            'process("é", output_docs_rows=local_output_docs_rows, label="output_docs_rows")\n',
        )

    def test_failed_schema_preflight_prevents_group_dispatch(self):
        def must_not_process(*args):
            self.fail("failed schema preflight must prevent concurrent schema-cache writes")

        with self.assertRaisesRegex(RuntimeError, "successful schema preflight"):
            self.fixture(prefetched_registry=None)([{"kinds": ["alpha"]}], must_not_process)

    def test_generated_defaults_are_inspect_only_and_isolated(self):
        main = json.loads(experiment_sync.MAIN.read_text(encoding="utf-8"))
        generated = experiment_sync.build_experiment(main)
        source = next("".join(cell["source"]) for cell in generated["cells"]
                      if cell["cell_type"] == "code" and "CUSTOMER SETTINGS" in "".join(cell["source"]))
        settings = {ast.unparse(node.targets[0]): ast.literal_eval(node.value)
                    for node in ast.parse(source).body if isinstance(node, ast.Assign)}
        self.assertEqual(settings["RUN_PROFILE"], "inspect")
        self.assertFalse(settings["ALLOW_OVERWRITE"])
        self.assertEqual(settings["TABLE_PREFIX"], "parallelism_exp_p1_")
        self.assertEqual(settings["WRITE_MODE"], "full_refresh")
        self.assertEqual(settings["OUTPUT_WRITE_PARALLELISM"], 1)
        self.assertEqual(settings["GROUP_PROCESSING_PARALLELISM"], 1)

    def test_real_generated_build_checks_guards_before_dependencies(self):
        main = json.loads(experiment_sync.MAIN.read_text(encoding="utf-8"))
        generated = experiment_sync.build_experiment(main)
        source = next("".join(cell["source"]) for cell in generated["cells"]
                      if cell["cell_type"] == "code" and "def run_silver_build(" in "".join(cell["source"]))
        function = next(node for node in ast.parse(source).body
                        if isinstance(node, ast.FunctionDef) and node.name == "run_silver_build")
        module = ast.Module(body=[function], type_ignores=[])
        for setting, value, incremental in [
            ("batch_metadata_writes", False, False),
            ("schema_preflight", False, False),
            ("output_write_parallelism", 2, False),
            ("output_write_parallelism", 1, True),
        ]:
            with self.subTest(setting=setting, incremental=incremental):
                namespace = {setting: value}
                exec(compile(module, "<generated-guards>", "exec"), namespace)
                with self.assertRaisesRegex(ValueError, "parallelism experiment requires"):
                    namespace["run_silver_build"](None, [], "", "", incremental=incremental)

    def test_real_delta_serial_and_parallel_outputs_match(self):
        import notebook_runner
        from test_notebook_integration import NotebookIntegrationBase, OfflineNotebookRunTests, WELL_KIND
        from test_notebook_modes import WELLBORE_KIND, row, schema_stub, write_bronze

        if getattr(OfflineNotebookRunTests, "__unittest_skip__", False):
            self.skipTest(OfflineNotebookRunTests.__unittest_skip_why__)

        class ExperimentDeltaRun(NotebookIntegrationBase):
            def runTest(self):
                write_bronze(self.spark, [
                    row("test:master-data--Well:sample", "1", {
                        "FacilityName": "Synthetic well",
                        "FacilityTypeID": "test:reference-data--FacilityType:sample:",
                    }),
                    row("test:master-data--Wellbore:sample", "1", {
                        "FacilityName": "Synthetic bore",
                    }, kind=WELLBORE_KIND),
                    row("test:reference-data--FacilityType:sample", "1", {"Code": "Synthetic"},
                        kind="osdu:wks:reference-data--FacilityType:1.0.0"),
                ])

                def stub_services(namespace):
                    namespace["get_adme_access_token"] = lambda: "synthetic-token"
                    namespace["_adme_schema_get_json"] = schema_stub()
                    namespace["_table_path_uri"] = lambda table: f"file:///nonexistent-delta-path/{table}"

                snapshots = []
                for parallelism in (1, 2):
                    prefix = f"experiment_p{parallelism}_"
                    settings = {
                        "ADME_ENDPOINT": "https://adme.example.test",
                        "ADME_DATA_PARTITION_ID": "test-partition",
                        "ADME_AUTH_METHOD": "CLI",
                        "RUN_PROFILE": "execute",
                        "KINDS": [WELL_KIND, WELLBORE_KIND],
                        "TABLE_PREFIX": prefix,
                        "ALLOW_OVERWRITE": True,
                        "WRITE_MODE": "full_refresh",
                        "VERSION_STRATEGY": "merge",
                        "SCHEMA_PREFLIGHT": True,
                        "BATCH_METADATA_WRITES": True,
                        "OUTPUT_WRITE_PARALLELISM": 1,
                        "GROUP_PROCESSING_PARALLELISM": parallelism,
                    }
                    with mock.patch.object(notebook_runner, "NOTEBOOK", experiment_sync.EXPERIMENT):
                        namespace = notebook_runner.run_notebook(self.spark, settings, before_pipeline=stub_services)
                    self.assertEqual([result.status for result in namespace["results"]], ["success", "success"])
                    tables = {}
                    bridge_count = 0
                    for result in namespace["results"]:
                        for name in [result.parent_table, *(result.child_tables or [])]:
                            frame = self.spark.table(name)
                            schema = frame.schema.json()
                            columns = sorted(set(frame.columns) - {"run_id", "ingested_at", "source_table"})
                            tables[name.removeprefix(prefix)] = (schema, sorted(frame.select(*columns).toJSON().collect()))
                            if name.startswith(f"{prefix}relationship__"):
                                bridge_count += frame.count()
                    self.assertEqual(bridge_count, 1)
                    snapshots.append(tables)
                self.assertEqual(snapshots[0], snapshots[1])

        result = unittest.TestResult()
        unittest.TestSuite([ExperimentDeltaRun()]).run(result)
        self.assertFalse(result.errors or result.failures, result.errors + result.failures)
        if result.skipped:
            self.skipTest(result.skipped[0][1])


if __name__ == "__main__":
    unittest.main()
