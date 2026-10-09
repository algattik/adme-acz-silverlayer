"""Generate the opt-in group-concurrency notebook from the main notebook."""

from __future__ import annotations

import argparse
import ast
import copy
import json
from pathlib import Path
import textwrap


ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "ADME ACZ Silver Layer.ipynb"
EXPERIMENT = ROOT / "ADME ACZ Silver Parallelism Experiment.ipynb"
KINDS = [
    "osdu:wks:master-data--Well:*",
    "osdu:wks:master-data--Wellbore:*",
    "osdu:wks:work-product-component--WellLog:*",
    "osdu:wks:work-product-component--WellboreMarkerSet:*",
    "osdu:wks:work-product-component--WellboreTrajectory:*",
    "osdu:wks:reference-data--CoordinateReferenceSystem:*",
]
BUFFERS = (
    "output_docs_rows",
    "data_quality_issue_rows",
    "relationship_frames",
    "relationship_changed_key_frames",
    "relationship_bridge_tables",
)


def _replace_once(source: str, before: str, after: str) -> str:
    if source.count(before) != 1:
        raise ValueError(f"Expected one experiment insertion point: {before!r}")
    return source.replace(before, after, 1)


def _localize_buffers(source: str) -> str:
    lines = source.splitlines(keepends=True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))
    edits = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Name) and node.id in BUFFERS:
            line = lines[node.lineno - 1].encode("utf-8")
            start = offsets[node.lineno - 1] + len(line[:node.col_offset].decode("utf-8"))
            end = offsets[node.lineno - 1] + len(line[:node.end_col_offset].decode("utf-8"))
            edits.append((start, end, f"local_{node.id}"))
    for start, end, replacement in sorted(edits, reverse=True):
        source = source[:start] + replacement + source[end:]
    return source


def concurrent_build(source: str) -> str:
    """Reuse the production group body, coordinating its results serially."""
    module = ast.parse(source)
    functions = [node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "run_silver_build"]
    if len(functions) != 1:
        raise ValueError("Expected one run_silver_build function")
    function = functions[0]
    loops = [
        node for node in ast.walk(function)
        if isinstance(node, ast.For) and ast.unparse(node.target) == "(group_index, group)"
    ]
    if len(loops) != 1:
        raise ValueError("Expected one serial group-processing loop")
    loop = loops[0]
    result_indexes = [
        index for index, node in enumerate(loop.body)
        if isinstance(node, ast.For) and ast.unparse(node.target) == "result"
        and ast.unparse(node.iter) == "group_results"
    ]
    if len(result_indexes) != 1:
        raise ValueError("Expected one result-recording loop")
    split = result_indexes[0]
    worker_body = loop.body[:split]
    timing_indexes = [
        index for index, node in enumerate(worker_body)
        if isinstance(node, ast.Assign)
        and any(ast.unparse(target) == "timings['group_processing']" for target in node.targets)
    ]
    if len(timing_indexes) != 1:
        raise ValueError("Expected one group-processing timer")
    lines = source.splitlines(keepends=True)
    prefix = textwrap.dedent("".join(lines[loop.body[0].lineno - 1:loop.body[split].lineno - 1]))
    timer = ast.get_source_segment(source, worker_body[timing_indexes[0]])
    prefix = _localize_buffers(_replace_once(prefix, timer, "elapsed = perf_counter() - t_group"))
    keys = ("start_time", "end_time", "error_message", "error_type", "group_results", "elapsed")
    returned = [f"{name!r}: {name}" for name in keys]
    returned.extend(f"{name!r}: local_{name}" for name in BUFFERS)
    worker = "def _run_experiment_group(group):\n" + textwrap.indent(
        "\n".join(f"local_{name} = []" for name in BUFFERS) + "\n"
        + prefix + "return {" + ", ".join(returned) + "}\n", "    "
    )
    prelude = ["group_kinds = list(group['kinds'])"]
    prelude.extend(f"{name} = outcome[{name!r}]" for name in keys if name != "elapsed")
    prelude.append("timings['group_processing'] = timings.get('group_processing', 0.0) + outcome['elapsed']")
    prelude.extend(f"{name}.extend(outcome[{name!r}])" for name in BUFFERS)
    tail = textwrap.dedent("".join(lines[loop.body[split].lineno - 1:loop.end_lineno]))
    coordinator = "for group_index, (group, outcome) in enumerate(zip(groups, group_outcomes), 1):\n" + textwrap.indent(
        "\n".join(prelude) + "\n" + tail, "    "
    )
    dispatch = (
        "if schema_registry is None:\n"
        "    raise RuntimeError('The parallelism experiment requires a successful schema preflight before dispatch.')\n"
        "group_wall_started = perf_counter()\n"
        "with ThreadPoolExecutor(max_workers=min(group_processing_parallelism, max(1, len(groups)))) as executor:\n"
        "    group_outcomes = list(executor.map(_run_experiment_group, groups))\n"
        "timings['group_processing_wall_seconds'] = perf_counter() - group_wall_started\n"
    )
    lines[loop.lineno - 1:loop.end_lineno] = [textwrap.indent(worker + dispatch + coordinator, " " * loop.col_offset)]
    guard = (
        "if incremental:\n"
        "    raise ValueError('The parallelism experiment requires full_refresh.')\n"
        "if not globals().get('batch_metadata_writes', True):\n"
        "    raise ValueError('The parallelism experiment requires BATCH_METADATA_WRITES = True.')\n"
        "if not globals().get('schema_preflight', True):\n"
        "    raise ValueError('The parallelism experiment requires SCHEMA_PREFLIGHT = True.')\n"
        "if int(globals().get('output_write_parallelism', 1)) != 1:\n"
        "    raise ValueError('The parallelism experiment requires OUTPUT_WRITE_PARALLELISM = 1.')\n"
    )
    docstring_offset = int(bool(function.body and isinstance(function.body[0], ast.Expr)
                                and isinstance(function.body[0].value, ast.Constant)
                                and isinstance(function.body[0].value.value, str)))
    first_statement = function.body[docstring_offset]
    lines[first_statement.lineno - 1:first_statement.lineno - 1] = [
        textwrap.indent(guard, " " * first_statement.col_offset)
    ]
    generated = "".join(lines)
    ast.parse(generated)
    return generated


def build_experiment(notebook: dict) -> dict:
    """Return a clean standalone experiment derived from the production notebook."""
    experiment = copy.deepcopy(notebook)
    found = set()
    for cell in experiment["cells"]:
        source = "".join(cell["source"])
        if cell["cell_type"] == "code":
            if "CUSTOMER SETTINGS" in source:
                found.add("settings")
                replacements = {
                    "KINDS": KINDS,
                    "TABLE_PREFIX": "parallelism_exp_p1_",
                    "WRITE_MODE": "full_refresh",
                    "OUTPUT_WRITE_PARALLELISM": 1,
                }
                replaced = set()
                for node in reversed(ast.parse(source).body):
                    if isinstance(node, ast.Assign) and len(node.targets) == 1:
                        name = ast.unparse(node.targets[0])
                        if name in replacements:
                            if name in replaced:
                                raise ValueError(f"Duplicate experiment setting: {name}")
                            replaced.add(name)
                            lines = source.splitlines(keepends=True)
                            lines[node.lineno - 1:node.end_lineno] = [f"{name} = {replacements[name]!r}\n"]
                            source = "".join(lines)
                if replaced != set(replacements):
                    raise ValueError(f"Missing experiment settings: {sorted(set(replacements) - replaced)}")
                source += "\nGROUP_PROCESSING_PARALLELISM = 1  # Compare 1 with 2; keep other settings fixed.\n"
            elif "FABRIC RUNTIME RESOLUTION" in source:
                found.add("runtime")
                anchor = 'output_write_parallelism, fabric_sku_size = _resolve_output_write_parallelism('
                source = _replace_once(source, anchor,
                    'group_processing_parallelism = max(1, int(os.environ.get("ADME_GROUP_PROCESSING_PARALLELISM", GROUP_PROCESSING_PARALLELISM)))\n'
                    + anchor)
                anchor = '    "output_write_parallelism": output_write_parallelism,'
                source = _replace_once(source, anchor, anchor + '\n    "group_processing_parallelism": group_processing_parallelism,')
                source += '\nprint(f"  Group processing parallelism: {group_processing_parallelism}")\n'
            elif "def run_silver_build(" in source:
                found.add("build")
                source = concurrent_build(source)
            compile(source, "<parallelism-experiment>", "exec")
            cell["outputs"] = []
            cell["execution_count"] = None
        elif source.startswith("# ADME ACZ Silver Layer\n"):
            source += (
                "\n**Opt-in group-concurrency experiment.** Generated from the main notebook by "
                "`scripts/sync_parallelism_experiment.py`; edit that generator or the main notebook, not this copy. "
                "Compare `GROUP_PROCESSING_PARALLELISM = 1` and `2` with identical inputs and resources. "
                "Use distinct output prefixes; full refresh, schema preflight, batched metadata and one write per group are required. "
                "Metadata tables and schema caches are shared: run trials sequentially in a dedicated test lakehouse. "
                "This experiment is not a production scheduling recommendation.\n"
            )
        cell["source"] = source.splitlines(keepends=True)
    if found != {"settings", "runtime", "build"}:
        raise ValueError(f"Missing experiment source cells: {sorted({'settings', 'runtime', 'build'} - found)}")
    return experiment


def main() -> int:
    """Regenerate the notebook, or fail when its checked-in projection is stale."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check the committed experiment without writing")
    args = parser.parse_args()
    try:
        rendered = json.dumps(build_experiment(json.loads(MAIN.read_text(encoding="utf-8"))), indent=1, ensure_ascii=False) + "\n"
        if args.check:
            if EXPERIMENT.read_text(encoding="utf-8") != rendered:
                raise ValueError("Experiment notebook is stale; run python scripts/sync_parallelism_experiment.py")
        else:
            EXPERIMENT.write_text(rendered, encoding="utf-8")
    except (OSError, ValueError, SyntaxError) as exc:
        parser.exit(1, f"error: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
