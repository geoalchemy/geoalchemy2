from __future__ import annotations

import json
from pathlib import Path
from xml.sax.saxutils import quoteattr

import pytest

from tools import compare_benchmarks


def test_filter_run_by_junitxml_keeps_only_passed_benchmarks(tmp_path: Path) -> None:
    passed = _nodeid("test_insert[passed]")
    failed = _nodeid("test_insert[failed]")
    errored = _nodeid("test_insert[errored]")
    skipped = _nodeid("test_insert[skipped]")
    missing = _nodeid("test_insert[missing]")
    run = _run(tmp_path, "branch", [passed, failed, errored, skipped, missing])
    junitxml = _junitxml(
        tmp_path,
        "branch",
        [
            ("tests.benchmarks.test_insert_select", "test_insert[passed]", "passed"),
            ("tests.benchmarks.test_insert_select", "test_insert[failed]", "failed"),
            ("tests.benchmarks.test_insert_select", "test_insert[errored]", "error"),
            ("tests.benchmarks.test_insert_select", "test_insert[skipped]", "skipped"),
        ],
    )

    filtered = compare_benchmarks.filter_run_by_junitxml(run, junitxml)

    assert set(filtered.benchmarks) == {passed}
    assert [benchmark["fullname"] for benchmark in filtered.data["benchmarks"]] == [passed]
    assert filtered.outcome_filter == {
        "junitxml": str(junitxml),
        "kept": 1,
        "excluded": {
            "failed": 1,
            "error": 1,
            "skipped": 1,
            "missing-outcome": 1,
        },
    }


def test_filter_run_by_junitxml_maps_class_based_nodeids(tmp_path: Path) -> None:
    nodeid = "tests/test_elements.py::TestDynamicElements::test_create_elements[WKTElement]"
    run = _run(tmp_path, "branch", [nodeid])
    junitxml = _junitxml(
        tmp_path,
        "branch",
        [
            (
                "tests.test_elements.TestDynamicElements",
                "test_create_elements[WKTElement]",
                "passed",
            ),
        ],
    )

    filtered = compare_benchmarks.filter_run_by_junitxml(run, junitxml)

    assert set(filtered.benchmarks) == {nodeid}


def test_main_filters_before_comparison(tmp_path: Path) -> None:
    passed = _nodeid("test_insert[passed]")
    failed = _nodeid("test_insert[failed]")
    base_json = _benchmark_json(tmp_path, "base", {passed: 1.0, failed: 2.0})
    compare_json = _benchmark_json(tmp_path, "compare", {passed: 1.5, failed: 0.1})
    base_xml = _junitxml(
        tmp_path,
        "base",
        [
            ("tests.benchmarks.test_insert_select", "test_insert[passed]", "passed"),
            ("tests.benchmarks.test_insert_select", "test_insert[failed]", "passed"),
        ],
    )
    compare_xml = _junitxml(
        tmp_path,
        "compare",
        [
            ("tests.benchmarks.test_insert_select", "test_insert[passed]", "passed"),
            ("tests.benchmarks.test_insert_select", "test_insert[failed]", "failed"),
        ],
    )
    output = tmp_path / "comparison"

    result = compare_benchmarks.main(
        [
            "--base",
            str(base_json),
            "--compare",
            str(compare_json),
            "--base-junitxml",
            str(base_xml),
            "--compare-junitxml",
            str(compare_xml),
            "--output",
            str(output),
            "--no-charts",
        ]
    )

    comparison = json.loads((output / "comparison.json").read_text(encoding="utf-8"))
    assert result == 0
    assert [benchmark["name"] for benchmark in comparison["benchmarks"]] == [passed]
    assert comparison["benchmarks"][0]["status"] == "slower"
    assert comparison["compare"]["outcome_filter"]["excluded"]["failed"] == 1


def test_main_warns_when_outcome_filtering_is_disabled(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    nodeid = _nodeid("test_insert[passed]")
    base_json = _benchmark_json(tmp_path, "base", {nodeid: 1.0})
    compare_json = _benchmark_json(tmp_path, "compare", {nodeid: 1.5})

    result = compare_benchmarks.main(
        [
            "--base",
            str(base_json),
            "--compare",
            str(compare_json),
            "--output",
            str(tmp_path / "comparison"),
            "--no-charts",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert "benchmark outcome filtering is disabled" in captured.err


def test_parse_args_requires_both_junitxml_paths(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc_info:
        compare_benchmarks.parse_args(
            [
                "--base",
                "base",
                "--compare",
                "compare",
                "--base-junitxml",
                str(tmp_path / "base.xml"),
                "--output",
                str(tmp_path / "comparison"),
            ]
        )

    assert exc_info.value.code == 2


def _nodeid(name: str) -> str:
    return f"tests/benchmarks/test_insert_select.py::{name}"


def _run(tmp_path: Path, label: str, nodeids: list[str]) -> compare_benchmarks.BenchmarkRun:
    path = _benchmark_json(tmp_path, label, dict.fromkeys(nodeids, 1.0))
    return compare_benchmarks.load_run(label, path)


def _benchmark_json(tmp_path: Path, label: str, values: dict[str, float]) -> Path:
    path = tmp_path / f"{label}.json"
    path.write_text(
        json.dumps(
            {
                "benchmarks": [
                    {"fullname": nodeid, "stats": {"mean": value}}
                    for nodeid, value in values.items()
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def _junitxml(tmp_path: Path, label: str, cases: list[tuple[str, str, str]]) -> Path:
    path = tmp_path / f"{label}.xml"
    testcases = [_testcase_xml(classname, name, outcome) for classname, name, outcome in cases]
    path.write_text(
        f"<testsuites><testsuite>{''.join(testcases)}</testsuite></testsuites>",
        encoding="utf-8",
    )
    return path


def _testcase_xml(classname: str, name: str, outcome: str) -> str:
    children = {
        "passed": "",
        "failed": "<failure />",
        "error": "<error />",
        "skipped": "<skipped />",
    }[outcome]
    return (
        f"<testcase classname={quoteattr(classname)} name={quoteattr(name)}>{children}</testcase>"
    )
