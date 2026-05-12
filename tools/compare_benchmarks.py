#!/usr/bin/env python3
"""Compare two pytest-benchmark saved runs."""

from __future__ import annotations

import argparse
import csv
import fnmatch
import json
import math
import re
import sys
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any

METRICS = ("min", "max", "mean", "median", "stddev", "iqr")
TIME_UNITS = {
    "s": 1.0,
    "ms": 1_000.0,
    "us": 1_000_000.0,
    "ns": 1_000_000_000.0,
}
SORT_KEYS = (
    "name",
    "base",
    "compare",
    "delta",
    "absolute-delta",
    "relative-delta",
    "absolute-relative-delta",
    "slowest",
    "status",
)


@dataclass(frozen=True)
class BenchmarkRun:
    label: str
    path: Path
    data: dict[str, Any]
    benchmarks: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class ComparisonRecord:
    name: str
    base_value: float | None
    compare_value: float | None
    absolute_delta: float | None
    relative_delta: float | None
    status: str
    base_benchmark: dict[str, Any] | None
    compare_benchmark: dict[str, Any] | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare two pytest-benchmark JSON runs. The --base and --compare values "
            "may be saved names such as 'main' or direct JSON file paths."
        )
    )
    parser.add_argument(
        "--storage",
        type=Path,
        default=Path(".benchmarks"),
        help="pytest-benchmark storage directory. Default: .benchmarks",
    )
    parser.add_argument(
        "--base",
        "--base-branch",
        dest="base",
        required=True,
        help="Base saved benchmark name or JSON file path.",
    )
    parser.add_argument(
        "--compare",
        "--compare-branch",
        dest="compare",
        required=True,
        help="Compare saved benchmark name or JSON file path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Directory where comparison exports and charts are written.",
    )
    parser.add_argument(
        "--metric",
        choices=METRICS,
        default="mean",
        help="Benchmark statistic to compare. Default: mean",
    )
    parser.add_argument(
        "--sort-by",
        choices=SORT_KEYS,
        default="relative-delta",
        help="Column used to sort the report. Default: relative-delta",
    )
    parser.add_argument(
        "--sort-order",
        choices=("asc", "desc"),
        default="desc",
        help="Sort order. Default: desc",
    )
    parser.add_argument(
        "--time-unit",
        choices=("auto", "s", "ms", "us", "ns"),
        default="auto",
        help="Display unit for times. Default: auto",
    )
    parser.add_argument(
        "--tolerance-percent",
        type=float,
        default=0.0,
        help="Treat changes within this relative percentage as similar. Default: 0",
    )
    parser.add_argument(
        "--histogram-bins",
        type=int,
        default=20,
        help="Number of bins for raw-data histograms. Default: 20",
    )
    parser.add_argument(
        "--fail-on-slower-percent",
        type=float,
        default=None,
        help="Exit with status 1 if any common benchmark is slower by more than this percentage.",
    )
    parser.add_argument(
        "--only-common",
        action="store_true",
        help="Only include benchmarks present in both runs.",
    )
    parser.add_argument(
        "--no-charts",
        action="store_true",
        help="Do not export SVG charts.",
    )
    return parser.parse_args(argv)


def find_run_file(storage: Path, label_or_path: str) -> Path:
    candidate = Path(label_or_path).expanduser()
    if candidate.is_file():
        return candidate.resolve()

    storage = storage.expanduser()
    if any(char in label_or_path for char in "*?[]"):
        matches = [
            path for path in storage.rglob("*.json") if fnmatch.fnmatch(path.name, label_or_path)
        ]
        matches.extend(path for path in storage.rglob(label_or_path) if path.is_file())
    else:
        matches = [
            path
            for path in storage.rglob("*.json")
            if _saved_name(path) == label_or_path or path.stem == label_or_path
        ]
        if not matches:
            matches = [
                path
                for path in storage.rglob("*.json")
                if label_or_path in path.stem or label_or_path in path.name
            ]

    matches = sorted(set(matches), key=_run_sort_key)
    if not matches:
        raise FileNotFoundError(
            f"No pytest-benchmark JSON file matching {label_or_path!r} under {storage}"
        )
    return matches[-1].resolve()


def load_run(label: str, path: Path) -> BenchmarkRun:
    data = json.loads(path.read_text(encoding="utf-8"))
    benchmarks = {benchmark["fullname"]: benchmark for benchmark in data.get("benchmarks", [])}
    return BenchmarkRun(label=label, path=path, data=data, benchmarks=benchmarks)


def compare_runs(
    base: BenchmarkRun,
    compare: BenchmarkRun,
    *,
    metric: str,
    tolerance_percent: float,
    only_common: bool = False,
) -> list[ComparisonRecord]:
    names = set(base.benchmarks) & set(compare.benchmarks)
    if not only_common:
        names |= set(base.benchmarks) | set(compare.benchmarks)

    records = []
    for name in sorted(names):
        base_benchmark = base.benchmarks.get(name)
        compare_benchmark = compare.benchmarks.get(name)
        base_value = _metric_value(base_benchmark, metric)
        compare_value = _metric_value(compare_benchmark, metric)
        absolute_delta = None
        relative_delta = None
        if base_value is not None and compare_value is not None:
            absolute_delta = compare_value - base_value
            relative_delta = math.inf if base_value == 0 else absolute_delta / base_value * 100
        status = _status(base_value, compare_value, relative_delta, tolerance_percent)
        records.append(
            ComparisonRecord(
                name=name,
                base_value=base_value,
                compare_value=compare_value,
                absolute_delta=absolute_delta,
                relative_delta=relative_delta,
                status=status,
                base_benchmark=base_benchmark,
                compare_benchmark=compare_benchmark,
            )
        )
    return records


def sort_records(
    records: list[ComparisonRecord], *, sort_by: str, sort_order: str
) -> list[ComparisonRecord]:
    reverse = sort_order == "desc"

    if sort_by in {"name", "status"}:
        return sorted(records, key=lambda record: getattr(record, sort_by), reverse=reverse)

    present: list[tuple[float, ComparisonRecord]] = []
    missing: list[ComparisonRecord] = []
    for record in records:
        value = _sort_value(record, sort_by)
        if value is None:
            missing.append(record)
        else:
            present.append((value, record))

    sorted_present = [
        record for _value, record in sorted(present, key=lambda item: item[0], reverse=reverse)
    ]
    return sorted_present + sorted(missing, key=lambda record: record.name)


def _sort_value(record: ComparisonRecord, sort_by: str) -> float | None:
    if sort_by == "name":
        return None
    if sort_by == "base":
        return record.base_value
    if sort_by == "compare":
        return record.compare_value
    if sort_by == "delta":
        return record.absolute_delta
    if sort_by == "absolute-delta":
        return None if record.absolute_delta is None else abs(record.absolute_delta)
    if sort_by == "relative-delta":
        return record.relative_delta
    if sort_by == "absolute-relative-delta":
        return None if record.relative_delta is None else abs(record.relative_delta)
    if sort_by == "slowest":
        values = [value for value in (record.base_value, record.compare_value) if value is not None]
        return max(values) if values else None
    if sort_by == "status":
        return None
    raise ValueError(f"Unsupported sort key: {sort_by}")


def export_reports(
    *,
    output: Path,
    base: BenchmarkRun,
    compare: BenchmarkRun,
    records: list[ComparisonRecord],
    metric: str,
    time_unit: str,
    chart_bins: int,
    export_charts: bool,
) -> list[str]:
    output.mkdir(parents=True, exist_ok=True)
    unit = resolve_time_unit(records, time_unit)
    exported = []

    csv_path = output / "comparison.csv"
    export_csv(csv_path, records, metric)
    exported.append(str(csv_path))

    json_path = output / "comparison.json"
    export_json(json_path, base, compare, records, metric)
    exported.append(str(json_path))

    markdown_path = output / "comparison.md"
    export_markdown(markdown_path, base, compare, records, metric, unit)
    exported.append(str(markdown_path))

    if export_charts:
        charts_dir = output / "charts"
        charts_dir.mkdir(parents=True, exist_ok=True)
        for index, record in enumerate(records, start=1):
            if record.base_value is None or record.compare_value is None:
                continue
            chart_path = charts_dir / f"{index:04}_{safe_filename(short_name(record.name))}.svg"
            export_chart(
                chart_path,
                record,
                base_label=base.label,
                compare_label=compare.label,
                metric=metric,
                unit=unit,
                bins=chart_bins,
            )
            exported.append(str(chart_path))

    return exported


def export_csv(path: Path, records: list[ComparisonRecord], metric: str) -> None:
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        fieldnames = list(_row(records[0], metric)) if records else []
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not records:
            return
        writer.writeheader()
        for record in records:
            writer.writerow(_row(record, metric))


def export_json(
    path: Path,
    base: BenchmarkRun,
    compare: BenchmarkRun,
    records: list[ComparisonRecord],
    metric: str,
) -> None:
    payload = {
        "metric": metric,
        "base": _run_summary(base),
        "compare": _run_summary(compare),
        "benchmarks": [_row(record, metric) for record in records],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def export_markdown(
    path: Path,
    base: BenchmarkRun,
    compare: BenchmarkRun,
    records: list[ComparisonRecord],
    metric: str,
    unit: str,
) -> None:
    lines = [
        f"# Benchmark comparison: {base.label} vs {compare.label}",
        "",
        f"- Base: `{base.path}`",
        f"- Compare: `{compare.path}`",
        f"- Metric: `{metric}`",
        "",
    ]
    lines.extend(format_table(records, base.label, compare.label, metric, unit, markdown=True))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def export_chart(
    path: Path,
    record: ComparisonRecord,
    *,
    base_label: str,
    compare_label: str,
    metric: str,
    unit: str,
    bins: int,
) -> None:
    base_data = _sample_data(record.base_benchmark)
    compare_data = _sample_data(record.compare_benchmark)
    if base_data and compare_data:
        svg = render_histogram_svg(
            record,
            base_data,
            compare_data,
            base_label=base_label,
            compare_label=compare_label,
            unit=unit,
            bins=bins,
        )
    else:
        svg = render_bar_svg(
            record,
            base_label=base_label,
            compare_label=compare_label,
            metric=metric,
            unit=unit,
        )
    path.write_text(svg, encoding="utf-8")


def render_histogram_svg(
    record: ComparisonRecord,
    base_data: list[float],
    compare_data: list[float],
    *,
    base_label: str,
    compare_label: str,
    unit: str,
    bins: int,
) -> str:
    scale = TIME_UNITS[unit]
    base_values = [value * scale for value in base_data]
    compare_values = [value * scale for value in compare_data]
    all_values = base_values + compare_values
    bin_edges = make_bins(all_values, max(1, bins))
    base_counts = histogram_counts(base_values, bin_edges)
    compare_counts = histogram_counts(compare_values, bin_edges)
    max_count = max(base_counts + compare_counts + [1])

    width = 960
    height = 420
    panel_width = 380
    panel_height = 230
    top = 80
    left_x = 60
    right_x = 520
    title = escape(short_name(record.name))

    parts = [_svg_header(width, height)]
    parts.append(f'<text x="30" y="34" class="title">{title}</text>')
    parts.append(
        f'<text x="30" y="58" class="subtitle">raw sample histogram, time in {unit}</text>'
    )
    parts.extend(
        _histogram_panel(
            left_x,
            top,
            panel_width,
            panel_height,
            base_counts,
            bin_edges,
            max_count,
            base_label,
            "#2f6fed",
        )
    )
    parts.extend(
        _histogram_panel(
            right_x,
            top,
            panel_width,
            panel_height,
            compare_counts,
            bin_edges,
            max_count,
            compare_label,
            "#c44e52",
        )
    )
    parts.append(
        f'<text x="{left_x}" y="{height - 38}" class="foot">delta: '
        f"{format_time(record.absolute_delta, unit)} ({format_percent(record.relative_delta)}) "
        f"- compare is {escape(record.status)}</text>"
    )
    parts.append("</svg>")
    return "\n".join(parts)


def render_bar_svg(
    record: ComparisonRecord,
    *,
    base_label: str,
    compare_label: str,
    metric: str,
    unit: str,
) -> str:
    scale = TIME_UNITS[unit]
    base_value = (record.base_value or 0) * scale
    compare_value = (record.compare_value or 0) * scale
    max_value = max(base_value, compare_value, 1e-12)
    width = 760
    height = 360
    top = 82
    chart_height = 190
    baseline = top + chart_height
    bar_width = 120
    base_height = chart_height * base_value / max_value
    compare_height = chart_height * compare_value / max_value
    title = escape(short_name(record.name))

    parts = [_svg_header(width, height)]
    parts.append(f'<text x="30" y="34" class="title">{title}</text>')
    parts.append(
        f'<text x="30" y="58" class="subtitle">no raw samples saved; '
        f"showing {escape(metric)} in {unit}</text>"
    )
    parts.append(f'<line x1="70" y1="{baseline}" x2="690" y2="{baseline}" class="axis" />')
    parts.extend(
        _bar(
            210,
            baseline,
            bar_width,
            base_height,
            "#2f6fed",
            base_label,
            format_time(record.base_value, unit),
        )
    )
    parts.extend(
        _bar(
            430,
            baseline,
            bar_width,
            compare_height,
            "#c44e52",
            compare_label,
            format_time(record.compare_value, unit),
        )
    )
    parts.append(
        f'<text x="70" y="{height - 34}" class="foot">delta: '
        f"{format_time(record.absolute_delta, unit)} ({format_percent(record.relative_delta)}) "
        f"- compare is {escape(record.status)}</text>"
    )
    parts.append("</svg>")
    return "\n".join(parts)


def format_table(
    records: list[ComparisonRecord],
    base_label: str,
    compare_label: str,
    metric: str,
    unit: str,
    *,
    markdown: bool = False,
) -> list[str]:
    headers = [
        "benchmark",
        f"{base_label} {metric}",
        f"{compare_label} {metric}",
        f"delta ({unit})",
        "delta (%)",
        "status",
    ]
    rows = [
        [
            record.name,
            format_time(record.base_value, unit),
            format_time(record.compare_value, unit),
            format_time(record.absolute_delta, unit, signed=True),
            format_percent(record.relative_delta, signed=True),
            record.status,
        ]
        for record in records
    ]
    if markdown:
        return _markdown_table(headers, rows)
    return _plain_table(headers, rows)


def resolve_time_unit(records: list[ComparisonRecord], requested: str) -> str:
    if requested != "auto":
        return requested
    values = [
        value
        for record in records
        for value in (record.base_value, record.compare_value, record.absolute_delta)
        if value is not None and math.isfinite(value)
    ]
    max_value = max((abs(value) for value in values), default=1.0)
    if max_value < 1e-6:
        return "ns"
    if max_value < 1e-3:
        return "us"
    if max_value < 1:
        return "ms"
    return "s"


def format_time(value: float | None, unit: str, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    if math.isinf(value):
        return "inf"
    scaled = value * TIME_UNITS[unit]
    sign = "+" if signed and scaled > 0 else ""
    return f"{sign}{scaled:.6g}"


def format_percent(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    if math.isinf(value):
        return "inf"
    sign = "+" if signed and value > 0 else ""
    return f"{sign}{value:.2f}%"


def short_name(name: str) -> str:
    return name.rsplit("/", 1)[-1]


def safe_filename(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.=-]+", "_", value)
    return value.strip("._")[:160] or "benchmark"


def make_bins(values: list[float], count: int) -> list[float]:
    lower = min(values)
    upper = max(values)
    if lower == upper:
        padding = abs(lower) * 0.05 or 1.0
        lower -= padding
        upper += padding
    step = (upper - lower) / count
    return [lower + step * index for index in range(count + 1)]


def histogram_counts(values: list[float], bin_edges: list[float]) -> list[int]:
    counts = [0 for _ in range(len(bin_edges) - 1)]
    for value in values:
        if value == bin_edges[-1]:
            counts[-1] += 1
            continue
        for index, (lower, upper) in enumerate(zip(bin_edges, bin_edges[1:], strict=False)):
            if lower <= value < upper:
                counts[index] += 1
                break
    return counts


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    base_path = find_run_file(args.storage, args.base)
    compare_path = find_run_file(args.storage, args.compare)
    base = load_run(args.base, base_path)
    compare = load_run(args.compare, compare_path)
    records = compare_runs(
        base,
        compare,
        metric=args.metric,
        tolerance_percent=args.tolerance_percent,
        only_common=args.only_common,
    )
    records = sort_records(records, sort_by=args.sort_by, sort_order=args.sort_order)
    exported = export_reports(
        output=args.output,
        base=base,
        compare=compare,
        records=records,
        metric=args.metric,
        time_unit=args.time_unit,
        chart_bins=args.histogram_bins,
        export_charts=not args.no_charts,
    )
    unit = resolve_time_unit(records, args.time_unit)
    print(f"Base:    {base.path}")
    print(f"Compare: {compare.path}")
    print(f"Metric:  {args.metric}")
    print()
    print("\n".join(format_table(records, base.label, compare.label, args.metric, unit)))
    print()
    print("Exported:")
    for path in exported:
        print(f"  {path}")

    if args.fail_on_slower_percent is not None:
        failures = [
            record
            for record in records
            if record.relative_delta is not None
            and record.relative_delta > args.fail_on_slower_percent
        ]
        if failures:
            print(
                f"\n{len(failures)} benchmark(s) are slower by more than "
                f"{args.fail_on_slower_percent:.2f}%.",
                file=sys.stderr,
            )
            return 1
    return 0


def _saved_name(path: Path) -> str:
    stem = path.stem
    if "_" not in stem:
        return stem
    _counter, saved_name = stem.split("_", 1)
    return saved_name


def _run_sort_key(path: Path) -> tuple[int, float, str]:
    counter = path.stem.split("_", 1)[0]
    try:
        counter_value = int(counter)
    except ValueError:
        counter_value = -1
    return counter_value, path.stat().st_mtime, str(path)


def _metric_value(benchmark: dict[str, Any] | None, metric: str) -> float | None:
    if benchmark is None:
        return None
    value = benchmark.get("stats", {}).get(metric)
    return float(value) if value is not None else None


def _sample_data(benchmark: dict[str, Any] | None) -> list[float]:
    if benchmark is None:
        return []
    data = benchmark.get("stats", {}).get("data") or []
    return [float(value) for value in data]


def _status(
    base_value: float | None,
    compare_value: float | None,
    relative_delta: float | None,
    tolerance_percent: float,
) -> str:
    if base_value is None:
        return "missing-base"
    if compare_value is None:
        return "missing-compare"
    if relative_delta is not None and abs(relative_delta) <= tolerance_percent:
        return "similar"
    if compare_value < base_value:
        return "faster"
    if compare_value > base_value:
        return "slower"
    return "similar"


def _row(record: ComparisonRecord, metric: str) -> dict[str, Any]:
    return {
        "name": record.name,
        "metric": metric,
        "base_seconds": record.base_value,
        "compare_seconds": record.compare_value,
        "absolute_delta_seconds": record.absolute_delta,
        "relative_delta_percent": record.relative_delta,
        "status": record.status,
    }


def _run_summary(run: BenchmarkRun) -> dict[str, Any]:
    return {
        "label": run.label,
        "path": str(run.path),
        "commit_info": run.data.get("commit_info", {}),
        "machine_info": run.data.get("machine_info", {}),
        "benchmark_count": len(run.benchmarks),
    }


def _plain_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    widths = [
        max(len(str(value)) for value in [header] + [row[index] for row in rows])
        for index, header in enumerate(headers)
    ]
    lines = [
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
        "  ".join("-" * width for width in widths),
    ]
    for row in rows:
        lines.append("  ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))
    return lines


def _markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value).replace("|", "\\|") for value in row) + " |")
    return lines


def _svg_header(width: int, height: int) -> str:
    svg_open = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">'
    )
    return f"""{svg_open}
<style>
  text {{ font-family: Arial, sans-serif; fill: #222; }}
  .title {{ font-size: 18px; font-weight: 700; }}
  .subtitle {{ font-size: 12px; fill: #555; }}
  .label {{ font-size: 13px; font-weight: 700; }}
  .tick {{ font-size: 11px; fill: #555; }}
  .foot {{ font-size: 12px; fill: #333; }}
  .axis {{ stroke: #777; stroke-width: 1; }}
  .grid {{ stroke: #ddd; stroke-width: 1; }}
</style>"""


def _histogram_panel(
    x: int,
    y: int,
    width: int,
    height: int,
    counts: list[int],
    bin_edges: list[float],
    max_count: int,
    label: str,
    color: str,
) -> list[str]:
    bar_gap = 2
    bar_width = max(1, (width - bar_gap * (len(counts) - 1)) / len(counts))
    bottom = y + height
    parts = [
        f'<text x="{x}" y="{y - 16}" class="label">{escape(label)}</text>',
        f'<line x1="{x}" y1="{bottom}" x2="{x + width}" y2="{bottom}" class="axis" />',
        f'<line x1="{x}" y1="{y}" x2="{x}" y2="{bottom}" class="axis" />',
    ]
    for index, count in enumerate(counts):
        bar_height = height * count / max_count
        bar_x = x + index * (bar_width + bar_gap)
        bar_y = bottom - bar_height
        parts.append(
            f'<rect x="{bar_x:.2f}" y="{bar_y:.2f}" width="{bar_width:.2f}" '
            f'height="{bar_height:.2f}" fill="{color}" opacity="0.82" />'
        )
    parts.append(f'<text x="{x}" y="{bottom + 18}" class="tick">{bin_edges[0]:.4g}</text>')
    parts.append(
        f'<text x="{x + width}" y="{bottom + 18}" text-anchor="end" class="tick">'
        f"{bin_edges[-1]:.4g}</text>"
    )
    parts.append(
        f'<text x="{x + width}" y="{y + 12}" text-anchor="end" class="tick">n={sum(counts)}</text>'
    )
    return parts


def _bar(
    x: int,
    baseline: int,
    width: int,
    height: float,
    color: str,
    label: str,
    value: str,
) -> list[str]:
    center = x + width / 2
    return [
        f'<rect x="{x}" y="{baseline - height:.2f}" width="{width}" height="{height:.2f}" '
        f'fill="{color}" opacity="0.86" />',
        f'<text x="{center}" y="{baseline + 24}" text-anchor="middle" '
        f'class="label">{escape(label)}</text>',
        f'<text x="{center}" y="{baseline - height - 8:.2f}" text-anchor="middle" '
        f'class="tick">{escape(value)}</text>',
    ]


if __name__ == "__main__":
    raise SystemExit(main())
