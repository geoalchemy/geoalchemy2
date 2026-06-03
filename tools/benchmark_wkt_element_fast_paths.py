"""Benchmark WKTElement WKT-only conversion paths.

Run from the GeoAlchemy2 source tree with a tox environment that has the
branch dependencies installed, for example:

    ./test_container/run.sh /output/py312-sqlalatest/bin/python \
        /geoalchemy2/tools/benchmark_wkt_element_fast_paths.py

The "rust_path" functions model the converter-based implementation. The
"regex_current" functions model the current branch implementation. The
"partition_candidate" functions model a faster string-only alternative for
EWKT prefix stripping.
"""

from __future__ import annotations

import argparse
import timeit
from collections.abc import Callable
from statistics import median

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKTElement


def strip_srid_partition(data: str) -> str:
    wkt = data.partition(";")[2]
    if wkt.startswith(" "):
        wkt = wkt[1:]
    return wkt


def bench(
    label: str,
    current: Callable[[], WKTElement],
    rust_path: Callable[[], WKTElement],
    *,
    number: int,
    repeat: int,
) -> None:
    current_value = current()
    rust_value = rust_path()
    assert current_value.data == rust_value.data, (label, current_value.data, rust_value.data)
    assert current_value.srid == rust_value.srid, (label, current_value.srid, rust_value.srid)
    assert current_value.extended == rust_value.extended, (
        label,
        current_value.extended,
        rust_value.extended,
    )

    current_times = timeit.repeat(current, repeat=repeat, number=number)
    rust_times = timeit.repeat(rust_path, repeat=repeat, number=number)
    current_med = median(current_times) / number * 1_000_000
    rust_med = median(rust_times) / number * 1_000_000
    print(
        f"{label}: current={current_med:.3f} us/op "
        f"rust_path={rust_med:.3f} us/op speedup={rust_med / current_med:.2f}x"
    )


def bench_one(
    label: str,
    func: Callable[[], WKTElement],
    *,
    number: int,
    repeat: int,
) -> None:
    times = timeit.repeat(func, repeat=repeat, number=number)
    func()
    print(f"{label}: {median(times) / number * 1_000_000:.3f} us/op")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--number", type=int, default=200_000)
    parser.add_argument("--repeat", type=int, default=9)
    args = parser.parse_args()

    plain = WKTElement("POINT (1 2)", srid=4326)
    ewkt = WKTElement("SRID=4326;POINT (1 2)", extended=True)
    ewkt_space = WKTElement("SRID=4326; POINT (1 2)", extended=True)

    def regex_as_wkt_plain() -> WKTElement:
        return plain.as_wkt()

    def rust_as_wkt_plain() -> WKTElement:
        return WKTElement(_wkb_wkt.to_wkt_no_srid(plain.data), srid=plain.srid, extended=False)

    def regex_as_wkt_extended() -> WKTElement:
        return ewkt.as_wkt()

    def rust_as_wkt_extended() -> WKTElement:
        return WKTElement(_wkb_wkt.to_wkt_no_srid(ewkt.data), srid=ewkt.srid, extended=False)

    def regex_as_ewkt_plain() -> WKTElement:
        return plain.as_ewkt()

    def rust_as_ewkt_plain() -> WKTElement:
        return WKTElement(_wkb_wkt.to_wkt(plain.data, srid=plain.srid), extended=True)

    def regex_as_ewkt_extended() -> WKTElement:
        return ewkt.as_ewkt()

    def rust_as_ewkt_extended() -> WKTElement:
        return WKTElement(_wkb_wkt.to_wkt(ewkt.data, srid=ewkt.srid), extended=True)

    def partition_as_wkt_extended() -> WKTElement:
        return WKTElement(strip_srid_partition(ewkt.data), ewkt.srid, extended=False)

    def partition_as_wkt_extended_space() -> WKTElement:
        return WKTElement(strip_srid_partition(ewkt_space.data), ewkt_space.srid, extended=False)

    def partition_as_ewkt_extended() -> WKTElement:
        return WKTElement(f"SRID={ewkt.srid};{strip_srid_partition(ewkt.data)}", extended=True)

    print("Current implementation versus converter-based Rust path")
    bench(
        "as_wkt plain WKT",
        regex_as_wkt_plain,
        rust_as_wkt_plain,
        number=args.number,
        repeat=args.repeat,
    )
    bench(
        "as_wkt EWKT strip SRID",
        regex_as_wkt_extended,
        rust_as_wkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench(
        "as_ewkt plain WKT add SRID",
        regex_as_ewkt_plain,
        rust_as_ewkt_plain,
        number=args.number,
        repeat=args.repeat,
    )
    bench(
        "as_ewkt EWKT normalize prefix",
        regex_as_ewkt_extended,
        rust_as_ewkt_extended,
        number=args.number,
        repeat=args.repeat,
    )

    print()
    print("EWKT prefix stripping alternatives")
    bench_one(
        "as_wkt EWKT regex current",
        regex_as_wkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_wkt EWKT partition candidate",
        partition_as_wkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_wkt EWKT partition with space candidate",
        partition_as_wkt_extended_space,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_wkt EWKT rust path",
        rust_as_wkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_ewkt EWKT regex current",
        regex_as_ewkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_ewkt EWKT partition candidate",
        partition_as_ewkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_ewkt EWKT rust path",
        rust_as_ewkt_extended,
        number=args.number,
        repeat=args.repeat,
    )


if __name__ == "__main__":
    main()
