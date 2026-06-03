"""Benchmark WKTElement WKT-only conversion paths.

Run from the GeoAlchemy2 source tree with a tox environment that has the
branch dependencies installed, for example:

    ./test_container/run.sh /output/py312-sqlalatest/bin/python \
        /geoalchemy2/tools/benchmark_wkt_element_fast_paths.py

The current implementation is hybrid: plain/non-extended WKT uses cheap Python
constructors, while already-extended EWKT uses the Rust-backed converter.
The "converter/Rust path" functions force converter use for comparison. The
"old regex" functions model the historical regex prefix stripper. The
"rejected strict partition candidate" functions keep a local string-only
candidate for comparison only.
"""

from __future__ import annotations

import argparse
import re
import timeit
from collections.abc import Callable
from statistics import median

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKTElement

OLD_REMOVE_SRID = re.compile("(SRID=([0-9]+); ?)?(.*)")


def strip_srid_old_regex(data: str) -> str:
    match = OLD_REMOVE_SRID.match(data)
    assert match is not None
    return match.group(3)


def strip_srid_rejected_strict_partition(data: str) -> str:
    if data[:5] == "SRID=":
        header, separator, wkt = data.partition(";")
        srid_text = header[5:]
        if separator and srid_text.isascii() and srid_text.isdigit() and wkt[:2] != "  ":
            if wkt[:1] == " ":
                return wkt[1:]
            return wkt
    return data


def bench(
    label: str,
    current: Callable[[], WKTElement],
    converter_path: Callable[[], WKTElement],
    *,
    number: int,
    repeat: int,
) -> None:
    current_value = current()
    converter_value = converter_path()
    assert current_value.data == converter_value.data, (
        label,
        current_value.data,
        converter_value.data,
    )
    assert current_value.srid == converter_value.srid, (
        label,
        current_value.srid,
        converter_value.srid,
    )
    assert current_value.extended == converter_value.extended, (
        label,
        current_value.extended,
        converter_value.extended,
    )

    current_times = timeit.repeat(current, repeat=repeat, number=number)
    converter_times = timeit.repeat(converter_path, repeat=repeat, number=number)
    current_med = median(current_times) / number * 1_000_000
    converter_med = median(converter_times) / number * 1_000_000
    print(
        f"{label}: current_hybrid={current_med:.3f} us/op "
        f"converter_rust={converter_med:.3f} us/op "
        f"converter/current={converter_med / current_med:.2f}x"
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

    def current_as_wkt_plain() -> WKTElement:
        return plain.as_wkt()

    def converter_as_wkt_plain() -> WKTElement:
        return WKTElement(_wkb_wkt.to_wkt_no_srid(plain.data), srid=plain.srid, extended=False)

    def current_as_wkt_extended() -> WKTElement:
        return ewkt.as_wkt()

    def converter_as_wkt_extended() -> WKTElement:
        return WKTElement(_wkb_wkt.to_wkt_no_srid(ewkt.data), srid=ewkt.srid, extended=False)

    def current_as_ewkt_plain() -> WKTElement:
        return plain.as_ewkt()

    def converter_as_ewkt_plain() -> WKTElement:
        return WKTElement(_wkb_wkt.to_wkt(plain.data, srid=plain.srid), extended=True)

    def current_as_ewkt_extended() -> WKTElement:
        return ewkt.as_ewkt()

    def converter_as_ewkt_extended() -> WKTElement:
        return WKTElement(_wkb_wkt.to_wkt(ewkt.data, srid=ewkt.srid), extended=True)

    def rejected_strict_partition_as_wkt_extended() -> WKTElement:
        return WKTElement(
            strip_srid_rejected_strict_partition(ewkt.data),
            ewkt.srid,
            extended=False,
        )

    def rejected_strict_partition_as_wkt_extended_space() -> WKTElement:
        return WKTElement(
            strip_srid_rejected_strict_partition(ewkt_space.data),
            ewkt_space.srid,
            extended=False,
        )

    def rejected_strict_partition_as_ewkt_extended() -> WKTElement:
        return WKTElement(
            f"SRID={ewkt.srid};{strip_srid_rejected_strict_partition(ewkt.data)}",
            extended=True,
        )

    print("Hybrid current implementation versus converter/Rust path")
    bench(
        "as_wkt plain WKT (current Python fast path)",
        current_as_wkt_plain,
        converter_as_wkt_plain,
        number=args.number,
        repeat=args.repeat,
    )
    bench(
        "as_wkt EWKT strip SRID (current Rust path)",
        current_as_wkt_extended,
        converter_as_wkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench(
        "as_ewkt plain WKT add SRID (current Python fast path)",
        current_as_ewkt_plain,
        converter_as_ewkt_plain,
        number=args.number,
        repeat=args.repeat,
    )
    bench(
        "as_ewkt EWKT normalize prefix (current Rust path)",
        current_as_ewkt_extended,
        converter_as_ewkt_extended,
        number=args.number,
        repeat=args.repeat,
    )

    print()
    print("Rejected EWKT string-only alternatives")

    def old_regex_as_wkt_extended() -> WKTElement:
        return WKTElement(strip_srid_old_regex(ewkt.data), ewkt.srid, extended=False)

    def old_regex_as_ewkt_extended() -> WKTElement:
        return WKTElement(f"SRID={ewkt.srid};{strip_srid_old_regex(ewkt.data)}", extended=True)

    assert strip_srid_rejected_strict_partition(ewkt.data) == strip_srid_old_regex(ewkt.data)
    assert strip_srid_rejected_strict_partition(ewkt_space.data) == strip_srid_old_regex(
        ewkt_space.data
    )
    assert (
        strip_srid_rejected_strict_partition("SRID=4326;  POINT (1 2)") == "SRID=4326;  POINT (1 2)"
    )

    bench_one(
        "as_wkt EWKT old regex",
        old_regex_as_wkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_wkt EWKT rejected strict partition candidate",
        rejected_strict_partition_as_wkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_wkt EWKT rejected strict partition candidate with space",
        rejected_strict_partition_as_wkt_extended_space,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_wkt EWKT converter/Rust path",
        converter_as_wkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_ewkt EWKT old regex",
        old_regex_as_ewkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_ewkt EWKT rejected strict partition candidate",
        rejected_strict_partition_as_ewkt_extended,
        number=args.number,
        repeat=args.repeat,
    )
    bench_one(
        "as_ewkt EWKT converter/Rust path",
        converter_as_ewkt_extended,
        number=args.number,
        repeat=args.repeat,
    )


if __name__ == "__main__":
    main()
