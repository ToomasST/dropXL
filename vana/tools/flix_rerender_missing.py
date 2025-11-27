#!/usr/bin/env python3
"""Re-render Flix fallback HTML for products missing description HTML.

This helper inspects the consolidated `1_samm_algandmed.json` payload,
rebuilds the Flix description HTML for products that previously failed,
and reports how many can now be recovered using the universal module
builder.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_PATH_DEFAULT = ROOT / "data" / "1_samm_algandmed.json"
REPORT_PATH_DEFAULT = ROOT / "data" / "processed" / "flix_rerender_report.json"

# Ensure project root is on sys.path so imports inside stage-1 module work reliably
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_alg_module() -> Any:
    module_path = ROOT / "1_Samm_alg_andmete_kogumine.py"
    spec = importlib.util.spec_from_file_location("prenta_stage1", module_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _iter_missing_products(products: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for product in products:
        html = str(product.get("flix_description_html") or "").strip()
        if html:
            continue
        yield product


def _normalise_root(tjson: Any) -> Optional[Dict[str, Any]]:
    if isinstance(tjson, dict):
        return tjson
    if isinstance(tjson, list) and tjson:
        first = tjson[0]
        if isinstance(first, dict):
            return first
    return None


def _collect_modules(modules_root: Any) -> List[Dict[str, Any]]:
    modules: List[Dict[str, Any]] = []
    if isinstance(modules_root, dict):
        raw_features = modules_root.get("features")
        if isinstance(raw_features, list):
            modules.extend([m for m in raw_features if isinstance(m, dict)])
        for value in modules_root.values():
            if isinstance(value, dict):
                modules.append(value)
            elif isinstance(value, list):
                modules.extend([m for m in value if isinstance(m, dict)])
    elif isinstance(modules_root, list):
        modules.extend([m for m in modules_root if isinstance(m, dict)])
    return modules


def rerender_missing(
    payload: Dict[str, Any],
    referer: str,
    timeout: int,
    limit: Optional[int],
    dump_dir: Optional[Path],
) -> Dict[str, Any]:
    module = _load_alg_module()
    build_from_modules = getattr(module, "_build_features_html_from_modules")
    process_features = getattr(module, "_process_flix_features_html")
    build_from_hotspot = getattr(module, "_build_features_html_from_key_features_html", None)
    probe_build_features = getattr(module, "probe_build_features", None)

    total_missing = 0
    recovered = 0
    outcomes: List[Dict[str, Any]] = []
    status_counter: Counter[str] = Counter()

    dump_dir_path: Optional[Path] = None
    if dump_dir:
        dump_dir_path = dump_dir
        dump_dir_path.mkdir(parents=True, exist_ok=True)

    for product in _iter_missing_products(payload.get("products", [])):
        if limit is not None and total_missing >= limit:
            break
        total_missing += 1

        pid = product.get("product_id")
        tjson_root = _normalise_root(product.get("flix_tjson"))
        if not tjson_root:
            status_counter["no_tjson"] += 1
            outcomes.append({"product_id": pid, "status": "no_tjson"})
            continue

        modules_root = tjson_root.get("modules")
        key_features_html: Optional[str] = None
        if isinstance(modules_root, dict):
            key_features_html = (
                ((modules_root.get("hotspot") or {}).get("key_features") or {}).get("html")
            )
        if isinstance(key_features_html, str) and key_features_html.strip():
            if probe_build_features is not None:
                try:
                    features_html = probe_build_features(key_features_html)
                except Exception:
                    features_html = None
            else:
                features_html = build_from_hotspot(key_features_html) if build_from_hotspot else None
        else:
            features_html = None

        if not features_html:
            modules_to_parse = _collect_modules(modules_root)
            if not modules_to_parse:
                status_counter["no_modules"] += 1
                outcomes.append({"product_id": pid, "status": "no_modules"})
                continue
            try:
                features_html = build_from_modules(modules_to_parse)
            except Exception as exc:  # pragma: no cover - defensive
                status_counter["build_error"] += 1
                outcomes.append({"product_id": pid, "status": "build_error", "error": str(exc)})
                continue

        if not features_html:
            status_counter["no_features_html"] += 1
            outcomes.append({"product_id": pid, "status": "no_features_html"})
            continue

        try:
            processed_html = process_features(features_html, pid, referer, timeout)
        except Exception as exc:  # pragma: no cover - defensive
            status_counter["process_error"] += 1
            outcomes.append({"product_id": pid, "status": "process_error", "error": str(exc)})
            continue

        processed_html = (processed_html or "").strip()
        if not processed_html:
            status_counter["empty_result"] += 1
            outcomes.append({"product_id": pid, "status": "empty_result"})
            continue

        recovered += 1
        status_counter["recovered"] += 1
        record: Dict[str, Any] = {
            "product_id": pid,
            "status": "recovered",
        }
        if dump_dir_path:
            out_file = dump_dir_path / f"{pid}.html"
            out_file.write_text(processed_html, encoding="utf-8")
            record["html_file"] = str(out_file.relative_to(ROOT))
        outcomes.append(record)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_file": str(DATA_PATH_DEFAULT),
        "referer": referer,
        "timeout": timeout,
        "attempted": total_missing,
        "recovered": recovered,
        "status_counts": dict(status_counter),
        "products": outcomes,
    }
    return report


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Re-render missing Flix HTML using the universal builder")
    parser.add_argument("--input", default=str(DATA_PATH_DEFAULT), help="Path to 1_samm_algandmed.json (default: %(default)s)")
    parser.add_argument("--output", default=str(REPORT_PATH_DEFAULT), help="Where to save the JSON report (default: %(default)s)")
    parser.add_argument("--limit", type=int, default=None, help="Limit how many missing products to process")
    parser.add_argument("--referer", default="https://e.prenta.lt/", help="Referer/origin to use when fetching images")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout (seconds) for image downloads")
    parser.add_argument("--dump-html", default=None, help="Optional directory to write recovered HTML snippets")
    parser.add_argument("--dry-run", action="store_true", help="Only print summary, do not write report file")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 1

    with input_path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)

    dump_dir = Path(args.dump_html) if args.dump_html else None
    report = rerender_missing(payload, args.referer, max(1, args.timeout), args.limit, dump_dir)
    report["input_file"] = str(input_path)

    attempted = report.get("attempted", 0)
    recovered = report.get("recovered", 0)
    still_missing = attempted - recovered

    print(
        json.dumps(
            {
                "attempted": attempted,
                "recovered": recovered,
                "still_missing": still_missing,
                "status_counts": report.get("status_counts", {}),
                "report_file": None if args.dry_run else args.output,
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    if not args.dry_run:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)

    return 0


if __name__ == "__main__":
    sys.exit(main())
