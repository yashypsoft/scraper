#!/usr/bin/env python3
"""
Compare/match competitor rows against system rows.

This file contains:
1) php: Magento/PHP-style match report pipeline with remark mapping and
   CSV outputs similar to the provided PHP class.
2) legacy: older product_id-based comparison flow.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import zipfile
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


SYSTEM_FIELDS = (
    "product_name",
    "sku",
    "mpn",
    "part_number",
    "brand_label_label",
    "cat",
    "our_price",
    "color",
    "size",
    "bed_size_measure",
    "mattress_size",
    "primary_id",
)

IDENTIFIER_FIELDS = ("sku", "mpn", "part_number", "web_id", "gtin")

REMARK_MAPPING = {
    1: "Can Add or Update Matches",
    2: "Can Mark as Approved",
    3: "Url Match But brand_label Not Match",
    4: "Url brand_label Both Not Match",
    5: "Correct Matches",
}

DEFAULT_CHUNK_SIZE = 5000


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def normalize(value: Any) -> str:
    text = clean_text(value).lower()
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", "", text)


def normalize_numeric(value: Any) -> str:
    text = normalize(value)
    if not text:
        return ""
    if text.isdigit():
        return text.lstrip("0") or "0"
    return text


def tokenize(value: Any) -> set[str]:
    text = clean_text(value).lower()
    if not text:
        return set()
    return {token for token in re.findall(r"[a-z0-9]+", text) if token}


def to_float(value: Any) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def slug_from_url(url: str) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        path = urlparse(raw).path.strip("/")
    except ValueError:
        return ""
    if not path:
        return ""
    parts = [p for p in path.split("/") if p]
    if not parts:
        return ""
    if "products" in parts:
        idx = parts.index("products")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return parts[-1]


# ---------------------------------------------------------------------------
# PHP-style engine
# ---------------------------------------------------------------------------
def normalize_brand_label_php(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def create_url_fingerprint(url: str) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    raw = re.sub(r"^https?://(www\.)?", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\?.*$", "", raw)
    raw = raw.rstrip("/")
    if not raw:
        return ""
    segments = raw.split("/")
    return "/".join(segments[:3])


def extract_domain(url: str) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        host = urlparse(raw).hostname or ""
    except ValueError:
        return ""
    host = host.lower()
    return re.sub(r"^www\.", "", host)


def extract_path_key_simple(url: str) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        path = urlparse(raw).path or ""
    except ValueError:
        return ""
    if not path or path == "/":
        return ""
    return path.strip("/")


def get_header_value(row: dict[str, str], names: list[str]) -> str:
    for name in names:
        if name in row:
            return clean_text(row.get(name))
    return ""


class PhpLikeMatcher:
    def __init__(
        self,
        competitor_file: Path,
        system_file: Path,
        output_dir: Path,
        cm_file: Path | None = None,
        limit: int | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        workers: int = 1,
    ):
        self.competitor_file = competitor_file
        self.system_file = system_file
        self.output_dir = output_dir
        self.cm_file = cm_file
        self.limit = limit
        self.chunk_size = chunk_size
        self.workers = max(1, workers)

        self.match_report_file = self.output_dir / "match_product_report.csv"
        self.new_matches_file = self.output_dir / "new_update_matches.csv"
        self.approve_mark_file = self.output_dir / "approve_mark_products.csv"
        self.unmatch_file = self.output_dir / "unmatch_products.csv"
        self.unmatch_matched_cm_file = self.output_dir / "unmatch_matched_with_cm.csv"
        self.report_zip_file = self.output_dir / "FORMATTED_competitor_data.zip"

        self.system_exact_index: dict[str, dict[str, str]] = {}
        self.normalized_system_mpn: dict[str, list[str]] = defaultdict(list)
        self.normalized_system_sku: dict[str, list[str]] = defaultdict(list)
        self.normalized_system_part_number: dict[str, list[str]] = defaultdict(list)
        self.normalized_system_barcode: dict[str, list[str]] = defaultdict(list)
        self.normalized_system_url_key: dict[str, list[str]] = defaultdict(list)
        self.normalized_system_web_id: dict[str, list[str]] = defaultdict(list)

        self.combined: dict[str, dict[str, list[str]]] = defaultdict(dict)
        self.combined_prefix: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

        self.cm_data: dict[str, dict[str, str]] = {}
        self.cm_data_clone: dict[str, dict[str, str]] = {}

        self.unmatch_rows: list[dict[str, str]] = []
        self.unmatch_matched_with_cm_products: list[dict[str, str]] = []

        self.brand_label_cache: dict[tuple[str, str], str] = {}
        self.brand_label_mapping: dict[str, str] = {}
        self.brand_label_resolve_cache: dict[str, str] = {}
        self.url_fingerprints: dict[str, str] = {}

        self.system_brand_labels_by_norm: dict[str, str] = {}
        self.system_brand_label_keys: list[str] = []

        self.competitor_headers: list[str] = []
        self.competitor_domain: str = ""
        self.competitor_id: str = ""

        self.stats = {
            "processed_rows": 0,
            "matched_rows": 0,
            "unmatched_rows": 0,
            "url_matched_from_unmatch": 0,
            "match_report_rows": 0,
            "new_match_rows": 0,
            "approve_rows": 0,
            "cm_rows_loaded": 0,
        }

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.competitor_domain = self.detect_competitor_domain()
        self.prepare_system_products()
        self.prepare_cm_products()
        self.process_csv_in_chunks()
        self.find_unmatch_matches_with_cm_products()
        self.rewrite_unmatch_file()
        self.write_unmatch_matched_with_cm_file()
        self.write_report_zip()

        summary = dict(self.stats)
        summary.update(
            {
                "competitor_domain": self.competitor_domain,
                "competitor_id": self.competitor_id,
                "match_report_file": str(self.match_report_file),
                "new_matches_file": str(self.new_matches_file),
                "approve_mark_file": str(self.approve_mark_file),
                "unmatch_file": str(self.unmatch_file),
                "unmatch_matched_with_cm_file": str(self.unmatch_matched_cm_file),
                "report_zip_file": str(self.report_zip_file),
            }
        )
        return summary

    def detect_competitor_domain(self) -> str:
        with self.competitor_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return ""
            for row in reader:
                url = clean_text(row.get("Ref Product URL"))
                if not url:
                    continue
                domain = extract_domain(url)
                if domain:
                    return domain
        return ""

    def detect_competitor_id_from_cm(self) -> str:
        if self.cm_file is None or not self.cm_file.exists():
            return ""
        if not self.competitor_domain:
            return ""

        with self.cm_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return ""

            for row in reader:
                comp_id = get_header_value(row, ["competitor_id"])
                if not comp_id:
                    continue

                cm_url = get_header_value(row, ["cm_url", "competitor_url", "ref_url", "Ref Product URL"])
                if not cm_url:
                    continue

                cm_domain = extract_domain(cm_url)
                if cm_domain and self.competitor_domain in cm_domain:
                    return comp_id

        return ""

    @staticmethod
    def _dedupe_map_values(map_obj: dict[str, list[str]]) -> None:
        for key, values in list(map_obj.items()):
            unique_vals = list(dict.fromkeys(values))
            map_obj[key] = unique_vals

    def prepare_system_products(self) -> None:
        required = {
            "product_id",
            "sku",
            "web_id",
            "gtin",
            "mpn",
            "brand_label",
            "cat",
            "part_number",
            "osb_url",
        }
        with self.system_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                missing = sorted(required - set(reader.fieldnames or []))
                raise ValueError(f"{self.system_file} is missing required columns: {', '.join(missing)}")

            for row in reader:
                product_id = clean_text(row.get("product_id"))
                if not product_id or product_id in self.system_exact_index:
                    continue

                system_row = {
                    "product_id": product_id,
                    "sku": clean_text(row.get("sku")),
                    "web_id": clean_text(row.get("web_id")),
                    "gtin": clean_text(row.get("gtin")),
                    "mpn": clean_text(row.get("mpn")),
                    "brand_label": clean_text(row.get("brand_label")),
                    "cat": clean_text(row.get("cat")),
                    "part_number": clean_text(row.get("part_number")),
                    "osb_url": clean_text(row.get("osb_url")),
                }
                self.system_exact_index[product_id] = system_row

                brand_label_key = normalize_brand_label_php(system_row["brand_label"])
                if brand_label_key and brand_label_key not in self.system_brand_labels_by_norm:
                    self.system_brand_labels_by_norm[brand_label_key] = system_row["brand_label"]

                validated_mpn = self.validate_sku(system_row["mpn"], ";" in system_row["mpn"])
                validated_gtin = self.validate_sku(system_row["gtin"], ";" in system_row["gtin"])
                validated_sku = self.validate_sku(system_row["sku"], ";" in system_row["sku"])
                validated_part = self.validate_sku(system_row["part_number"], ";" in system_row["part_number"])
                validated_url_key = self.validate_sku(
                    system_row["osb_url"], ";" in system_row["osb_url"]
                )
                validated_web_id = self.validate_sku(system_row["web_id"], ";" in system_row["web_id"])

                if validated_mpn:
                    self.normalized_system_mpn[validated_mpn].append(product_id)
                    if brand_label_key:
                        self.combined[brand_label_key].setdefault(validated_mpn, []).append(product_id)
                if validated_gtin:
                    self.normalized_system_barcode[validated_gtin].append(product_id)
                if validated_sku:
                    self.normalized_system_sku[validated_sku].append(product_id)
                    if brand_label_key:
                        self.combined[brand_label_key].setdefault(validated_sku, []).append(product_id)
                if validated_part:
                    self.normalized_system_part_number[validated_part].append(product_id)
                    if brand_label_key:
                        self.combined[brand_label_key].setdefault(validated_part, []).append(product_id)
                if validated_url_key:
                    self.normalized_system_url_key[validated_url_key].append(product_id)
                if validated_web_id:
                    self.normalized_system_web_id[validated_web_id].append(product_id)

        for map_obj in (
            self.normalized_system_mpn,
            self.normalized_system_sku,
            self.normalized_system_part_number,
            self.normalized_system_barcode,
            self.normalized_system_url_key,
            self.normalized_system_web_id,
        ):
            self._dedupe_map_values(map_obj)

        for brand_label_key, brand_label_map in self.combined.items():
            for key, values in list(brand_label_map.items()):
                unique_vals = list(dict.fromkeys(values))
                brand_label_map[key] = unique_vals
                if len(key) >= 3:
                    self.combined_prefix[brand_label_key][key[:3]].add(key)

        self.system_brand_label_keys = list(self.system_brand_labels_by_norm.keys())

    def prepare_cm_products(self) -> None:
        if self.cm_file is None:
            self.cm_data = {}
            self.cm_data_clone = {}
            return
        if not self.cm_file.exists():
            self.cm_data = {}
            self.cm_data_clone = {}
            return

        with self.cm_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                self.cm_data = {}
                self.cm_data_clone = {}
                return

            fieldnames = set(reader.fieldnames)
            if "product_id" not in fieldnames and "osb_product_id" not in fieldnames:
                self.cm_data = {}
                self.cm_data_clone = {}
                return

        # Match PHP behavior: if competitor id is detected, filter CM rows by that id.
        self.competitor_id = self.detect_competitor_id_from_cm()

        with self.cm_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_id = get_header_value(row, ["product_id", "osb_product_id"])
                if not product_id:
                    continue

                row_comp_id = get_header_value(row, ["competitor_id"])
                if self.competitor_id:
                    if not row_comp_id or row_comp_id != self.competitor_id:
                        continue

                cm_url = get_header_value(row, ["cm_url", "competitor_url", "ref_url", "Ref Product URL"])

                cm_row = {
                    "reason": get_header_value(row, ["reason", "Reason"]),
                    "cm_pr_match": get_header_value(row, ["cm_pr_match", "cm_pr_mismatch_url"]),
                    "cm_url": cm_url,
                    "pr_url": get_header_value(row, ["pr_url", "other_url"]),
                    "our_system_comp_price": get_header_value(row, ["our_system_comp_price", "competitor_price"]),
                    "competitor_id": get_header_value(row, ["competitor_id"]),
                }

                if product_id not in self.cm_data:
                    self.cm_data[product_id] = cm_row
                    continue

                # Prefer rows that provide a URL; otherwise keep first.
                if cm_row["cm_url"] and not self.cm_data[product_id].get("cm_url"):
                    self.cm_data[product_id] = cm_row

        self.cm_data_clone = dict(self.cm_data)
        self.stats["cm_rows_loaded"] = len(self.cm_data)

    def write_report_zip(self) -> None:
        files = [
            self.unmatch_file,
            self.match_report_file,
            self.new_matches_file,
            self.approve_mark_file,
            self.unmatch_matched_cm_file,
        ]
        with zipfile.ZipFile(self.report_zip_file, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
            for file_path in files:
                if file_path.exists():
                    zip_file.write(file_path, arcname=file_path.name)

    def validate_sku(self, value: Any, is_multi_part: bool = False) -> str:
        raw = clean_text(value)
        if not raw:
            return ""

        if is_multi_part:
            parts = re.split(r"[;|,]+", raw)
        else:
            parts = [raw]

        normalized_parts = [normalize(part) for part in parts if normalize(part)]
        if not normalized_parts:
            return ""
        if len(normalized_parts) == 1:
            return normalized_parts[0]

        # Deterministic merge for multipart identifiers.
        unique_sorted = sorted(dict.fromkeys(normalized_parts))
        return "".join(unique_sorted)

    def resolve_current_brand_label(self, competitor_brand_label: str) -> str:
        competitor_brand_label = clean_text(competitor_brand_label)
        if not competitor_brand_label:
            return ""

        if competitor_brand_label in self.brand_label_mapping:
            return self.brand_label_mapping[competitor_brand_label]

        if competitor_brand_label in self.brand_label_resolve_cache:
            return self.brand_label_resolve_cache[competitor_brand_label]

        comp_norm = normalize_brand_label_php(competitor_brand_label)
        if not comp_norm:
            self.brand_label_resolve_cache[competitor_brand_label] = ""
            return ""

        if comp_norm in self.system_brand_labels_by_norm:
            self.brand_label_resolve_cache[competitor_brand_label] = comp_norm
            return comp_norm

        for system_norm in self.system_brand_label_keys:
            if comp_norm in system_norm or system_norm in comp_norm:
                self.brand_label_resolve_cache[competitor_brand_label] = system_norm
                return system_norm

        self.brand_label_resolve_cache[competitor_brand_label] = ""
        return ""

    def find_system_match_fast(self, sku: str, current_brand_label: str) -> list[str]:
        keys = list(
            dict.fromkeys(
                [
                    self.validate_sku(sku, True),
                    self.validate_sku(sku, False),
                ]
            )
        )

        exact_maps = [
            self.normalized_system_mpn,
            self.normalized_system_sku,
            self.normalized_system_part_number,
            self.normalized_system_barcode,
            # Improved exact fallbacks (does not alter original priority).
            self.normalized_system_url_key,
            self.normalized_system_web_id,
        ]

        for key in keys:
            if key == "":
                continue

            for map_obj in exact_maps:
                if key in map_obj:
                    return map_obj[key]

            partial = self.partial_match_fast(key, current_brand_label)
            if partial is not None:
                return partial

        return []

    def partial_match_fast(self, competitor_key: str, current_brand_label: str) -> list[str] | None:
        if len(competitor_key) < 3:
            return None
        if not current_brand_label:
            return None

        brand_label_map = self.combined.get(current_brand_label, {})
        if not brand_label_map:
            return None

        prefix_map = self.combined_prefix.get(current_brand_label, {})
        candidate_keys: set[str] = set()
        primary = prefix_map.get(competitor_key[:3])
        if primary:
            candidate_keys.update(primary)

        if not candidate_keys:
            trigrams = {competitor_key[i : i + 3] for i in range(max(len(competitor_key) - 2, 0))}
            for tri in trigrams:
                keys = prefix_map.get(tri)
                if keys:
                    candidate_keys.update(keys)
                if len(candidate_keys) > 5000:
                    break

        if not candidate_keys:
            return None

        for key in candidate_keys:
            if len(key) < 3:
                continue
            if key.find(competitor_key) != -1 or competitor_key.find(key) != -1:
                return brand_label_map[key]
        return None

    def fast_url_match(self, url1: str, url2: str) -> bool:
        key1 = hashlib.md5(clean_text(url1).encode("utf-8")).hexdigest()
        key2 = hashlib.md5(clean_text(url2).encode("utf-8")).hexdigest()

        if key1 not in self.url_fingerprints:
            self.url_fingerprints[key1] = create_url_fingerprint(url1)
        if key2 not in self.url_fingerprints:
            self.url_fingerprints[key2] = create_url_fingerprint(url2)

        return self.url_fingerprints[key1] == self.url_fingerprints[key2]

    def fast_brand_label_match(self, sys_brand_label: str, comp_brand_label: str) -> str:
        cache_key = (clean_text(sys_brand_label), clean_text(comp_brand_label))
        if cache_key in self.brand_label_cache:
            return self.brand_label_cache[cache_key]

        sys_norm = normalize_brand_label_php(sys_brand_label)
        comp_norm = normalize_brand_label_php(comp_brand_label)

        if sys_norm == "" or comp_norm == "":
            self.brand_label_cache[cache_key] = "No"
            return "No"

        result = "Yes" if (comp_norm.find(sys_norm) != -1 or sys_norm.find(comp_norm) != -1) else "No"
        self.brand_label_cache[cache_key] = result
        if result == "Yes" and comp_brand_label:
            self.brand_label_mapping[comp_brand_label] = sys_norm
        return result

    @staticmethod
    def generate_remark_fast(brand_label_match: str, url_match: str, reason: str) -> int:
        key = f"{brand_label_match}_{url_match}"
        if key == "Yes_Match" and clean_text(reason).lower() == "wrong match":
            return 2
        remark_map = {
            "Yes_Not Match": 1,
            "No_Match": 3,
            "No_Not Match": 4,
            "Yes_Match": 5,
        }
        return remark_map.get(key, 0)

    @staticmethod
    def build_new_match_row(row: dict[str, str], sys: dict[str, str]) -> dict[str, Any]:
        return {
            "sku": sys.get("sku", ""),
            "ref_sku": row.get("Ref MPN", ""),
            "ref_url": row.get("Ref Product URL", ""),
            "ref_name": row.get("Ref Product Name", ""),
            "send_in_feed": 1,
        }

    @staticmethod
    def build_approve_row(sys: dict[str, str], cm_row: dict[str, str]) -> dict[str, str]:
        return {
            "product_id": sys.get("product_id", ""),
            "competitor_id": cm_row.get("competitor_id", ""),
            "type": "update",
            "source": "CM",
            "is_issue": "Approved",
        }

    @staticmethod
    def build_match_row(
        row: dict[str, str],
        sys: dict[str, str],
        cm_row: dict[str, str],
        remark_id: int,
    ) -> dict[str, str]:
        merged = dict(row)
        merged.update(
            {
                "osb_product_id": sys.get("product_id", ""),
                "osb_web_id": sys.get("web_id", ""),
                "osb_sku": sys.get("sku", ""),
                "osb_gtin": sys.get("gtin", ""),
                "osb_mpn": sys.get("mpn", ""),
                "osb_brand_label": sys.get("brand_label", ""),
                "osb_catagory": sys.get("cat", ""),
                "part_number": sys.get("part_number", ""),
                "cm_url": cm_row.get("cm_url", ""),
                "pr_url": cm_row.get("pr_url", ""),
                "remark": REMARK_MAPPING.get(remark_id, ""),
            }
        )
        return merged

    def _prepare_chunk_entry(self, row: dict[str, str]) -> dict[str, Any]:
        competitor_sku = clean_text(row.get("Ref MPN", ""))
        if not competitor_sku:
            return {
                "competitor_sku": "",
                "normalized_ok": False,
                "validated_comp_sku": "",
            }

        normalized_with = self.validate_sku(competitor_sku, True)
        normalized_without = self.validate_sku(competitor_sku, False)
        is_multi = ";" in competitor_sku
        return {
            "competitor_sku": competitor_sku,
            "normalized_ok": bool(normalized_with or normalized_without),
            "validated_comp_sku": self.validate_sku(competitor_sku, is_multi),
        }

    def _prepare_chunk_entries(self, chunk_data: list[dict[str, str]]) -> list[dict[str, Any]]:
        if self.workers <= 1 or len(chunk_data) < 500:
            return [self._prepare_chunk_entry(row) for row in chunk_data]

        # Ordered map keeps row-to-result alignment deterministic.
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            return list(pool.map(self._prepare_chunk_entry, chunk_data))

    def process_chunk(self, chunk_data: list[dict[str, str]], writers: dict[str, csv.DictWriter]) -> None:
        prepared_entries = self._prepare_chunk_entries(chunk_data)

        for index, row in enumerate(chunk_data):
            prepared = prepared_entries[index]
            competitor_sku = prepared["competitor_sku"]
            self.stats["processed_rows"] += 1

            self_current_brand_label = self.resolve_current_brand_label(row.get("Ref Brand Name", ""))

            if not competitor_sku:
                writers["unmatch"].writerow(row)
                self.unmatch_rows.append(row)
                self.stats["unmatched_rows"] += 1
                continue

            if not prepared["normalized_ok"]:
                writers["unmatch"].writerow(row)
                self.unmatch_rows.append(row)
                self.stats["unmatched_rows"] += 1
                continue

            match_product_ids = self.find_system_match_fast(competitor_sku, self_current_brand_label)
            validated_comp_sku = prepared["validated_comp_sku"]

            if not match_product_ids:
                writers["unmatch"].writerow(row)
                self.unmatch_rows.append(row)
                self.stats["unmatched_rows"] += 1
                continue

            self.stats["matched_rows"] += 1
            for product_id in match_product_ids:
                sys = self.system_exact_index.get(product_id)
                if not sys:
                    continue

                if product_id in self.cm_data_clone:
                    del self.cm_data_clone[product_id]
                cm_row = self.cm_data.get(sys["product_id"], {})

                pr_url = clean_text(row.get("Ref Product URL", ""))
                cm_url = clean_text(cm_row.get("cm_url", ""))
                url_match = "Match" if self.fast_url_match(cm_url, pr_url) else "Not Match"
                brand_label_match = self.fast_brand_label_match(sys.get("brand_label", ""), row.get("Ref Brand Name", ""))
                remark = self.generate_remark_fast(brand_label_match, url_match, cm_row.get("reason", ""))

                match_row = self.build_match_row(row, sys, cm_row, remark)
                writers["match"].writerow(match_row)
                self.stats["match_report_rows"] += 1

                if brand_label_match == "Yes" and self_current_brand_label and validated_comp_sku:
                    brand_label_map = self.combined.get(self_current_brand_label, {})
                    if validated_comp_sku in brand_label_map:
                        del brand_label_map[validated_comp_sku]
                        prefix = validated_comp_sku[:3]
                        if prefix in self.combined_prefix.get(self_current_brand_label, {}):
                            self.combined_prefix[self_current_brand_label][prefix].discard(validated_comp_sku)

                if remark == 1:
                    writers["new"].writerow(self.build_new_match_row(row, sys))
                    self.stats["new_match_rows"] += 1

                if remark == 2:
                    writers["approve"].writerow(self.build_approve_row(sys, cm_row))
                    self.stats["approve_rows"] += 1

    def process_csv_in_chunks(self) -> None:
        required_headers = ["Ref Product URL", "Ref MPN", "Ref Brand Name", "Ref Product Name"]

        with self.competitor_file.open("r", newline="", encoding="utf-8-sig") as in_f, \
            self.unmatch_file.open("w", newline="", encoding="utf-8") as unmatch_f, \
            self.match_report_file.open("w", newline="", encoding="utf-8") as match_f, \
            self.new_matches_file.open("w", newline="", encoding="utf-8") as new_f, \
            self.approve_mark_file.open("w", newline="", encoding="utf-8") as approve_f:
            reader = csv.DictReader(in_f)
            if not reader.fieldnames:
                raise ValueError(f"{self.competitor_file} has no header row")

            self.competitor_headers = list(reader.fieldnames)
            for required in required_headers:
                if required not in self.competitor_headers:
                    raise ValueError(f"CSV file must contain '{required}' column in header.")

            match_extra_fields = [
                "osb_product_id",
                "osb_web_id",
                "osb_sku",
                "osb_gtin",
                "osb_mpn",
                "osb_brand_label",
                "osb_catagory",
                "part_number",
                "cm_url",
                "pr_url",
                "remark",
            ]

            writers = {
                "unmatch": csv.DictWriter(unmatch_f, fieldnames=self.competitor_headers),
                "match": csv.DictWriter(match_f, fieldnames=self.competitor_headers + match_extra_fields),
                "new": csv.DictWriter(new_f, fieldnames=["sku", "ref_sku", "ref_url", "ref_name", "send_in_feed"]),
                "approve": csv.DictWriter(
                    approve_f, fieldnames=["product_id", "competitor_id", "type", "source", "is_issue"]
                ),
            }

            for writer in writers.values():
                writer.writeheader()

            chunk_data: list[dict[str, str]] = []
            for row in reader:
                clean_row = {key: clean_text(value) for key, value in row.items()}
                chunk_data.append(clean_row)

                if self.limit is not None and self.stats["processed_rows"] + len(chunk_data) >= self.limit:
                    allowed = self.limit - self.stats["processed_rows"]
                    self.process_chunk(chunk_data[:allowed], writers)
                    chunk_data = []
                    break

                if len(chunk_data) >= self.chunk_size:
                    self.process_chunk(chunk_data, writers)
                    chunk_data = []

                    if self.stats["processed_rows"] % (self.chunk_size * 5) == 0:
                        self.clear_caches()
                        print(f"[php] Processed {self.stats['processed_rows']:,} rows...")

            if chunk_data:
                self.process_chunk(chunk_data, writers)

    def clear_caches(self) -> None:
        self.brand_label_cache = {}
        self.url_fingerprints = {}

    def find_unmatch_matches_with_cm_products(self) -> None:
        if not self.unmatch_rows and not self.cm_data_clone:
            self.unmatch_matched_with_cm_products = []
            return

        cm_path_lookup: dict[str, dict[str, Any]] = {}
        for product_id, cm_row in self.cm_data_clone.items():
            cm_url = clean_text(cm_row.get("cm_url", ""))
            if not cm_url:
                continue
            path_key = extract_path_key_simple(cm_url)
            if not path_key:
                continue
            cm_path_lookup[path_key] = {
                "product_id": product_id,
                "data": cm_row,
                "system_data": self.system_exact_index.get(product_id, {}),
            }

        all_headers = self.get_all_headers(cm_path_lookup, self.unmatch_rows)
        matched_results: list[dict[str, str]] = []
        matched_unmatch_indexes: set[int] = set()

        for idx, comp_row in enumerate(self.unmatch_rows):
            comp_url = clean_text(comp_row.get("Ref Product URL", ""))
            if not comp_url:
                continue
            path_key = extract_path_key_simple(comp_url)
            if not path_key:
                continue

            if path_key in cm_path_lookup:
                cm_data = cm_path_lookup[path_key]
                matched_results.append(
                    self.create_dynamic_merged_row(
                        cm_data["data"],
                        cm_data["system_data"],
                        comp_row,
                        path_key,
                        "MATCHED",
                        all_headers,
                    )
                )
                matched_unmatch_indexes.add(idx)
                del cm_path_lookup[path_key]

        for path_key, cm_data in cm_path_lookup.items():
            matched_results.append(
                self.create_dynamic_merged_row(
                    cm_data["data"],
                    cm_data["system_data"],
                    {},
                    path_key,
                    "NO_MATCH",
                    all_headers,
                )
            )

        self.unmatch_matched_with_cm_products = matched_results
        if matched_unmatch_indexes:
            self.unmatch_rows = [
                row for idx, row in enumerate(self.unmatch_rows) if idx not in matched_unmatch_indexes
            ]
        self.stats["url_matched_from_unmatch"] = len(matched_unmatch_indexes)
        self.stats["unmatched_rows"] = len(self.unmatch_rows)

    def rewrite_unmatch_file(self) -> None:
        if not self.competitor_headers:
            return
        with self.unmatch_file.open("w", newline="", encoding="utf-8") as unmatch_f:
            writer = csv.DictWriter(unmatch_f, fieldnames=self.competitor_headers)
            writer.writeheader()
            for row in self.unmatch_rows:
                writer.writerow(row)

    def get_all_headers(
        self,
        cm_lookup: dict[str, dict[str, Any]],
        competitor_data: list[dict[str, str]],
    ) -> list[str]:
        headers: dict[str, bool] = {}

        if cm_lookup:
            sample_cm = next(iter(cm_lookup.values()))
            for key in sample_cm.get("data", {}).keys():
                headers[f"cm_{key}"] = True
            for key in sample_cm.get("system_data", {}).keys():
                headers[f"osb_{key}"] = True

        if competitor_data:
            sample_comp = competitor_data[0]
            for key in sample_comp.keys():
                clean_key = key.replace("Ref ", "")
                clean_key = clean_key.replace(" ", "_").lower().strip()
                headers[f"competitor_{clean_key}"] = True

        headers["match_path_key"] = True
        headers["match_status"] = True
        return list(headers.keys())

    def create_dynamic_merged_row(
        self,
        cm_row: dict[str, str],
        system_data: dict[str, str],
        comp_row: dict[str, str],
        path_key: str,
        status: str,
        all_headers: list[str],
    ) -> dict[str, str]:
        merged_row = {header: "" for header in all_headers}

        for key, value in cm_row.items():
            merged_row[f"cm_{key}"] = clean_text(value)
        for key, value in system_data.items():
            merged_row[f"osb_{key}"] = clean_text(value)
        for key, value in comp_row.items():
            clean_key = key.replace("Ref ", "")
            clean_key = clean_key.replace(" ", "_").lower().strip()
            merged_row[f"competitor_{clean_key}"] = clean_text(value)

        merged_row["match_path_key"] = path_key
        merged_row["match_status"] = status
        return merged_row

    def write_unmatch_matched_with_cm_file(self) -> None:
        headers: list[str] = ["match_path_key", "match_status"]
        if self.unmatch_matched_with_cm_products:
            all_keys: dict[str, bool] = {}
            for row in self.unmatch_matched_with_cm_products:
                for key in row.keys():
                    all_keys[key] = True

            ordered = ["match_path_key", "match_status"]
            for key in all_keys.keys():
                if key not in ("match_path_key", "match_status"):
                    ordered.append(key)
            headers = ordered

        with self.unmatch_matched_cm_file.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in self.unmatch_matched_with_cm_products:
                writer.writerow(row)


# ---------------------------------------------------------------------------
# Legacy compare helper kept for backward compatibility
# ---------------------------------------------------------------------------
def load_system_index(system_file: Path) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    with system_file.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "product_id" not in reader.fieldnames:
            raise ValueError(f"{system_file} must contain a 'product_id' column")

        for row in reader:
            product_id = clean_text(row.get("product_id"))
            if not product_id or product_id in index:
                continue

            item = {field: clean_text(row.get(field)) for field in SYSTEM_FIELDS}
            item["_identifiers"] = {normalize(row.get(field)) for field in IDENTIFIER_FIELDS}
            item["_identifiers"].discard("")
            item["_name_tokens"] = tokenize(row.get("product_name"))
            item["_our_price_num"] = to_float(row.get("our_price"))
            index[product_id] = item
    return index


def dice_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    common = len(left & right)
    return (2.0 * common) / (len(left) + len(right)) * 100.0


def compare_sku(comp_sku: str, system_ids: set[str]) -> str:
    if not comp_sku:
        return "MISSING"
    if comp_sku in system_ids:
        return "MATCH"
    return "MISMATCH"


def classify_result(
    system_found: bool, sku_result: str, name_overlap_pct: float, price_gap_pct: float | None
) -> str:
    if not system_found:
        return "MISSING_SYSTEM_PRODUCT"
    if sku_result == "MATCH":
        return "LIKELY_MATCH"
    if sku_result == "MISSING":
        if name_overlap_pct >= 70:
            return "LIKELY_MATCH"
        if name_overlap_pct >= 45:
            return "REVIEW"
        return "LIKELY_MISMATCH"
    if name_overlap_pct >= 75 and (price_gap_pct is None or abs(price_gap_pct) <= 20):
        return "REVIEW"
    return "LIKELY_MISMATCH"


def compare_legacy(
    system_file: Path,
    competitor_file: Path,
    output_file: Path,
    limit: int | None = None,
) -> dict[str, int]:
    system_index = load_system_index(system_file)

    with competitor_file.open("r", newline="", encoding="utf-8-sig") as comp_f:
        reader = csv.DictReader(comp_f)
        if not reader.fieldnames or "product_id" not in reader.fieldnames:
            raise ValueError(f"{competitor_file} must contain a 'product_id' column")

        extra_fields = [
            "system_found",
            "system_product_name",
            "system_sku",
            "system_mpn",
            "system_part_number",
            "system_brand_label_label",
            "system_category",
            "system_our_price",
            "system_color",
            "system_size",
            "system_bed_size_measure",
            "system_mattress_size",
            "system_primary_id",
            "sku_compare",
            "name_overlap_pct",
            "price_gap_pct",
            "compare_status",
        ]
        fieldnames = list(reader.fieldnames) + extra_fields

        processed = 0
        missing_system = 0
        likely_match = 0
        likely_mismatch = 0
        review = 0

        with output_file.open("w", newline="", encoding="utf-8") as out_f:
            writer = csv.DictWriter(out_f, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                if limit is not None and processed >= limit:
                    break

                processed += 1
                product_id = clean_text(row.get("product_id"))
                system_row = system_index.get(product_id)

                system_found = system_row is not None
                if not system_found:
                    missing_system += 1

                comp_sku_norm = normalize(row.get("cm_received_sku"))
                comp_name_tokens = tokenize(row.get("comp_received_name"))
                comp_price = to_float(row.get("competitor_price"))

                if system_found:
                    sku_result = compare_sku(comp_sku_norm, system_row["_identifiers"])
                    name_overlap = dice_similarity(comp_name_tokens, system_row["_name_tokens"])

                    system_price = system_row["_our_price_num"]
                    price_gap = None
                    if system_price is not None and system_price > 0 and comp_price is not None:
                        price_gap = ((comp_price - system_price) / system_price) * 100.0
                else:
                    sku_result = "UNKNOWN"
                    name_overlap = 0.0
                    price_gap = None

                status = classify_result(system_found, sku_result, name_overlap, price_gap)
                if status == "LIKELY_MATCH":
                    likely_match += 1
                elif status == "LIKELY_MISMATCH":
                    likely_mismatch += 1
                elif status == "REVIEW":
                    review += 1

                output_row = dict(row)
                output_row.update(
                    {
                        "system_found": "Yes" if system_found else "No",
                        "system_product_name": system_row.get("product_name", "") if system_found else "",
                        "system_sku": system_row.get("sku", "") if system_found else "",
                        "system_mpn": system_row.get("mpn", "") if system_found else "",
                        "system_part_number": system_row.get("part_number", "") if system_found else "",
                        "system_brand_label_label": system_row.get("brand_label_label", "") if system_found else "",
                        "system_category": system_row.get("cat", "") if system_found else "",
                        "system_our_price": system_row.get("our_price", "") if system_found else "",
                        "system_color": system_row.get("color", "") if system_found else "",
                        "system_size": system_row.get("size", "") if system_found else "",
                        "system_bed_size_measure": (
                            system_row.get("bed_size_measure", "") if system_found else ""
                        ),
                        "system_mattress_size": (
                            system_row.get("mattress_size", "") if system_found else ""
                        ),
                        "system_primary_id": system_row.get("primary_id", "") if system_found else "",
                        "sku_compare": sku_result,
                        "name_overlap_pct": f"{name_overlap:.2f}",
                        "price_gap_pct": "" if price_gap is None else f"{price_gap:.2f}",
                        "compare_status": status,
                    }
                )
                writer.writerow(output_row)

                if processed % 100000 == 0:
                    print(f"[legacy] Processed {processed:,} rows...")

    return {
        "processed": processed,
        "missing_system": missing_system,
        "likely_match": likely_match,
        "likely_mismatch": likely_mismatch,
        "review": review,
    }


def pick_existing(defaults: list[str], fallback: str) -> str:
    for name in defaults:
        if Path(name).exists():
            return name
    return fallback


def build_parser() -> argparse.ArgumentParser:
    default_competitor = pick_existing(
        ["afastore.csv", "afastore-final.csv", "competior.csv", "competitor.csv"],
        "afastore.csv",
    )
    default_system = pick_existing(["system.csv", "system-1.csv"], "system.csv")
    default_cm = pick_existing(["competitor-full.csv", "competitor.csv"], "")

    parser = argparse.ArgumentParser(description="Match competitor products against system products.")
    parser.add_argument("competitor_file", nargs="?", default=default_competitor)
    parser.add_argument("system_file", nargs="?", default=default_system)
    parser.add_argument("--mode", choices=["php", "legacy"], default="php")
    parser.add_argument("--output-file", default="compareMatched_output.csv")
    parser.add_argument("--output-dir", default=".")
    parser.add_argument("--cm-file", default=default_cm)
    parser.add_argument("--no-cm", action="store_true", help="Disable CM input file usage in php mode.")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(8, os.cpu_count() or 1)),
        help="Parallel workers for chunk preprocessing in php mode.",
    )
    parser.add_argument("--limit", type=int, default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    competitor_file = Path(args.competitor_file)
    system_file = Path(args.system_file)

    if not competitor_file.exists():
        raise FileNotFoundError(f"Competitor file not found: {competitor_file}")
    if not system_file.exists():
        raise FileNotFoundError(f"System file not found: {system_file}")

    if args.mode == "legacy":
        output_file = Path(args.output_file)
        summary = compare_legacy(
            system_file=system_file,
            competitor_file=competitor_file,
            output_file=output_file,
            limit=args.limit,
        )
        print(f"Saved {output_file}")
        print(
            "Rows={processed:,}, MissingSystem={missing_system:,}, "
            "LikelyMatch={likely_match:,}, Review={review:,}, LikelyMismatch={likely_mismatch:,}".format(
                **summary
            )
        )
        return 0

    output_dir = Path(args.output_dir)
    cm_file: Path | None
    if args.no_cm:
        cm_file = None
    else:
        cm_value = clean_text(args.cm_file)
        cm_file = Path(cm_value) if cm_value else None
        if cm_file == competitor_file:
            cm_file = None

    matcher = PhpLikeMatcher(
        competitor_file=competitor_file,
        system_file=system_file,
        output_dir=output_dir,
        cm_file=cm_file,
        limit=args.limit,
        chunk_size=max(100, args.chunk_size),
        workers=max(1, args.workers),
    )
    summary = matcher.run()
    print("Saved php-style output files:")
    print(f"- {summary['match_report_file']}")
    print(f"- {summary['new_matches_file']}")
    print(f"- {summary['approve_mark_file']}")
    print(f"- {summary['unmatch_file']}")
    print(f"- {summary['unmatch_matched_with_cm_file']}")
    print(f"- {summary['report_zip_file']}")
    print(
        "Processed={processed_rows:,}, Matched={matched_rows:,}, Unmatched={unmatched_rows:,}, "
        "UrlMatchedFromUnmatch={url_matched_from_unmatch:,}, "
        "MatchRows={match_report_rows:,}, NewMatchRows={new_match_rows:,}, "
        "ApproveRows={approve_rows:,}, CMLoaded={cm_rows_loaded:,}, "
        "Domain={competitor_domain}, CompetitorId={competitor_id}".format(**summary)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
