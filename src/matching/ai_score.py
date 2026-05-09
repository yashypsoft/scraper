#!/usr/bin/env python3
"""
AI score engine for master-vs-competitor product matching.

Features:
- Reusable service class for API/UI integration.
- CSV cache to avoid repeated model calls for unchanged rows.
- Improved prompt with richer product context.
- CLI mode to score one or many products and export CSV.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


DEFAULT_MODEL = "deepseek-ai/deepseek-v3.1-terminus"
DEFAULT_API_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
PROMPT_VERSION = "v2-2026-02-25"
DEFAULT_AI_THRESHOLD = 60

VALID_DECISIONS = {"CORRECT_MATCH", "POSSIBLE_MATCH", "WRONG_MATCH"}
KNOWN_COLORS = {
    "black",
    "white",
    "gray",
    "grey",
    "brown",
    "beige",
    "blue",
    "green",
    "red",
    "pink",
    "orange",
    "purple",
    "gold",
    "silver",
    "ivory",
    "espresso",
    "cherry",
    "oak",
    "walnut",
    "navy",
}

CACHE_COLUMNS = [
    "cache_key",
    "product_id",
    "competitor_key",
    "competitor_name",
    "source",
    "competitor_url",
    "decision",
    "ai_score",
    "confidence",
    "reason",
    "matched_signals",
    "mismatched_signals",
    "model",
    "prompt_version",
    "master_hash",
    "competitor_hash",
    "result_source",
    "raw_response",
    "created_at",
    "updated_at",
]

DEFAULT_RULE_CONFIG = {
    "exact_mpn_match": 270,
    "full_name_match": 70,
    "high_name_match": 60,
    "partial_name_match": 25,
    "full_url_match": 70,
    "high_url_match": 60,
    "partial_url_match": 25,
    "full_config_match": 70,
    "config_match": 60,
    "price_valid": 20,
    "no_pending_parts": 60,
    "add_set_missmatch_score": 140,
    "set_mismatch": -200,
    "same_brand_wrong_match": -100,
    "attribute_mismatch": -70,
    "same_group_wrong_match": -80,
    "min_confidence_score": 60,
    "name_match_threshold_high": 90,
    "name_match_threshold_partial": 50,
    "url_match_threshold_high": 90,
    "url_match_threshold_partial": 50,
    "price_range_percent": 15,
    "wrong_match_threshold": 70,
    "fuzzy_match_threshold": 80,
}

DEFAULT_STOP_WORDS = {
    "by",
    "in",
    "the",
    "and",
    "collection",
    "is",
    "set",
    "of",
    "furniture",
    "home",
    "with",
    "small",
    "products",
    "product",
    "htm",
    "html",
}

DEFAULT_SYNONYMS = {
    "gray": ["grey"],
    "grey": ["gray"],
    "wardrobe": ["storage", "unit", "storageunit"],
    "storage": ["wardrobe"],
    "phillipe": ["philippe"],
    "philippe": ["phillipe"],
    "unit": ["wardrobe"],
    "californiaking": ["calking", "cking"],
    "calking": ["californiaking"],
    "cking": ["californiaking"],
    "philips": ["ps"],
    "caribbean": ["carribean"],
    "carribean": ["caribbean"],
    "blacksilver": ["black", "silver"],
}

DEFAULT_EXCLUDE_SYNONYMS = {
    "king": ["calking", "californiaking", "cking"],
}

DEFAULT_EXCLUDE_CATEGORIES = {
    "Dining Sets",
    "Home Bar Sets",
    "Bedroom Sets",
    "Living Room Sets",
    "Coffee Table Sets",
    "Home Office Sets",
    "Game Table Sets",
    "Bedding and Comforter Sets",
    "Outdoor Conversation Sets",
}

SYSTEM_PROMPT = """
You are an expert ecommerce product matching analyst.

Task:
- Decide whether COMPETITOR_PRODUCT is the same sellable product variant as MASTER_PRODUCT.
- Use strict identifier-first reasoning:
  1) GTIN/UPC/EAN exact match is strongest.
  2) MPN/SKU/Part Number normalized match is strong.
  3) Variant attributes (color, size, bed size, layout, dimensions) must align.
  4) Conflicting identifiers should sharply reduce score.

Scoring rules:
- score is an integer from 0 to 100.
- CORRECT_MATCH usually >= 85.
- POSSIBLE_MATCH usually 45-84.
- WRONG_MATCH usually <= 44.
- confidence is 0.0 to 1.0.

Return STRICT JSON only (no markdown, no extra text):
{
  "decision": "CORRECT_MATCH | POSSIBLE_MATCH | WRONG_MATCH",
  "score": 0,
  "confidence": 0.0,
  "reason": "short reason",
  "matched_signals": ["..."],
  "mismatched_signals": ["..."]
}
""".strip()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    if not text or text == "nan":
        return ""
    return re.sub(r"[^a-z0-9]+", "", text)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def levenshtein_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) < len(b):
        a, b = b, a

    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        current = [i]
        for j, cb in enumerate(b, start=1):
            insertions = previous[j] + 1
            deletions = current[j - 1] + 1
            substitutions = previous[j - 1] + (ca != cb)
            current.append(min(insertions, deletions, substitutions))
        previous = current
    return previous[-1]


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def score_to_decision(score: float) -> str:
    if score >= 85:
        return "CORRECT_MATCH"
    if score >= 45:
        return "POSSIBLE_MATCH"
    return "WRONG_MATCH"


class AIScoreService:
    def __init__(
        self,
        system_file: str | Path = "system.csv",
        competitor_file: str | Path = "competitor.csv",
        cache_db: str | Path | None = None,
        cache_file: str | Path | None = None,
        api_key: str | None = None,
        model: str = DEFAULT_MODEL,
        api_url: str = DEFAULT_API_URL,
        prompt_version: str = PROMPT_VERSION,
        ai_threshold: int = DEFAULT_AI_THRESHOLD,
    ):
        self.system_file = Path(system_file)
        self.competitor_file = Path(competitor_file)
        self.cache_file = self._resolve_cache_file(cache_file=cache_file, cache_db=cache_db)
        self.api_key = api_key or os.getenv("NVIDIA_API_KEY", "")
        self.model = model
        self.api_url = api_url
        self.prompt_version = prompt_version
        self.ai_threshold = self._normalize_ai_threshold(ai_threshold)

        self.system_df = pd.DataFrame()
        self.comp_df = pd.DataFrame()
        self._products_index: list[dict[str, Any]] = []
        self._master_by_product: dict[str, pd.Series] = {}
        self._cache_lock = threading.RLock()
        self._cache_rows: dict[str, dict[str, Any]] = {}
        self._rule_config = dict(DEFAULT_RULE_CONFIG)
        self._stop_words = set(DEFAULT_STOP_WORDS)
        self._synonyms = {k: list(v) for k, v in DEFAULT_SYNONYMS.items()}
        self._exclude_synonyms = {k: list(v) for k, v in DEFAULT_EXCLUDE_SYNONYMS.items()}
        self._exclude_categories = set(DEFAULT_EXCLUDE_CATEGORIES)
        self._token_cache: dict[str, list[str]] = {}
        self._normalize_cache: dict[str, str] = {}
        self._fuzzy_variant_cache: dict[str, list[str]] = {}
        self._brand_mpn_map: dict[str, dict[str, str]] = {}
        self._primary_ids: dict[str, list[str]] = {}
        self._config_data: dict[str, dict[str, str]] = {}
        self._system_name_tokens: dict[str, list[str]] = {}
        self._set_regex = re.compile(r"(^|[^a-z0-9])set(?!-of)([^a-z0-9]|$)", flags=re.IGNORECASE)

        self._init_cache_file()
        self.reload_data()

    @staticmethod
    def _resolve_cache_file(
        cache_file: str | Path | None,
        cache_db: str | Path | None,
    ) -> Path:
        if cache_file is not None:
            return Path(cache_file)
        if cache_db is not None:
            legacy_path = Path(cache_db)
            if legacy_path.suffix.lower() == ".sqlite3":
                return legacy_path.with_suffix(".csv")
            return legacy_path
        return Path("ai_score_cache.csv")

    @staticmethod
    def _normalize_ai_threshold(value: Any) -> int:
        try:
            numeric = float(value)
        except Exception:
            numeric = DEFAULT_AI_THRESHOLD
        return int(clamp(numeric, 0, 100))

    def _init_cache_file(self) -> None:
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)

        if self.cache_file.exists() and self.cache_file.stat().st_size > 0:
            try:
                df = pd.read_csv(self.cache_file, dtype=str, keep_default_na=False)
            except Exception as exc:
                raise ValueError(f"failed to read cache CSV {self.cache_file}: {exc}") from exc
        else:
            df = pd.DataFrame(columns=CACHE_COLUMNS)

        for column in CACHE_COLUMNS:
            if column not in df.columns:
                df[column] = ""
        df = df[CACHE_COLUMNS].copy()
        df["cache_key"] = df["cache_key"].astype(str).str.strip()
        df = df[df["cache_key"] != ""]
        if not df.empty:
            df = df.drop_duplicates(subset=["cache_key"], keep="last")

        with self._cache_lock:
            self._cache_rows = {
                clean_text(row["cache_key"]): {
                    column: clean_text(row.get(column))
                    for column in CACHE_COLUMNS
                }
                for row in df.to_dict(orient="records")
                if clean_text(row.get("cache_key"))
            }
            self._save_cache_rows_locked()

    def _save_cache_rows_locked(self) -> None:
        if self._cache_rows:
            rows = list(self._cache_rows.values())
            df = pd.DataFrame(rows, columns=CACHE_COLUMNS)
        else:
            df = pd.DataFrame(columns=CACHE_COLUMNS)
        df.to_csv(self.cache_file, index=False)

    def reload_data(self) -> None:
        self.system_df = pd.read_csv(self.system_file, dtype=str, keep_default_na=False)
        self.comp_df = pd.read_csv(self.competitor_file, dtype=str, keep_default_na=False)

        if "product_id" not in self.system_df.columns:
            raise ValueError(f"{self.system_file} missing required column: product_id")
        if "product_id" not in self.comp_df.columns:
            raise ValueError(f"{self.competitor_file} missing required column: product_id")

        self.system_df["product_id"] = self.system_df["product_id"].astype(str).str.strip()
        self.comp_df["product_id"] = self.comp_df["product_id"].astype(str).str.strip()
        self.comp_df = self.comp_df.reset_index(drop=True)
        self.comp_df["competitor_key"] = self.comp_df.apply(self._build_competitor_key, axis=1)
        self._rebuild_indexes()

    def _rebuild_indexes(self) -> None:
        comp_counts = self.comp_df.groupby("product_id").size().to_dict()
        self._master_by_product = {}
        self._products_index = []
        self._brand_mpn_map = {}
        self._primary_ids = {}
        self._config_data = {}
        self._system_name_tokens = {}

        seen: set[str] = set()
        for _, row in self.system_df.iterrows():
            product_id = clean_text(row.get("product_id"))
            if not product_id:
                continue

            if product_id not in seen:
                seen.add(product_id)
                self._master_by_product[product_id] = row
                self._products_index.append(
                    {
                        "product_id": product_id,
                        "product_name": clean_text(row.get("product_name")),
                        "brand_label": clean_text(row.get("brand_label")),
                        "competitor_count": int(comp_counts.get(product_id, 0)),
                    }
                )

            brand_id = clean_text(row.get("brand_id"))
            mpn_value = clean_text(row.get("mpn"))
            if brand_id and mpn_value:
                merged = self._merge_mpn(mpn_value)
                variants = {
                    self._normalize_text(mpn_value),
                    self._normalize_text(merged),
                }
                for variant in variants:
                    if variant:
                        if brand_id not in self._brand_mpn_map:
                            self._brand_mpn_map[brand_id] = {}
                        self._brand_mpn_map[brand_id][variant] = product_id

            primary_id = clean_text(row.get("primary_id"))
            if primary_id:
                if primary_id not in self._primary_ids:
                    self._primary_ids[primary_id] = []
                self._primary_ids[primary_id].append(product_id)

            cfg: dict[str, str] = {}
            config_keys = [
                "color",
                "size",
                "bed_size_measure",
                "mattress_size",
                "layout_icon",
                "rug_size",
                "power_option",
                "fireplace_option",
                "dimension_text",
                "comfort_level",
                "mattress_thickness",
            ]
            for key in config_keys:
                value = self._normalize_text(row.get(key))
                if value:
                    cfg[key] = value

            first_key = clean_text(row.get("first_config"))
            second_key = clean_text(row.get("second_config"))
            first_val = clean_text(row.get("first_config_value")) or clean_text(row.get(first_key))
            second_val = clean_text(row.get("second_config_value")) or clean_text(row.get(second_key))
            if first_key and first_val:
                cfg[first_key] = self._normalize_text(first_val)
            if second_key and second_val:
                cfg[second_key] = self._normalize_text(second_val)

            self._config_data[product_id] = cfg

            product_name = clean_text(row.get("product_name")).lower()
            brand_label = clean_text(row.get("brand_label"))
            collection = clean_text(row.get("collection"))
            name_clean = self._remove_brand_collection(product_name, brand_label, collection)
            self._system_name_tokens[product_id] = [
                tok for tok in self._tokenize_text(name_clean) if tok not in self._stop_words
            ]

        self._products_index.sort(key=self._product_sort_key)

    @staticmethod
    def _product_sort_key(item: dict[str, Any]) -> tuple[int, str]:
        pid = clean_text(item.get("product_id"))
        return (0, f"{int(pid):020d}") if pid.isdigit() else (1, pid)

    @staticmethod
    def _normalize_pagination(page: int, page_size: int, max_page_size: int = 200) -> tuple[int, int]:
        try:
            page = int(page)
        except Exception:
            page = 1
        try:
            page_size = int(page_size)
        except Exception:
            page_size = 50

        page = max(page, 1)
        page_size = max(1, min(page_size, max_page_size))
        return page, page_size

    @staticmethod
    def _build_competitor_key(row: pd.Series) -> str:
        key_parts = [
            clean_text(row.get("product_id")),
            clean_text(row.get("repricer_id")),
            clean_text(row.get("competitor_id")),
            clean_text(row.get("competitor_name")),
            clean_text(row.get("source")),
            clean_text(row.get("competitor_url")),
            clean_text(row.get("cm_received_sku")),
        ]
        digest = hashlib.sha1("|".join(key_parts).encode("utf-8")).hexdigest()
        return digest[:20]

    def list_products(self) -> list[dict[str, Any]]:
        return [
            {
                "product_id": row["product_id"],
                "product_name": row["product_name"],
                "brand_label": row["brand_label"],
            }
            for row in self._products_index
        ]

    def list_products_page(
        self,
        query: str = "",
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        page, page_size = self._normalize_pagination(page, page_size, max_page_size=250)
        query_text = clean_text(query).lower()

        products = self._products_index
        if query_text:
            products = [
                row
                for row in products
                if (
                    query_text in row["product_id"].lower()
                    or query_text in row["product_name"].lower()
                    or query_text in row["brand_label"].lower()
                )
            ]

        total = len(products)
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = min(page, total_pages)
        offset = (page - 1) * page_size
        items = products[offset : offset + page_size]

        return {
            "items": items,
            "meta": {
                "query": query,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_prev": page > 1,
                "has_next": page < total_pages,
            },
        }

    def get_product_details(
        self,
        product_id: str,
        page: int = 1,
        page_size: int = 50,
        query: str = "",
        source: str = "",
    ) -> dict[str, Any]:
        product_id = clean_text(product_id)
        if not product_id:
            raise ValueError("product_id is required")
        page, page_size = self._normalize_pagination(page, page_size, max_page_size=300)
        query_text = clean_text(query).lower()
        source_text = clean_text(source).lower()

        master = self._get_master_row(product_id)
        competitors_df = self.comp_df[self.comp_df["product_id"] == product_id]
        all_sources = sorted(
            {
                clean_text(v)
                for v in competitors_df["source"].tolist()
                if clean_text(v)
            },
            key=lambda v: v.lower(),
        )

        if source_text:
            competitors_df = competitors_df[
                competitors_df["source"].astype(str).str.lower() == source_text
            ]

        if query_text:
            search_cols = [
                "competitor_name",
                "comp_received_name",
                "cm_received_sku",
                "competitor_url",
                "source",
            ]
            mask = pd.Series(False, index=competitors_df.index)
            for col in search_cols:
                if col in competitors_df.columns:
                    mask = mask | competitors_df[col].astype(str).str.lower().str.contains(query_text, na=False)
            competitors_df = competitors_df[mask]

        competitors_df = competitors_df.copy()
        competitors_df["_sort_name"] = competitors_df["competitor_name"].astype(str).str.lower()
        competitors_df["_sort_source"] = competitors_df["source"].astype(str).str.lower()
        competitors_df = competitors_df.sort_values(by=["_sort_name", "_sort_source"], kind="stable")
        total_filtered = len(competitors_df)
        total_pages = max(1, (total_filtered + page_size - 1) // page_size)
        page = min(page, total_pages)
        offset = (page - 1) * page_size
        page_df = competitors_df.iloc[offset : offset + page_size]

        contexts: list[tuple[pd.Series, dict[str, Any]]] = []
        cache_keys: list[str] = []
        for _, comp in page_df.iterrows():
            context = self._build_match_context(master, comp)
            contexts.append((comp, context))
            cache_keys.append(context["cache_key"])

        cached_map = self._fetch_cache_many(cache_keys)
        competitors: list[dict[str, Any]] = []
        for comp, context in contexts:
            cached = cached_map.get(context["cache_key"])
            competitors.append(self._serialize_competitor(comp, cached))

        return {
            "product": self._serialize_master(master),
            "competitors": competitors,
            "meta": {
                "product_id": product_id,
                "total_competitors": int(len(self.comp_df[self.comp_df["product_id"] == product_id])),
                "filtered_competitors": int(total_filtered),
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "query": query,
                "source": source,
                "available_sources": all_sources,
                "scored_competitors": self._count_cached_product_scores(product_id),
                "model": self.model,
                "prompt_version": self.prompt_version,
                "ai_threshold": self.ai_threshold,
            },
        }

    def score_competitor(
        self,
        product_id: str,
        competitor_key: str,
        force: bool = False,
    ) -> dict[str, Any]:
        product_id = clean_text(product_id)
        competitor_key = clean_text(competitor_key)
        if not product_id:
            raise ValueError("product_id is required")
        if not competitor_key:
            raise ValueError("competitor_key is required")

        master = self._get_master_row(product_id)
        comp = self._get_competitor_row(product_id, competitor_key)
        context = self._build_match_context(master, comp)

        if not force:
            cached = self._fetch_cache(context["cache_key"])
            if cached:
                result = self._serialize_competitor(comp, cached)
                result["score"]["cached"] = True
                return result

        heuristic = self._heuristic_score(context["master_payload"], context["competitor_payload"])
        heuristic_score = int(heuristic["score"]) if heuristic else -1
        should_call_ai = force or not heuristic or heuristic_score < self.ai_threshold

        if heuristic and not should_call_ai:
            stored = self._upsert_cache(context, heuristic, result_source="heuristic")
            return self._serialize_competitor(comp, stored)

        ai_result, raw_response = self._call_ai(
            context["master_payload"], context["competitor_payload"]
        )
        if ai_result:
            stored = self._upsert_cache(
                context,
                ai_result,
                result_source="ai",
                raw_response=raw_response,
            )
            return self._serialize_competitor(comp, stored)

        # AI unavailable or malformed response fallback
        fallback = heuristic or {
            "decision": "POSSIBLE_MATCH",
            "score": 50,
            "confidence": 0.2,
            "reason": "AI unavailable and no strong heuristic signal.",
            "matched_signals": [],
            "mismatched_signals": ["AI call failed"],
        }
        stored = self._upsert_cache(context, fallback, result_source="fallback", raw_response=raw_response)
        return self._serialize_competitor(comp, stored)

    def score_all(
        self,
        product_id: str,
        force: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        product_id = clean_text(product_id)
        if not product_id:
            raise ValueError("product_id is required")

        competitors_df = self.comp_df[self.comp_df["product_id"] == product_id]
        if limit is not None and limit > 0:
            competitors_df = competitors_df.head(limit)

        results: list[dict[str, Any]] = []
        for _, row in competitors_df.iterrows():
            comp_key = clean_text(row.get("competitor_key"))
            try:
                results.append(self.score_competitor(product_id, comp_key, force=force))
            except Exception as exc:
                results.append(
                    {
                        "competitor_key": comp_key,
                        "product_id": product_id,
                        "error": str(exc),
                    }
                )
        return results

    def update_score(
        self,
        product_id: str,
        competitor_key: str,
        ai_score: float,
        decision: str | None = None,
        confidence: float | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        product_id = clean_text(product_id)
        competitor_key = clean_text(competitor_key)
        master = self._get_master_row(product_id)
        comp = self._get_competitor_row(product_id, competitor_key)
        context = self._build_match_context(master, comp)

        score_value = int(clamp(float(ai_score), 0, 100))
        manual_decision = decision or score_to_decision(score_value)
        manual_confidence = clamp(float(confidence if confidence is not None else 1.0), 0.0, 1.0)
        manual_reason = clean_text(reason) or "Manual score override from UI."

        result = {
            "decision": manual_decision,
            "score": score_value,
            "confidence": manual_confidence,
            "reason": manual_reason,
            "matched_signals": ["Manual override"],
            "mismatched_signals": [],
        }
        stored = self._upsert_cache(context, result, result_source="manual")
        return self._serialize_competitor(comp, stored)

    def _get_master_row(self, product_id: str) -> pd.Series:
        row = self._master_by_product.get(product_id)
        if row is not None:
            return row
        rows = self.system_df[self.system_df["product_id"] == product_id]
        if rows.empty:
            raise KeyError(f"product_id not found in system.csv: {product_id}")
        return rows.iloc[0]

    def _get_competitor_row(self, product_id: str, competitor_key: str) -> pd.Series:
        rows = self.comp_df[
            (self.comp_df["product_id"] == product_id)
            & (self.comp_df["competitor_key"] == competitor_key)
        ]
        if rows.empty:
            raise KeyError(f"competitor not found for product_id={product_id}, key={competitor_key}")
        return rows.iloc[0]

    def _serialize_master(self, row: pd.Series) -> dict[str, Any]:
        fields = [
            "product_id",
            "product_name",
            "brand_label",
            "sku",
            "mpn",
            "gtin",
            "part_number",
            "color",
            "size",
            "bed_size_measure",
            "mattress_size",
            "dimension_text",
            "our_price",
            "osb_url",
        ]
        return {key: clean_text(row.get(key)) for key in fields}

    def _serialize_competitor(
        self,
        row: pd.Series,
        cached: dict[str, Any] | None,
    ) -> dict[str, Any]:
        result = {
            "product_id": clean_text(row.get("product_id")),
            "competitor_key": clean_text(row.get("competitor_key")),
            "competitor_name": clean_text(row.get("competitor_name")),
            "source": clean_text(row.get("source")),
            "comp_received_name": clean_text(row.get("comp_received_name")),
            "cm_received_sku": clean_text(row.get("cm_received_sku")),
            "competitor_price": clean_text(row.get("competitor_price")),
            "competitor_status": clean_text(row.get("competitor_status")),
            "competitor_url": clean_text(row.get("competitor_url")),
            "other_url": clean_text(row.get("other_url")),
            "reason": clean_text(row.get("reason")),
            "other_reason": clean_text(row.get("other_reason")),
            "last_update_date": clean_text(row.get("last_update_date")),
            "score": None,
        }
        if cached:
            result["score"] = {
                "decision": cached["decision"],
                "ai_score": cached["ai_score"],
                "confidence": cached["confidence"],
                "reason": cached["reason"],
                "matched_signals": cached["matched_signals"],
                "mismatched_signals": cached["mismatched_signals"],
                "model": cached["model"],
                "prompt_version": cached["prompt_version"],
                "result_source": cached["result_source"],
                "updated_at": cached["updated_at"],
                "cached": False,
            }
        return result

    def _build_match_context(self, master: pd.Series, comp: pd.Series) -> dict[str, Any]:
        master_payload = self._master_payload(master)
        competitor_payload = self._competitor_payload(comp)
        master_hash = self._payload_hash(master_payload)
        competitor_hash = self._payload_hash(competitor_payload)
        cache_key = self._cache_key(master_hash, competitor_hash)

        return {
            "product_id": clean_text(master.get("product_id")),
            "competitor_key": clean_text(comp.get("competitor_key")),
            "master_payload": master_payload,
            "competitor_payload": competitor_payload,
            "master_hash": master_hash,
            "competitor_hash": competitor_hash,
            "cache_key": cache_key,
        }

    def _master_payload(self, row: pd.Series) -> dict[str, Any]:
        keys = [
            "product_id",
            "product_name",
            "type",
            "web_id",
            "status",
            "brand_label",
            "brand_id",
            "primary_id",
            "sku",
            "mpn",
            "gtin",
            "part_number",
            "Visibility",
            "first_config",
            "second_config",
            "first_config_value",
            "second_config_value",
            "color",
            "size",
            "bed_size_measure",
            "mattress_size",
            "layout_icon",
            "rug_size",
            "power_option",
            "fireplace_option",
            "mattress_thickness",
            "comfort_level",
            "dimension_text",
            "our_price",
            "map_price",
            "cat",
            "collection",
        ]
        return {k: clean_text(row.get(k)) for k in keys}

    def _competitor_payload(self, row: pd.Series) -> dict[str, Any]:
        keys = [
            "product_id",
            "repricer_id",
            "competitor_name",
            "competitor_id",
            "source",
            "comp_received_name",
            "cm_received_sku",
            "competitor_price",
            "competitor_status",
            "competitor_url",
            "other_url",
            "reason",
            "other_reason",
            "last_update_date",
            "other_last_update_date",
            "cm_pr_mismatch_url",
            "sku_mismatch",
        ]
        return {k: clean_text(row.get(k)) for k in keys}

    @staticmethod
    def _payload_hash(payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _cache_key(self, master_hash: str, competitor_hash: str) -> str:
        raw = f"{self.model}|{self.prompt_version}|{master_hash}|{competitor_hash}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _fetch_cache(self, cache_key: str) -> dict[str, Any] | None:
        cache_key = clean_text(cache_key)
        if not cache_key:
            return None
        with self._cache_lock:
            row = self._cache_rows.get(cache_key)
        if not row:
            return None
        return self._row_to_cache_dict(row)

    def _fetch_cache_many(self, cache_keys: list[str]) -> dict[str, dict[str, Any]]:
        keys = [clean_text(key) for key in cache_keys if clean_text(key)]
        if not keys:
            return {}

        result: dict[str, dict[str, Any]] = {}
        with self._cache_lock:
            for key in keys:
                row = self._cache_rows.get(key)
                if not row:
                    continue
                parsed = self._row_to_cache_dict(row)
                result[parsed["cache_key"]] = parsed
        return result

    def _count_cached_product_scores(self, product_id: str) -> int:
        with self._cache_lock:
            unique_competitors = {
                clean_text(row.get("competitor_key"))
                for row in self._cache_rows.values()
                if (
                    clean_text(row.get("product_id")) == product_id
                    and clean_text(row.get("model")) == self.model
                    and clean_text(row.get("prompt_version")) == self.prompt_version
                    and clean_text(row.get("competitor_key"))
                )
            }
        return len(unique_competitors)

    @staticmethod
    def _parse_json_list(value: Any) -> list[str]:
        text = clean_text(value)
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            return [text]
        if isinstance(parsed, list):
            return [clean_text(v) for v in parsed if clean_text(v)]
        parsed_text = clean_text(parsed)
        return [parsed_text] if parsed_text else []

    def _row_to_cache_dict(self, row: dict[str, Any]) -> dict[str, Any]:
        try:
            ai_score = int(round(float(row.get("ai_score", 0) or 0)))
        except Exception:
            ai_score = 0
        try:
            confidence = float(row.get("confidence", 0) or 0)
        except Exception:
            confidence = 0.0

        return {
            "cache_key": clean_text(row.get("cache_key")),
            "product_id": clean_text(row.get("product_id")),
            "competitor_key": clean_text(row.get("competitor_key")),
            "competitor_name": clean_text(row.get("competitor_name")),
            "source": clean_text(row.get("source")),
            "competitor_url": clean_text(row.get("competitor_url")),
            "decision": clean_text(row.get("decision")),
            "ai_score": ai_score,
            "confidence": confidence,
            "reason": clean_text(row.get("reason")),
            "matched_signals": self._parse_json_list(row.get("matched_signals", "")),
            "mismatched_signals": self._parse_json_list(row.get("mismatched_signals", "")),
            "model": clean_text(row.get("model")),
            "prompt_version": clean_text(row.get("prompt_version")),
            "result_source": clean_text(row.get("result_source")),
            "updated_at": clean_text(row.get("updated_at")),
        }

    def _upsert_cache(
        self,
        context: dict[str, Any],
        result: dict[str, Any],
        result_source: str,
        raw_response: str = "",
    ) -> dict[str, Any]:
        normalized = self._normalize_result(result)
        now = utc_now_iso()
        cache_key = context["cache_key"]

        with self._cache_lock:
            existing = self._cache_rows.get(cache_key)
            created_at = clean_text(existing.get("created_at")) if existing else now
            self._cache_rows[cache_key] = {
                "cache_key": cache_key,
                "product_id": context["product_id"],
                "competitor_key": context["competitor_key"],
                "competitor_name": clean_text(context["competitor_payload"].get("competitor_name", "")),
                "source": clean_text(context["competitor_payload"].get("source", "")),
                "competitor_url": clean_text(context["competitor_payload"].get("competitor_url", "")),
                "decision": normalized["decision"],
                "ai_score": str(normalized["score"]),
                "confidence": str(normalized["confidence"]),
                "reason": normalized["reason"],
                "matched_signals": json.dumps(normalized["matched_signals"], ensure_ascii=True),
                "mismatched_signals": json.dumps(normalized["mismatched_signals"], ensure_ascii=True),
                "model": self.model,
                "prompt_version": self.prompt_version,
                "master_hash": context["master_hash"],
                "competitor_hash": context["competitor_hash"],
                "result_source": clean_text(result_source),
                "raw_response": clean_text(raw_response),
                "created_at": created_at,
                "updated_at": now,
            }
            self._save_cache_rows_locked()

        return {
            "cache_key": context["cache_key"],
            "product_id": context["product_id"],
            "competitor_key": context["competitor_key"],
            "competitor_name": context["competitor_payload"].get("competitor_name", ""),
            "source": context["competitor_payload"].get("source", ""),
            "competitor_url": context["competitor_payload"].get("competitor_url", ""),
            "decision": normalized["decision"],
            "ai_score": normalized["score"],
            "confidence": normalized["confidence"],
            "reason": normalized["reason"],
            "matched_signals": normalized["matched_signals"],
            "mismatched_signals": normalized["mismatched_signals"],
            "model": self.model,
            "prompt_version": self.prompt_version,
            "result_source": result_source,
            "updated_at": now,
        }

    def _normalize_result(self, result: dict[str, Any]) -> dict[str, Any]:
        score = int(clamp(float(result.get("score", 0)), 0, 100))
        decision = clean_text(result.get("decision")).upper()
        if decision not in VALID_DECISIONS:
            decision = score_to_decision(score)
        confidence = float(clamp(float(result.get("confidence", 0.5)), 0.0, 1.0))
        reason = clean_text(result.get("reason")) or "No reason provided."

        matched = result.get("matched_signals", [])
        mismatched = result.get("mismatched_signals", [])
        if not isinstance(matched, list):
            matched = [str(matched)]
        if not isinstance(mismatched, list):
            mismatched = [str(mismatched)]

        return {
            "decision": decision,
            "score": score,
            "confidence": confidence,
            "reason": reason,
            "matched_signals": [clean_text(v) for v in matched if clean_text(v)],
            "mismatched_signals": [clean_text(v) for v in mismatched if clean_text(v)],
        }

    def _heuristic_score(
        self,
        master: dict[str, Any],
        competitor: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            score = 0
            reasons: list[str] = []
            wrong_reasons: list[str] = []
            replace_words: set[str] = set()
            mpn_match_for_set = False
            name_match_for_set = False
            url_match_for_set = False
            is_set_mismatch = False
            config_or_mpn_match = False

            master_name = clean_text(master.get("product_name")).lower()
            comp_name = clean_text(competitor.get("comp_received_name")).lower()
            comp_url = clean_text(competitor.get("competitor_url")).lower()
            comp_sku = clean_text(competitor.get("cm_received_sku")).lower()

            if not comp_name and not comp_sku and not comp_url:
                return {
                    "decision": "WRONG_MATCH",
                    "score": 5,
                    "confidence": 0.99,
                    "reason": "Competitor listing lacks product name, URL, and SKU.",
                    "matched_signals": [],
                    "mismatched_signals": ["Missing core competitor identifiers"],
                }

            brand_label = clean_text(master.get("brand_label"))
            collection = clean_text(master.get("collection"))
            product_type = clean_text(master.get("type")).lower() or "simple"
            cat_type = clean_text(master.get("cat"))

            url_clean = f"{comp_url}-{comp_sku}-{comp_name}".strip("-")
            url_clean = self._remove_brand_collection(url_clean, brand_label, collection)
            url_norm = self._normalize_text(url_clean)
            url_tokens = self._tokenize_text(url_clean)

            mpn_raw = clean_text(master.get("mpn")).lstrip("0")
            sku_raw = clean_text(master.get("sku")).lstrip("0")
            part_raw = clean_text(master.get("part_number")).lstrip("0")
            gtin_raw = clean_text(master.get("gtin")).lstrip("0")
            brand_id = clean_text(master.get("brand_id"))
            mpn_extra = ""
            if brand_id == "13863":
                mpn_extra = re.sub(r"(?<=\d)[A-Za-z]$", "", mpn_raw)

            matching_values = []
            for value in [mpn_raw, sku_raw, part_raw, gtin_raw, mpn_extra]:
                value = clean_text(value)
                if value and value not in matching_values:
                    matching_values.append(value)

            if self._match_mpn_part_sku(
                values=matching_values,
                target_raw=comp_url,
                target_norm=url_norm,
                replace_words=replace_words,
            ):
                score += self._rule_config["exact_mpn_match"]
                reasons.append("Exact SKU MATCH")
                mpn_match_for_set = True
                config_or_mpn_match = True

            cm_sku_norm = self._normalize_text(comp_sku)
            if self._match_mpn_part_sku(
                values=matching_values,
                target_raw=comp_sku,
                target_norm=cm_sku_norm,
                replace_words=replace_words,
            ):
                score += self._rule_config["exact_mpn_match"]
                reasons.append("CM SKU MATCH")
                mpn_match_for_set = True
                config_or_mpn_match = True

            is_set = bool(self._set_regex.search(url_clean))
            name_tokens = [
                tok
                for tok in self._tokenize_text(
                    self._remove_brand_collection(master_name, brand_label, collection)
                )
                if tok not in self._stop_words
            ]

            if (
                is_set
                and "set" not in name_tokens
                and product_type == "simple"
                and cat_type not in self._exclude_categories
            ):
                score += self._rule_config["set_mismatch"]
                wrong_reasons.append("Match with Set Product")
                is_set_mismatch = True

            name_match_percent = 0.0
            if name_tokens:
                fuzzy_threshold = self._rule_config["fuzzy_match_threshold"]
                matched = 0
                for word in name_tokens:
                    if self._fuzzy_match(word, url_tokens) >= fuzzy_threshold:
                        matched += 1
                        replace_words.add(word)
                name_match_percent = (matched / len(name_tokens)) * 100

                if name_match_percent >= 100:
                    score += self._rule_config["full_name_match"]
                    reasons.append("Full Name")
                    name_match_for_set = True
                elif name_match_percent >= self._rule_config["name_match_threshold_high"]:
                    score += self._rule_config["high_name_match"]
                    reasons.append("High Name")
                    name_match_for_set = True
                elif name_match_percent >= self._rule_config["name_match_threshold_partial"]:
                    score += self._rule_config["partial_name_match"]
                    reasons.append("Partial Name")

            osb_url_raw = clean_text(master.get("osb_url")).lower()
            osb_url_raw = self._remove_brand_collection(osb_url_raw, brand_label, collection)
            osb_url_parts = [p.strip() for p in osb_url_raw.split("-") if p.strip()]
            if osb_url_parts and name_match_percent > 14:
                fuzzy_threshold = self._rule_config["fuzzy_match_threshold"]
                matched = 0
                for word in osb_url_parts:
                    if self._fuzzy_match(word, url_tokens) >= fuzzy_threshold:
                        matched += 1
                        replace_words.add(word)
                percent = (matched / len(osb_url_parts)) * 100 if osb_url_parts else 0
                if percent >= 100:
                    score += self._rule_config["full_url_match"]
                    reasons.append("Full URL")
                    url_match_for_set = True
                elif percent >= self._rule_config["url_match_threshold_high"]:
                    score += self._rule_config["high_url_match"]
                    reasons.append("High URL")
                    url_match_for_set = True
                elif percent >= self._rule_config["url_match_threshold_partial"]:
                    score += self._rule_config["partial_url_match"]
                    reasons.append("Partial URL")

            config_result = self._match_config(master, url_norm)
            if config_result["is_match"]:
                score += config_result["score_delta"]
                reasons.extend(config_result["reasons"])
                config_or_mpn_match = True
            else:
                score += config_result["score_delta"]
                wrong_reasons.extend(config_result["wrong_reasons"])
                if config_result["config_or_mpn_match"]:
                    config_or_mpn_match = True

            if not mpn_match_for_set and not config_result["is_match"]:
                wrong = self._detect_wrong_matches(
                    master=master,
                    url_norm=url_norm,
                    url_tokens=url_tokens,
                    is_set=is_set,
                    family_ids=config_result["family_ids"],
                )
                score += wrong["score_delta"]
                wrong_reasons.extend(wrong["wrong_reasons"])
                if wrong["config_or_mpn_match"]:
                    config_or_mpn_match = True

            color_signal = self._color_mismatch_signal(master.get("color", ""), " ".join([comp_name, comp_url]))
            if color_signal:
                score += self._rule_config["attribute_mismatch"]
                wrong_reasons.append(color_signal)

            our_price = self._to_float(master.get("our_price"))
            comp_price = self._to_float(competitor.get("competitor_price"))
            if our_price > 0 and comp_price > 0:
                pct = self._rule_config["price_range_percent"] / 100.0
                min_price = our_price * (1 - pct)
                max_price = our_price * (1 + pct)
                if min_price <= comp_price <= max_price:
                    score += self._rule_config["price_valid"]
                    reasons.append("Price Valid")

            pending = self._calculate_pending_url(url_tokens, replace_words, brand_label, collection)
            if not pending and not config_or_mpn_match:
                score += self._rule_config["no_pending_parts"]
                reasons.append("No Pending Parts")
            elif pending:
                wrong_reasons.append(f"Pending URL parts: {', '.join(pending[:8])}")

            if (
                (
                    len(reasons) >= 3
                    and mpn_match_for_set
                    and url_match_for_set
                    and name_match_for_set
                    and is_set_mismatch
                )
                or len(reasons) >= 7
            ):
                score += self._rule_config["add_set_missmatch_score"]
                reasons.append("Set Mismatch Recovery")

            confidence = self._confidence_from_score(score)
            decision = score_to_decision(score)
            merged_reasons = list(dict.fromkeys([clean_text(v) for v in reasons if clean_text(v)]))
            merged_wrong = list(dict.fromkeys([clean_text(v) for v in wrong_reasons if clean_text(v)]))

            reason = ""
            if decision == "CORRECT_MATCH":
                reason = merged_reasons[0] if merged_reasons else "High confidence match signals found."
            elif decision == "WRONG_MATCH":
                reason = merged_wrong[0] if merged_wrong else "Strong mismatch signals found."
            else:
                if merged_reasons:
                    reason = merged_reasons[0]
                elif merged_wrong:
                    reason = merged_wrong[0]
                else:
                    reason = "Mixed signals; requires AI validation."

            return {
                "decision": decision,
                "score": int(score),
                "confidence": confidence,
                "reason": reason,
                "matched_signals": merged_reasons[:12],
                "mismatched_signals": merged_wrong[:12],
            }
        except Exception as exc:
            return {
                "decision": "POSSIBLE_MATCH",
                "score": 20,
                "confidence": 0.3,
                "reason": "Rule engine error; using AI fallback.",
                "matched_signals": [],
                "mismatched_signals": [str(exc)],
            }

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(clean_text(value) or 0)
        except Exception:
            return 0.0

    @staticmethod
    def _confidence_from_score(score: float) -> float:
        if score >= 85:
            return 0.92
        if score >= 60:
            return 0.82
        if score >= 45:
            return 0.7
        return 0.86

    def _normalize_text(self, value: Any) -> str:
        text = clean_text(value).lower()
        if not text:
            return ""
        cached = self._normalize_cache.get(text)
        if cached is not None:
            return cached
        if len(self._normalize_cache) > 50000:
            self._normalize_cache = {}

        words = [w for w in re.split(r"[^a-z0-9]+", text) if w]
        deduped = list(dict.fromkeys(words))
        normalized = "".join(deduped)
        self._normalize_cache[text] = normalized
        return normalized

    def _tokenize_text(self, value: Any) -> list[str]:
        text = clean_text(value).lower()
        if not text:
            return []
        cached = self._token_cache.get(text)
        if cached is not None:
            return cached
        if len(self._token_cache) > 50000:
            self._token_cache = {}

        clean = text.replace("http://", "").replace("https://", "")
        clean = clean.split("?", 1)[0]
        clean = re.sub(r"\.(html?|php|aspx?)$", "", clean)
        tokens = [tok for tok in re.split(r"[^a-z0-9]+", clean) if tok]
        filtered: list[str] = []
        for tok in tokens:
            if len(tok) <= 1:
                continue
            if len(tok) >= 6 and re.search(r"[0-9]", tok) and re.search(r"[a-z]", tok):
                continue
            filtered.append(tok)
        deduped = list(dict.fromkeys(filtered))
        self._token_cache[text] = deduped
        return deduped

    def _split_values_for_synonyms(self, value: Any) -> list[str]:
        text = clean_text(value).lower()
        if not text:
            return []
        text = re.sub(r"[^a-z0-9]+", " ", text)
        result: list[str] = []
        seen: set[str] = set()
        for word in text.split(" "):
            if not word or word in self._stop_words or word in seen:
                continue
            seen.add(word)
            result.append(word)
        return result

    def _config_contains_with_synonyms(self, norm_config: str, value: Any) -> bool:
        norm_config = self._normalize_text(norm_config)
        base = self._normalize_text(value)
        if not norm_config or not base:
            return False

        # Dynamic synonym expansion for multi-word config values.
        options = self._split_values_for_synonyms(value)
        if len(options) > 1:
            if base not in self._synonyms:
                self._synonyms[base] = []
            for opt in options:
                opt_norm = self._normalize_text(opt)
                if opt_norm and opt_norm not in self._synonyms[base]:
                    self._synonyms[base].append(opt_norm)

        if base in self._exclude_synonyms:
            for blocked in self._exclude_synonyms[base]:
                blocked_norm = self._normalize_text(blocked)
                if blocked_norm and blocked_norm in norm_config:
                    return False

        if base in norm_config:
            return True
        for syn in self._synonyms.get(base, []):
            syn_norm = self._normalize_text(syn)
            if syn_norm and syn_norm in norm_config:
                return True
        return False

    def _fuzzy_match(self, needle: str, haystack_tokens: list[str]) -> int:
        needle = clean_text(needle).lower()
        if not needle:
            return 0
        cache_key = needle
        if cache_key not in self._fuzzy_variant_cache:
            variants = {
                needle,
                needle + "s",
                needle + "es",
                needle.rstrip("s"),
            }
            for syn in self._synonyms.get(needle, []):
                syn_norm = self._normalize_text(syn)
                if not syn_norm:
                    continue
                variants.add(syn_norm)
                variants.add(syn_norm + "s")
                variants.add(syn_norm.rstrip("s"))
            self._fuzzy_variant_cache[cache_key] = [v for v in variants if v]
            if len(self._fuzzy_variant_cache) > 50000:
                self._fuzzy_variant_cache = {}
                self._fuzzy_variant_cache[cache_key] = [v for v in variants if v]

        variants = self._fuzzy_variant_cache[cache_key]
        needle_len = len(needle)
        fuzzy_threshold = self._rule_config["fuzzy_match_threshold"]

        for token in haystack_tokens:
            if token in variants:
                return 100
            if needle_len > 3 and abs(len(token) - needle_len) <= 2:
                for variant in variants:
                    max_dist = int(len(variant) * 0.2)
                    if max_dist >= 1 and levenshtein_distance(variant, token) <= max_dist:
                        return fuzzy_threshold
        return 0

    def _merge_mpn(self, value: Any) -> str:
        text = clean_text(value).lower()
        if not text:
            return ""
        parts = [p.strip() for p in text.split(";") if p.strip()]
        if len(parts) < 2:
            return text
        parts.sort()
        first = parts[0]
        prefix = first.split("-", 1)[0]
        result = [first]
        for mpn in parts[1:]:
            if mpn.startswith(prefix + "-"):
                mpn = mpn[len(prefix) + 1 :]
            result.append(mpn)
        return "-".join(result)

    def _match_single_value(self, value: str, target_raw: str, target_norm: str) -> bool:
        value = clean_text(value).lower()
        if not value:
            return False
        merged = self._merge_mpn(value)
        variants = {
            value,
            merged,
            self._normalize_text(value),
            self._normalize_text(merged),
        }
        for variant in variants:
            if not variant:
                continue
            if variant in target_raw or variant in target_norm:
                return True

        parts = [p.strip() for p in value.split(";") if p.strip()]
        if len(parts) <= 1:
            return False
        for part in parts:
            part = part.lstrip("0")
            part_norm = self._normalize_text(part)
            if part not in target_raw and (not part_norm or part_norm not in target_norm):
                return False
        return True

    def _match_mpn_part_sku(
        self,
        values: list[str],
        target_raw: str,
        target_norm: str,
        replace_words: set[str],
    ) -> bool:
        if not values:
            return False
        target_raw = clean_text(target_raw).lower()
        target_norm = clean_text(target_norm).lower()
        for value in values:
            if self._match_single_value(value, target_raw, target_norm):
                replace_words.add(self._normalize_text(value))
                return True
        return False

    def _match_config(self, master: dict[str, Any], url_norm: str) -> dict[str, Any]:
        product_id = clean_text(master.get("product_id"))
        primary_id = clean_text(master.get("primary_id"))
        first_key = clean_text(master.get("first_config"))
        second_key = clean_text(master.get("second_config"))
        current_cfg = self._config_data.get(product_id, {})

        def cfg_val(cfg: dict[str, str], key: str, fallback: Any = "") -> str:
            if key and key in cfg:
                return cfg[key]
            if key:
                return self._normalize_text(fallback)
            return ""

        cur_val1 = cfg_val(current_cfg, first_key, master.get("first_config_value"))
        cur_val2 = cfg_val(current_cfg, second_key, master.get("second_config_value"))

        has1 = bool(cur_val1 and self._config_contains_with_synonyms(url_norm, cur_val1))
        has2 = bool(cur_val2 and self._config_contains_with_synonyms(url_norm, cur_val2))

        score_delta = 0
        reasons: list[str] = []
        wrong_reasons: list[str] = []
        family_ids: list[str] = []
        is_match = False
        config_or_mpn_match = False

        if (has1 and has2) or (has1 and not cur_val2):
            is_match = True
            config_or_mpn_match = True
            score_delta += self._rule_config["config_match"]
            reasons.append("Full Config Match" if has1 and has2 else "Config Match")
            return {
                "is_match": is_match,
                "score_delta": score_delta,
                "reasons": reasons,
                "wrong_reasons": wrong_reasons,
                "family_ids": family_ids,
                "config_or_mpn_match": config_or_mpn_match,
            }

        if primary_id and primary_id in self._primary_ids:
            family_rows: list[dict[str, Any]] = []
            for fam_pid in self._primary_ids.get(primary_id, []):
                cfg = self._config_data.get(fam_pid, {})
                fam_val1 = cfg_val(cfg, first_key)
                fam_val2 = cfg_val(cfg, second_key)
                family_ids.append(fam_pid)
                family_rows.append(
                    {
                        "pid": fam_pid,
                        "val1": fam_val1,
                        "val2": fam_val2,
                        "priority": max(len(fam_val1), len(fam_val2)),
                    }
                )
            family_rows.sort(key=lambda row: row["priority"], reverse=True)

            for fam in family_rows:
                has_fam1 = bool(fam["val1"] and self._config_contains_with_synonyms(url_norm, fam["val1"]))
                has_fam2 = bool(fam["val2"] and self._config_contains_with_synonyms(url_norm, fam["val2"]))
                if (has_fam1 and has_fam2) or (has_fam1 and not fam["val2"]):
                    config_or_mpn_match = True
                    if fam["pid"] == product_id:
                        is_match = True
                        score_delta += self._rule_config["config_match"]
                        reasons.append("Full Config Match" if has_fam1 and has_fam2 else "Config Match")
                    else:
                        score_delta += self._rule_config["same_group_wrong_match"]
                        wrong_reasons.append("Match with Same Group Product")
                    break
        elif not primary_id:
            bed_val = current_cfg.get("bed_size_measure", self._normalize_text(master.get("bed_size_measure")))
            color_val = current_cfg.get("color", self._normalize_text(master.get("color")))
            has_bed = bool(bed_val and self._config_contains_with_synonyms(url_norm, bed_val))
            has_color = bool(color_val and self._config_contains_with_synonyms(url_norm, color_val))
            if has_bed or has_color:
                is_match = True
                config_or_mpn_match = True
                score_delta += self._rule_config["config_match"]
                reasons.append("Full Config Match W/G" if has_bed and has_color else "Config Match W/G")

        return {
            "is_match": is_match,
            "score_delta": score_delta,
            "reasons": reasons,
            "wrong_reasons": wrong_reasons,
            "family_ids": family_ids,
            "config_or_mpn_match": config_or_mpn_match,
        }

    def _detect_wrong_matches(
        self,
        master: dict[str, Any],
        url_norm: str,
        url_tokens: list[str],
        is_set: bool,
        family_ids: list[str],
    ) -> dict[str, Any]:
        product_id = clean_text(master.get("product_id"))
        brand_id = clean_text(master.get("brand_id"))
        score_delta = 0
        wrong_reasons: list[str] = []
        config_or_mpn_match = False
        if not brand_id or brand_id not in self._brand_mpn_map:
            return {
                "score_delta": score_delta,
                "wrong_reasons": wrong_reasons,
                "config_or_mpn_match": config_or_mpn_match,
            }

        for other_mpn_norm, other_pid in self._brand_mpn_map[brand_id].items():
            if other_pid == product_id:
                continue
            if other_mpn_norm not in url_norm:
                continue

            other_row = self._master_by_product.get(other_pid)
            if other_row is None:
                continue

            other_type = clean_text(other_row.get("type")).lower() or "simple"
            other_cat = clean_text(other_row.get("cat"))
            other_tokens = self._system_name_tokens.get(other_pid, [])

            if (
                is_set
                and "set" not in other_tokens
                and other_type == "simple"
                and other_cat not in self._exclude_categories
            ):
                continue

            if other_pid in family_ids:
                score_delta += self._rule_config["same_group_wrong_match"]
                wrong_reasons.append("Match with Same Group Product")
                config_or_mpn_match = True
                break

            if other_tokens:
                threshold = self._rule_config["wrong_match_threshold"] / 100.0
                matched = 0
                fuzzy_threshold = self._rule_config["fuzzy_match_threshold"]
                for word in other_tokens:
                    if self._fuzzy_match(word, url_tokens) >= fuzzy_threshold:
                        matched += 1
                if (matched / len(other_tokens)) > threshold:
                    score_delta += self._rule_config["same_brand_wrong_match"]
                    wrong_reasons.append("Match with Same Brand Product")
                    config_or_mpn_match = True
                    break

        return {
            "score_delta": score_delta,
            "wrong_reasons": wrong_reasons,
            "config_or_mpn_match": config_or_mpn_match,
        }

    def _calculate_pending_url(
        self,
        url_tokens: list[str],
        replace_words: set[str],
        brand_label: str,
        collection: str,
    ) -> list[str]:
        if not replace_words:
            pending = list(url_tokens)
        else:
            pending = [token for token in url_tokens if token not in replace_words]
        pending = [word for word in pending if word not in self._stop_words]

        brand_tokens = self._tokenize_text(brand_label)
        if brand_tokens:
            pending = [word for word in pending if word not in brand_tokens]

        if collection:
            collection_clean = clean_text(collection).lower().replace("collection", "")
            collection_tokens = self._tokenize_text(collection_clean)
            if collection_tokens:
                pending = [word for word in pending if word not in collection_tokens]

        return list(dict.fromkeys(pending))

    @staticmethod
    def _remove_brand_collection(url_clean: str, brand_label: str, collection: str) -> str:
        text = clean_text(url_clean).lower()
        brand = clean_text(brand_label).lower()
        coll = clean_text(collection).lower().replace("collection", "").strip()
        if brand:
            text = text.replace(brand, "")
        if coll:
            text = text.replace(coll, "")
        return text

    @staticmethod
    def _token_similarity(a: str, b: str) -> float:
        a_tokens = {tok for tok in re.split(r"[^a-z0-9]+", a.lower()) if tok}
        b_tokens = {tok for tok in re.split(r"[^a-z0-9]+", b.lower()) if tok}
        if not a_tokens or not b_tokens:
            return 0.0
        intersection = len(a_tokens & b_tokens)
        union = len(a_tokens | b_tokens)
        return intersection / union if union else 0.0

    @staticmethod
    def _color_mismatch_signal(master_color: str, competitor_name: str) -> str:
        master_color_norm = clean_text(master_color).lower()
        if not master_color_norm:
            return ""

        comp_colors = {c for c in KNOWN_COLORS if re.search(rf"\b{re.escape(c)}\b", competitor_name.lower())}
        if not comp_colors:
            return ""

        master_parts = set(re.split(r"[^a-z0-9]+", master_color_norm))
        master_parts = {p for p in master_parts if p}
        if master_parts & comp_colors:
            return ""

        return f"Master color '{master_color}' vs competitor colors {sorted(comp_colors)}"

    def _call_ai(self, master: dict[str, Any], competitor: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if not self.api_key:
            return None, "Missing NVIDIA_API_KEY"

        user_prompt = self._build_user_prompt(master, competitor)
        payload = {
            "model": self.model,
            "temperature": 0.1,
            "max_tokens": 400,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=60,
            )
        except Exception as exc:
            return None, f"Request error: {exc}"

        if response.status_code != 200:
            snippet = response.text[:1000]
            return None, f"HTTP {response.status_code}: {snippet}"

        try:
            content = response.json()["choices"][0]["message"]["content"]
        except Exception:
            return None, "Malformed API response structure"

        parsed = self._parse_model_json(content)
        if not parsed:
            return None, content

        return self._normalize_result(parsed), content

    def _build_user_prompt(self, master: dict[str, Any], competitor: dict[str, Any]) -> str:
        return (
            "Decide if these products are the same exact sellable variant.\n\n"
            "MASTER_PRODUCT:\n"
            f"{json.dumps(master, indent=2, ensure_ascii=True)}\n\n"
            "COMPETITOR_PRODUCT:\n"
            f"{json.dumps(competitor, indent=2, ensure_ascii=True)}\n"
        )

    @staticmethod
    def _parse_model_json(content: str) -> dict[str, Any] | None:
        text = clean_text(content)
        if not text:
            return None

        # Remove Markdown code fences if present.
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return None
        try:
            obj = json.loads(match.group(0))
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AI score master-vs-competitor matches.")
    parser.add_argument("system_file", nargs="?", default="system.csv")
    parser.add_argument("competitor_file", nargs="?", default="competitor.csv")
    parser.add_argument("output_file", nargs="?", default="ai_match_output.csv")
    parser.add_argument("--product-id", dest="product_id")
    parser.add_argument(
        "--cache-file",
        dest="cache_file",
        default=None,
        help="Path to CSV cache file (default: ai_score_cache.csv).",
    )
    parser.add_argument(
        "--cache-db",
        dest="cache_db",
        default=None,
        help="Deprecated alias for --cache-file.",
    )
    parser.add_argument(
        "--ai-threshold",
        dest="ai_threshold",
        type=int,
        default=DEFAULT_AI_THRESHOLD,
        help="Call AI only when heuristic score is below this threshold (0-100).",
    )
    parser.add_argument("--model", dest="model", default=DEFAULT_MODEL)
    parser.add_argument("--force", action="store_true", help="Force rescore and bypass cache.")
    parser.add_argument("--limit", type=int, help="Limit competitors per product.")
    return parser


def run_cli(args: argparse.Namespace) -> int:
    service = AIScoreService(
        system_file=args.system_file,
        competitor_file=args.competitor_file,
        cache_db=args.cache_db,
        cache_file=args.cache_file,
        model=args.model,
        ai_threshold=args.ai_threshold,
    )

    product_ids: list[str]
    if args.product_id:
        product_ids = [clean_text(args.product_id)]
    else:
        product_ids = [row["product_id"] for row in service.list_products()]

    output_rows: list[dict[str, Any]] = []
    for product_id in product_ids:
        scored = service.score_all(product_id=product_id, force=args.force, limit=args.limit)
        for item in scored:
            score = item.get("score") or {}
            output_rows.append(
                {
                    "product_id": item.get("product_id"),
                    "competitor_key": item.get("competitor_key"),
                    "competitor_name": item.get("competitor_name"),
                    "source": item.get("source"),
                    "competitor_url": item.get("competitor_url"),
                    "decision": score.get("decision"),
                    "ai_score": score.get("ai_score"),
                    "confidence": score.get("confidence"),
                    "reason": score.get("reason"),
                    "result_source": score.get("result_source"),
                    "updated_at": score.get("updated_at"),
                    "error": item.get("error", ""),
                }
            )

    df = pd.DataFrame(output_rows)
    df.to_csv(args.output_file, index=False)
    print(
        f"Saved {args.output_file} with {len(output_rows)} rows "
        f"(products={len(product_ids)}, force={args.force})"
    )
    return 0


def main() -> int:
    parser = build_cli_parser()
    args = parser.parse_args()
    return run_cli(args)


if __name__ == "__main__":
    raise SystemExit(main())
