#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gBizINFO: 法人番号+社名をダンプし、法人番号ごとに基本情報を取得して紐づけるワンファイルツール。

機能:
- dump:  都道府県ごとに指定法人種別の「法人番号・社名」を収集 (最大 5000件/ページ × 10ページ/都道府県)
         corporate_type（法人種別：301=株式会社, 305=合同会社など）の指定に対応
         exist_flg（法人活動情報：true=あり / false=なし）の絞り込みに対応
- hydrate: dumpで得た法人番号をもとに /v1/hojin/{corporate_number} を叩き、基本情報をCSVに追記（進捗表示つき）
- pipeline: dump→hydrate を連続実行（corporate_type, exist_flg 指定、hydrate の進捗表示に対応）

前提:
- .env に GBIZ_API_TOKEN を設定 (X-hojinInfo-api-token ヘッダとして送信)
- 都道府県コードは JIS X 0401 の 2桁表記 (01〜47)
- 法人種別コード: 101=国の機関, 201=地方公共団体, 301=株式会社, 302=有限会社, 
  303=合名会社, 304=合資会社, 305=合同会社, 401=外国会社, 499=その他
"""

from __future__ import annotations
import os
import sys
import csv
import time
import argparse
from typing import Dict, Any, Iterable, List, Set, Optional
import requests
from dotenv import load_dotenv

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_BASE = "https://info.gbiz.go.jp/hojin"
SEARCH = API_BASE + "/v1/hojin"
DETAIL = API_BASE + "/v1/hojin/{corporate_number}"
TIMEOUT = 60  # タイムアウトを30秒から60秒に延長

BASIC_FIELDS = [
    "corporate_number",
    "name",
    "date_of_establishment",
    "employee_number",
    "capital_stock",
    "prefecture_code",
    "city_code",
    "postal_code",
    "location",
    "company_url",
    "business_summary",
]


# -----------------------------
# HTTP helpers
# -----------------------------
def _hdr(tok: str) -> Dict[str, str]:
    """gBizINFO APIリクエスト用ヘッダを生成する。"""
    return {"X-hojinInfo-api-token": tok}


def _get_json(
    url: str, headers: Dict[str, str], params: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """指定URLへGETし、JSONを辞書で返す（204は空配列ラップで正規化）。リトライ機能付き。"""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    try:
        r = session.get(url, headers=headers, params=params, timeout=TIMEOUT)
        if r.status_code == 204:
            return {"hojin-infos": []}
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
        return r.json()
    except requests.exceptions.Timeout:
        raise RuntimeError(f"Request timeout after {TIMEOUT} seconds")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Connection error: {str(e)}")
    finally:
        session.close()


def _make_session(tok: str) -> requests.Session:
    """Keep-Alive とコネクションプールを有効にした Session を返す。"""
    s = requests.Session()
    s.headers.update({"X-hojinInfo-api-token": tok})
    adapter = HTTPAdapter(
        pool_connections=64,
        pool_maxsize=64,
        max_retries=Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        ),
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# -----------------------------
# step1: 法人一覧ダンプ
# -----------------------------
def iter_corporate_list(
    tok: str, 
    pref_code: str, 
    corporate_type: str = "301",
    limit: int = 5000, 
    exist_flg: Optional[str] = None,
    max_pages: int = 10
) -> Iterable[Dict[str, Any]]:
    """指定法人種別の「法人番号・社名」を、指定の都道府県についてページング取得するジェネレータ。"""
    headers = _hdr(tok)
    for page in range(1, max_pages + 1):
        params = {
            "corporate_type": corporate_type,  # 法人種別
            "prefecture": pref_code,  # JIS X 0401 (2桁)
            "limit": str(limit),  # <= 5000
            "page": str(page),
        }
        if exist_flg in ("true", "false"):
            params["exist_flg"] = exist_flg

        js = _get_json(SEARCH, headers, params)
        items = js.get("hojin-infos") or []
        if not items:
            break
        for it in items:
            yield {
                "corporate_number": it.get("corporate_number"),
                "name": it.get("name"),
            }
        if len(items) < limit:
            break


# -----------------------------
# step2: 法人番号から基本情報を取得
# -----------------------------
def fetch_basic(tok: str, corporate_number: str) -> Optional[Dict[str, Any]]:
    """法人番号をキーに、/v1/hojin/{corporate_number} で法人基本情報を1件取得する。"""
    url = DETAIL.format(corporate_number=corporate_number.strip())
    js = _get_json(url, _hdr(tok))
    arr = js.get("hojin-infos") or []
    return arr[0] if arr else None


def fetch_basic_with_session(
    session: requests.Session, corporate_number: str
) -> Optional[Dict[str, Any]]:
    """Session を用いて /v1/hojin/{corporate_number} を取得（コネクション再利用）。"""
    url = f"{API_BASE}/v1/hojin/{corporate_number.strip()}"
    r = session.get(url, timeout=TIMEOUT)
    if r.status_code == 200:
        js = r.json()
        arr = js.get("hojin-infos") or []
        return arr[0] if arr else None
    if r.status_code in (204, 404):
        return None
    # Retryで拾いきれなかった異常はここに来る
    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")


# -----------------------------
# CSV helpers
# -----------------------------
def read_existing_numbers(path: str) -> Set[str]:
    """既存CSVから `corporate_number` を読み取り、重複スキップ用セットを作る。"""
    s: Set[str] = set()
    if not os.path.exists(path):
        return s
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            c = (row.get("corporate_number") or "").strip()
            if c:
                s.add(c)
    return s


def append_rows(path: str, rows: List[Dict[str, Any]], header: List[str]) -> None:
    """CSVに行を追記する（存在しないファイルにはヘッダも書く）。"""
    newfile = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if newfile:
            w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})


# -----------------------------
# Progress helpers（hydrate用）
# -----------------------------
def _count_csv_rows(path: str) -> int:
    """CSVのデータ行数（ヘッダ除く）を数える。"""
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        total = sum(1 for _ in f)
    return max(0, total - 1)


def _fmt_hms(seconds: float) -> str:
    """秒を h:mm:ss に整形して返す。"""
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:d}:{m:02d}:{s:02d}"


def _print_hydrate_progress(
    done: int, total: int, added: int, errors: int, t0: float
) -> None:
    """hydrate処理の進捗を1行で表示する（ETA/処理レート付き）。"""
    elapsed = max(1e-6, time.monotonic() - t0)
    rate = done / elapsed
    pct = (done / total * 100) if total else 100.0
    remain = max(0, total - done)
    eta = remain / rate if rate > 0 else 0.0
    msg = (
        f"[hydrate] {done}/{total} ({pct:5.1f}%) "
        f"added={added} err={errors} "
        f"rate={rate:5.1f}/s ETA={_fmt_hms(eta)} elapsed={_fmt_hms(elapsed)}"
    )
    print(msg, file=sys.stderr, flush=True)


# -----------------------------
# Hydrate runner（単体/パイプライン両対応）
# -----------------------------
def _run_hydrate(
    tok: str,
    infile: str,
    out: str,
    sleep: float,
    resume: bool,
    progress_every: int,
    progress_interval: float,
) -> int:
    """法人番号リストから基本情報を取得してCSVに追記する共通関数（進捗表示つき）。"""
    processed = read_existing_numbers(out) if resume else set()
    if not os.path.exists(infile):
        print(f"ERROR: not found: {infile}", file=sys.stderr)
        return 0

    total_rows = _count_csv_rows(infile)
    approx_already = len(processed)
    approx_target = max(0, total_rows - approx_already)
    print(
        f"[hydrate] start infile={infile} total_rows={total_rows} "
        f"resume={resume} approx_target={approx_target}",
        file=sys.stderr,
        flush=True,
    )

    added = 0
    errors = 0
    done = 0
    t0 = time.monotonic()
    last_print = t0
    show_every = max(0, int(progress_every))
    show_interval = max(0.0, float(progress_interval))

    with _make_session(tok) as session, open(infile, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cno = (row.get("corporate_number") or "").strip()
            if not cno or cno in processed:
                continue

            d = None
            try:
                d = fetch_basic_with_session(session, cno)
            except Exception as e:
                errors += 1
                print(
                    f"[hydrate] corporate_number={cno} error: {e}",
                    file=sys.stderr,
                    flush=True,
                )

            if d is not None:
                append_rows(out, [d], header=BASIC_FIELDS)
                processed.add(cno)
                added += 1

            done += 1

            # 進捗表示（1件目 / 件数間隔 / 時間間隔）
            now = time.monotonic()
            need_by_count = show_every and (done % show_every == 0)
            need_by_time = show_interval > 0 and (now - last_print) >= show_interval
            if done == 1 or need_by_count or need_by_time:
                _print_hydrate_progress(
                    done=done,
                    total=approx_target or total_rows,
                    added=added,
                    errors=errors,
                    t0=t0,
                )
                last_print = now

            if sleep:
                time.sleep(sleep)

    _print_hydrate_progress(
        done=done,
        total=approx_target or total_rows,
        added=added,
        errors=errors,
        t0=t0,
    )
    print(f"OK: {added}件 追記しました -> {out}", file=sys.stderr, flush=True)
    return added


# -----------------------------
# CLI main
# -----------------------------
def main() -> int:
    """コマンドラインエントリポイント。"""
    ap = argparse.ArgumentParser(
        description="gBizINFO: 法人一覧ダンプ -> 個別詳細で紐付け"
    )
    sub = ap.add_subparsers(dest="cmd")

    # dump サブコマンド
    ap_dump = sub.add_parser(
        "dump", help="指定法人種別の法人番号＋社名を都道府県ごとに収集"
    )
    ap_dump.add_argument(
        "--out", default="gbiz_list.csv", help="出力CSVパス（法人番号・社名）"
    )
    ap_dump.add_argument(
        "--pref", default="all", help='都道府県コード 01-47 / "all" で全件'
    )
    ap_dump.add_argument(
        "--limit", type=int, default=5000, help="1ページ件数（1〜5000）"
    )
    ap_dump.add_argument("--sleep", type=float, default=0.2, help="ページ間スリープ秒")
    ap_dump.add_argument(
        "--resume", action="store_true", help="既存CSVを読み、重複をスキップ"
    )
    ap_dump.add_argument(
        "--exist-flg",
        choices=["true", "false", "any"],
        default="any",
        help="法人活動情報で絞り込み。true=あり / false=なし / any=指定なし（既定）",
    )
    ap_dump.add_argument(
        "--corporate-type",
        type=str,
        default="301",
        help="法人種別コード。301=株式会社, 305=合同会社など（既定: 301）",
    )
    ap_dump.add_argument(
        "--max-pages", type=int, default=10, help="最大ページ数（1〜10）"
    )

    # hydrate サブコマンド
    ap_h = sub.add_parser("hydrate", help="法人番号リストから基本情報を付与")
    ap_h.add_argument(
        "--in",
        dest="infile",
        default="gbiz_list.csv",
        help="入力CSVパス（法人番号・社名）",
    )
    ap_h.add_argument(
        "--out", default="gbiz_enriched.csv", help="出力CSVパス（基本情報）"
    )
    ap_h.add_argument("--sleep", type=float, default=0.2, help="リクエスト間スリープ秒")
    ap_h.add_argument(
        "--resume",
        action="store_true",
        help="既存出力CSVを読み、処理済み番号をスキップ",
    )
    ap_h.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="何件ごとに進捗を表示するか（0で無効）。既定: 50",
    )
    ap_h.add_argument(
        "--progress-interval",
        type=float,
        default=0.0,
        help="何秒ごとに進捗を表示するか（0で無効）。件数間隔と併用可。",
    )

    # 1-shot パイプライン
    ap_p = sub.add_parser("pipeline", help="dump -> hydrate を連続実行")
    ap_p.add_argument("--pref", default="all", help='都道府県コード 01-47 / "all"')
    ap_p.add_argument("--list-out", default="gbiz_list.csv", help="dumpの出力CSV")
    ap_p.add_argument(
        "--enrich-out", default="gbiz_enriched.csv", help="hydrateの出力CSV"
    )
    ap_p.add_argument(
        "--sleep", type=float, default=0.2, help="各リクエスト間のスリープ秒"
    )
    ap_p.add_argument(
        "--exist-flg",
        choices=["true", "false", "any"],
        default="any",
        help="dump時の法人活動情報で絞り込み（true/false/any）。",
    )
    ap_p.add_argument(
        "--corporate-type",
        type=str,
        default="301",
        help="法人種別コード。301=株式会社, 305=合同会社など（既定: 301）",
    )
    ap_p.add_argument(
        "--limit", type=int, default=5000, help="1ページ件数（1〜5000）"
    )
    ap_p.add_argument(
        "--max-pages", type=int, default=10, help="最大ページ数（1〜10）"
    )
    ap_p.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="hydrate の進捗: 何件ごとに表示（0で無効）",
    )
    ap_p.add_argument(
        "--progress-interval",
        type=float,
        default=0.0,
        help="hydrate の進捗: 何秒ごとに表示（0で無効）",
    )
    ap_p.add_argument(
        "--resume",
        action="store_true",
        help="dump/hydrate ともに既存出力を読み、重複/処理済みをスキップ",
    )

    args = ap.parse_args()

    load_dotenv()
    tok = os.getenv("GBIZ_API_TOKEN")
    if not tok:
        print("ERROR: .env に GBIZ_API_TOKEN を設定してください。", file=sys.stderr)
        return 2

    # ---- dump ----
    if args.cmd == "dump":
        dest = args.out
        seen = read_existing_numbers(dest) if args.resume else set()
        targets = (
            [f"{i:02d}" for i in range(1, 48)] if args.pref == "all" else [args.pref]
        )
        total_new = 0
        exist_flag_param = None if args.exist_flg == "any" else args.exist_flg
        for pref in targets:
            got = 0
            for row in iter_corporate_list(
                tok,
                pref,
                corporate_type=args.corporate_type,
                limit=min(max(1, args.limit), 5000),
                exist_flg=exist_flag_param,
                max_pages=min(max(1, args.max_pages), 10),
            ):
                cno = (row.get("corporate_number") or "").strip()
                if not cno or cno in seen:  # スキップ/重複
                    continue
                append_rows(dest, [row], header=["corporate_number", "name"])
                seen.add(cno)
                got += 1
                total_new += 1
            print(f"[dump] pref={pref} added={got} total={total_new}", file=sys.stderr)
            if args.sleep:
                time.sleep(args.sleep)
        print(f"OK: {total_new}件 追記しました -> {dest}")
        return 0

    # ---- hydrate ----
    if args.cmd == "hydrate":
        added = _run_hydrate(
            tok=tok,
            infile=args.infile,
            out=args.out,
            sleep=args.sleep,
            resume=args.resume,
            progress_every=args.progress_every,  # ← タイポ修正
            progress_interval=args.progress_interval,
        )
        return 0 if added >= 0 else 1

    # ---- pipeline ----
    if args.cmd == "pipeline":
        # dump
        print("[pipeline] dump...", file=sys.stderr)
        targets = (
            [f"{i:02d}" for i in range(1, 48)] if args.pref == "all" else [args.pref]
        )
        exist_flag_param = None if args.exist_flg == "any" else args.exist_flg
        for pref in targets:
            got = 0
            seen = read_existing_numbers(args.list_out) if args.resume else set()
            for row in iter_corporate_list(
                tok, pref, corporate_type=args.corporate_type, limit=args.limit, exist_flg=exist_flag_param, max_pages=args.max_pages
            ):
                cno = (row.get("corporate_number") or "").strip()
                if not cno or cno in seen:
                    continue
                append_rows(args.list_out, [row], header=["corporate_number", "name"])
                seen.add(cno)
                got += 1
            print(f"[dump] pref={pref} added={got}", file=sys.stderr)
            if args.sleep:
                time.sleep(args.sleep)

        # hydrate（進捗表示つき）
        print("[pipeline] hydrate...", file=sys.stderr)
        _run_hydrate(
            tok=tok,
            infile=args.list_out,
            out=args.enrich_out,
            sleep=args.sleep,
            resume=args.resume,
            progress_every=args.progress_every,
            progress_interval=args.progress_interval,
        )
        return 0

    # ヘルプ表示
    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
