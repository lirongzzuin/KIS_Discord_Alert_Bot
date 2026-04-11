# kis_discord_alert.py
import os
import json
import time
import signal
import sys
import schedule
import requests
import redis
import traceback
from datetime import datetime, timedelta, date
from pytz import timezone
from dotenv import load_dotenv
from threading import Thread, Event
from typing import List, Dict, Tuple, Optional
import holidays
import re as _re_module

# ================== 환경설정 ==================
load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
REDIS_URL = os.getenv("REDIS_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()

# 환율 fallback
FALLBACK_USDKRW = float(os.getenv("FALLBACK_USDKRW", "1350"))
FALLBACK_JPYKRW = float(os.getenv("FALLBACK_JPYKRW", "9.5"))    # 1JPY=x KRW
FALLBACK_HKDKRW = float(os.getenv("FALLBACK_HKDKRW", "175"))
FALLBACK_CNYKRW = float(os.getenv("FALLBACK_CNYKRW", "190"))
FX_CACHE_TTL_SEC = int(os.getenv("FX_CACHE_TTL_SEC", "900"))

# 설정값
FOREIGN_TREND_TOPN = int(os.getenv("FOREIGN_TREND_TOPN", "15"))
KST = timezone("Asia/Seoul")

# Graceful shutdown
shutdown_event = Event()

def _handle_signal(signum, frame):
    print(f"[시그널 수신] {signum} — 종료 중...")
    shutdown_event.set()

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# ================== 동적 연도 ==================
def current_year() -> int:
    return datetime.now(KST).year

def current_year_str() -> str:
    return str(current_year())

def year_start_date() -> str:
    return f"{current_year()}0101"

# ================== Redis ==================
r = None
if REDIS_URL:
    try:
        r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print(f"[Redis] 연결 성공")
    except Exception as e:
        print(f"[Redis] 연결 실패 (Redis 없이 계속 동작): {e}")
        r = None
else:
    print("[Redis] REDIS_URL 미설정 — Redis 없이 동작합니다 (스냅샷/캐시 기능 제한)")

# ================== 발송 ==================
DISCORD_MAX_LEN = 1950
TELEGRAM_MAX_LEN = 4000

def _chunk_message(content: str, max_len: int) -> List[str]:
    if len(content) <= max_len:
        return [content]
    chunks = []
    lines = content.split("\n")
    current = ""
    for line in lines:
        if current and len(current) + len(line) + 1 > max_len:
            chunks.append(current)
            current = line
        else:
            current = current + "\n" + line if current else line
    if current:
        chunks.append(current)
    return chunks

def _kis_api_request(method, url, headers, params, timeout=15, max_retries=3, label="KIS API"):
    """KIS API 호출 + 재시도 (타임아웃/네트워크 오류 시 최대 max_retries회)"""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            if method == "GET":
                res = requests.get(url, headers=headers, params=params, timeout=timeout)
            else:
                res = requests.post(url, headers=headers, json=params, timeout=timeout)
            res.raise_for_status()
            return res.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(2 * attempt)
        except requests.exceptions.RequestException as e:
            raise Exception(f"[{label} 네트워크 오류] {e}")
        except ValueError:
            raise Exception(f"[{label} 응답 오류] JSON 포맷이 아님 또는 응답이 없음")
    raise Exception(f"[{label} 네트워크 오류] {max_retries}회 재시도 실패: {last_err}")

def send_alert_message(content: str):
    if not content:
        return
    send_discord_message(content)
    send_telegram_message(content)

def send_discord_message(content: str):
    if not DISCORD_WEBHOOK_URL:
        return
    for chunk in _chunk_message(content, DISCORD_MAX_LEN):
        try:
            resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
            if resp.status_code == 429:
                retry_after = resp.json().get("retry_after", 1)
                time.sleep(retry_after)
                requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
        except Exception as e:
            print(f"[디스코드 전송 오류] {e}")

def send_telegram_message(content: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chunk in _chunk_message(content, TELEGRAM_MAX_LEN):
        try:
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "disable_web_page_preview": True}
            if "[" in chunk and "](" in chunk:
                payload["parse_mode"] = "Markdown"
            res = requests.post(url, json=payload, timeout=10)
            if res.status_code == 429:
                retry_after = res.json().get("parameters", {}).get("retry_after", 1)
                time.sleep(retry_after)
                requests.post(url, json=payload, timeout=10)
            elif not res.json().get("ok") and payload.get("parse_mode"):
                # Markdown 파싱 실패 시 plain text로 재시도
                del payload["parse_mode"]
                requests.post(url, json=payload, timeout=10)
        except Exception as e:
            print(f"[텔레그램 전송 오류] {e}")

# ================== 휴장/장운영 ==================
# holidays 패키지로 한국 공휴일 자동 판별 (연도 하드코딩 불필요)
_kr_holidays_cache: Dict[int, holidays.HolidayBase] = {}

# 증시 추가 휴장일 (holidays 패키지에 없는 경우 — 연말 폐장 등)
_EXTRA_MARKET_CLOSED = {
    "12-31",  # 연말 폐장일 (매년 고정)
}

def _get_kr_holidays(year: int) -> holidays.HolidayBase:
    if year not in _kr_holidays_cache:
        _kr_holidays_cache[year] = holidays.KR(years=year)
    return _kr_holidays_cache[year]

def is_holiday(dt: datetime = None):
    now = dt or datetime.now(KST)
    d = now.date() if hasattr(now, 'date') else now
    # holidays 패키지 공휴일 체크
    if d in _get_kr_holidays(d.year):
        return True
    # 증시 추가 휴장일 체크 (MM-DD)
    if d.strftime("%m-%d") in _EXTRA_MARKET_CLOSED:
        return True
    return False

def is_trading_day(dt: datetime = None):
    now = dt or datetime.now(KST)
    return now.weekday() < 5 and not is_holiday(now)

def is_market_hour(dt: datetime = None):
    now = dt or datetime.now(KST)
    return 9 <= now.hour <= 15

# ================== 공통 유틸/토큰 ==================
_token_cache = {"token": None, "expire": 0.0}

def get_kis_access_token():
    now_ts = time.time()
    # 메모리 캐시 (Redis 없이도 동작)
    if _token_cache["token"] and _token_cache["expire"] > now_ts:
        return _token_cache["token"]
    # Redis 캐시
    if r:
        token = r.get("KIS_ACCESS_TOKEN")
        expire_ts = r.get("KIS_TOKEN_EXPIRE_TIME")
        if token and expire_ts and float(expire_ts) > now_ts:
            _token_cache["token"] = token
            _token_cache["expire"] = float(expire_ts)
            return token
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    headers = {"Content-Type": "application/json"}
    data = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    res = requests.post(url, headers=headers, data=json.dumps(data), timeout=12).json()
    if "access_token" not in res:
        raise Exception(f"[토큰 오류] {res}")
    token = res["access_token"]
    expires_in = int(res.get("expires_in", 86400))
    expire_at = now_ts + expires_in - 60
    _token_cache["token"] = token
    _token_cache["expire"] = expire_at
    if r:
        r.set("KIS_ACCESS_TOKEN", token)
        r.set("KIS_TOKEN_EXPIRE_TIME", expire_at)
    return token

def safe_int(v):
    try: return int(str(v).replace(",", "").strip())
    except: return 0

def safe_float(v):
    try: return float(str(v).replace(",", "").strip())
    except: return 0.0

# ================== 국내 시세/수급 ==================
def parse_int_field(value):
    value = (value or "").replace(",", "").strip()
    try: return int(value)
    except ValueError: return 0

def get_market_summary(token, stock_code):
    now = datetime.now(KST)
    if now.hour < 15 or (now.hour == 15 and now.minute < 40):
        return ""
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-investor"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010900",
        "Content-Type": "application/json"
    }
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10).json()
        if res.get("rt_cd") == "0" and res.get("output"):
            output = res["output"][0]
            frgn = parse_int_field(output.get("frgn_ntby_qty"))
            inst = parse_int_field(output.get("orgn_ntby_qty"))
            frgn_str = f"🟢 매수 {frgn:+,}주" if frgn > 0 else f"🔴 매도 {frgn:+,}주"
            inst_str = f"🟢 매수 {inst:+,}주" if inst > 0 else f"🔴 매도 {inst:+,}주"
            return f"외국인: {frgn_str} | 기관: {inst_str}"
        return "수급 정보 없음 또는 제공되지 않음"
    except Exception as e:
        return f"수급 정보 오류: {e}"

def get_current_cash_balance(token):
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-psbl-order"
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8908R", "Content-Type": "application/json"
    }
    acct_raw = KIS_ACCOUNT_NO.replace("-", "")
    cano, acct_cd = acct_raw[:8], acct_raw[8:]
    params = {
        "CANO": cano, "ACNT_PRDT_CD": acct_cd, "PDNO": "", "ORD_UNPR": "0", "ORD_DVSN": "00",
        "CMA_EVLU_AMT_ICLD_YN": "N", "OVRS_ICLD_YN": "N"
    }
    res = requests.get(url, headers=headers, params=params, timeout=10).json()
    if res.get("rt_cd") != "0":
        raise Exception(f"[현금 조회 실패] {res.get('msg1', res)}")
    output = res.get("output", {})
    return safe_int(output.get("dnca_tot_amt", "0"))

# ================== 환율 ==================
FX_FALLBACKS = {
    "USD": FALLBACK_USDKRW,
    "JPY": FALLBACK_JPYKRW,
    "HKD": FALLBACK_HKDKRW,
    "CNY": FALLBACK_CNYKRW,
}

def _fx_cache_key(ccy: str) -> str:
    return f"FX:{ccy}KRW"

def _fx_save(ccy: str, v: float):
    if r:
        r.set(_fx_cache_key(ccy), str(v), ex=FX_CACHE_TTL_SEC)

def _fx_load(ccy: str) -> Optional[float]:
    if not r: return None
    v = r.get(_fx_cache_key(ccy))
    try:
        return float(v) if v else None
    except:
        return None

def get_fx_rate_ccykrw(ccy: str) -> float:
    ccy = ccy.upper().strip()
    if ccy == "KRW":
        return 1.0
    v = _fx_load(ccy)
    if v: return v
    try:
        token = get_kis_access_token()
        url = "https://openapi.koreainvestment.com:9443/uapi/overseas-stock/v1/quotations/inquire-ccy-price"
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "HHDFS00000300",
            "custtype": "P",
            "Content-Type": "application/json"
        }
        params = {"CCY": ccy}
        res = requests.get(url, headers=headers, params=params, timeout=8)
        if res.status_code == 200:
            data = res.json()
            out = data.get("output") or {}
            rate = out.get("rate") or out.get("aply_xchg_rt") or out.get("fx_rate")
            if rate:
                fx = float(str(rate).replace(",", ""))
                if fx > 0:
                    _fx_save(ccy, fx)
                    return fx
    except Exception:
        pass
    if ccy in FX_FALLBACKS and FX_FALLBACKS[ccy] > 0:
        _fx_save(ccy, FX_FALLBACKS[ccy])
        return FX_FALLBACKS[ccy]
    return 1200.0 if ccy == "USD" else 10.0

# ================== 해외 잔고 조회(강화) ==================
def _map_market_to_ccy(market_code: str) -> str:
    m = (market_code or "").upper().strip()
    if m in ("NASD","NYSE","AMEX","XNAS","XNYS","XASE"): return "USD"
    if m in ("TKSE","XTKS"): return "JPY"
    if m in ("SEHK","XHKG"): return "HKD"
    if m in ("SHAA","SZAA","XSHG","XSHE"): return "CNY"
    return "USD"

def _fmt_amount_won(v: float) -> str:
    try: return f"{int(round(v)):,}원"
    except: return f"{v:.0f}원"

def _fmt_price_won(v: float) -> str:
    try: return f"{int(round(v)):,}원"
    except: return f"{v:.2f}원"

def _fmt_rate(v: float) -> str:
    try: return f"{v:.2f}%"
    except: return f"{v}%"

def _fmt_won_short(v, sign=False) -> str:
    """큰 금액을 읽기 쉽게 축약: 1.2억원, 3,450만원, 12,345원"""
    try:
        n = int(round(float(v)))
    except (ValueError, TypeError):
        return f"{v}원"
    prefix = "+" if sign and n > 0 else ("-" if n < 0 else "")
    n = abs(n)
    if n >= 100_000_000:
        return f"{prefix}{n / 100_000_000:,.1f}억원"
    if n >= 10_000:
        return f"{prefix}{n // 10_000:,}만원"
    return f"{prefix}{n:,}원"

def _kis_headers(tr_id: str) -> Dict[str, str]:
    token = get_kis_access_token()
    return {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": tr_id,
        "custtype": "P",
        "Content-Type": "application/json"
    }

# 시장/국가 + 거래소 기반 쿼리 세트
MARKET_COUNTRY_LIST = [
    ("10", "840", "미국"),   # USA
    ("20", "392", "일본"),
    ("30", "344", "홍콩"),
    ("40", "156", "중국"),
]
US_EXCHANGES = ["NASD", "NYSE", "AMEX"]  # 미국 거래소 세부

def _call_overseas_present_balance_once(base_params: dict) -> dict:
    acct_raw = KIS_ACCOUNT_NO.replace("-", "")
    cano, acct_cd = acct_raw[:8], acct_raw[8:]
    url = "https://openapi.koreainvestment.com:9443/uapi/overseas-stock/v1/trading/inquire-present-balance"
    headers = _kis_headers("CTRP6504R")
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acct_cd,
        "INQR_DVSN_CD": base_params.get("INQR_DVSN_CD", "01"),
        "TR_MKET_CD": base_params.get("TR_MKET_CD",""),
        "NATN_CD": base_params.get("NATN_CD",""),
        "OVRS_EXCG_CD": base_params.get("OVRS_EXCG_CD",""),
        "WCRC_FRCR_DVSN_CD": base_params.get("WCRC_FRCR_DVSN_CD",""),
        "TR_CRCY_CD": base_params.get("TR_CRCY_CD",""),
        "CTX_AREA_FK200": base_params.get("CTX_AREA_FK200",""),
        "CTX_AREA_NK200": base_params.get("CTX_AREA_NK200",""),
    }
    res = requests.get(url, headers=headers, params=params, timeout=12)
    res.raise_for_status()
    data = res.json()
    if data.get("rt_cd") != "0":
        raise Exception(data.get("msg1", data))
    return data

def _paginate_all_pages(initial_params: dict) -> List[dict]:
    params = dict(initial_params)
    results = []
    while True:
        data = _call_overseas_present_balance_once(params)
        rows = data.get("output1") or data.get("output2") or data.get("output") or []
        if isinstance(rows, dict):
            rows = [rows]
        results.extend(rows)
        fk = (data.get("CTX_AREA_FK200") or "").strip()
        nk = (data.get("CTX_AREA_NK200") or "").strip()
        if fk or nk:
            params["CTX_AREA_FK200"] = fk
            params["CTX_AREA_NK200"] = nk
            time.sleep(0.2)
            continue
        break
    return results

def get_overseas_present_balance() -> List[dict]:
    all_rows: List[dict] = []
    errors: List[str] = []

    # 1) 국가/시장 기반
    for tr_mket_cd, natn_cd, _desc in MARKET_COUNTRY_LIST:
        for wcrc in ("01","02"):
            try:
                rs = _paginate_all_pages({
                    "INQR_DVSN_CD": "01",
                    "TR_MKET_CD": tr_mket_cd,
                    "NATN_CD": natn_cd,
                    "OVRS_EXCG_CD": "",
                    "WCRC_FRCR_DVSN_CD": wcrc,
                    "TR_CRCY_CD": "KRW" if wcrc=="01" else "",
                    "CTX_AREA_FK200": "",
                    "CTX_AREA_NK200": "",
                })
                all_rows.extend(rs)
            except Exception as e:
                errors.append(f"MK({tr_mket_cd}/{natn_cd}/w{wcrc}):{e}")
            time.sleep(0.12)

    # 2) 미국 거래소 기반(미국 보유 누락 보완용)
    for excg in US_EXCHANGES:
        for wcrc in ("01","02"):
            try:
                rs = _paginate_all_pages({
                    "INQR_DVSN_CD": "01",
                    "TR_MKET_CD": "10",
                    "NATN_CD": "840",
                    "OVRS_EXCG_CD": excg,
                    "WCRC_FRCR_DVSN_CD": wcrc,
                    "TR_CRCY_CD": "KRW" if wcrc=="01" else "",
                    "CTX_AREA_FK200": "",
                    "CTX_AREA_NK200": "",
                })
                all_rows.extend(rs)
            except Exception as e:
                errors.append(f"EX({excg}/w{wcrc}):{e}")
            time.sleep(0.12)

    # 3) 전체 폴백 (원화)
    if not all_rows:
        try:
            all_rows = _paginate_all_pages({
                "INQR_DVSN_CD": "01",
                "TR_MKET_CD": "00",
                "NATN_CD": "",
                "OVRS_EXCG_CD": "",
                "WCRC_FRCR_DVSN_CD": "01",
                "TR_CRCY_CD": "KRW",
                "CTX_AREA_FK200": "",
                "CTX_AREA_NK200": "",
            })
        except Exception as e_all:
            raise Exception(f"[해외 잔고 조회 실패] " + " / ".join(errors + [f"ALL:{e_all}"]))

    return all_rows

def _parse_overseas_row(row: dict) -> Optional[dict]:
    if not row:
        return None
    name = row.get("ovrs_item_name") or row.get("prdt_name") or row.get("hts_kor_isnm") or ""
    code = row.get("ovrs_pdno") or row.get("pdno") or row.get("symb") or ""
    market = (row.get("ovrs_excg_cd") or row.get("excg_cd") or "").upper()

    qty = safe_float(
        row.get("ovrs_cblc_qty")
        or row.get("hldg_qty")
        or row.get("frcr_cblc_qty")
        or row.get("qty")
        or 0
    )
    if qty <= 0:
        return None

    avg_ccy = safe_float(
        row.get("frcr_pchs_avg_pric")
        or row.get("pchs_avg_pric")
        or row.get("avg_prc")
        or row.get("frcr_avg_unpr")
        or 0
    )
    cur_ccy = safe_float(
        row.get("ovrs_now_pric")
        or row.get("now_pric")
        or row.get("prpr")
        or row.get("last")
        or 0
    )
    ccy = (row.get("tr_crcy_cd") or row.get("crcy_cd") or _map_market_to_ccy(market) or "USD").upper()

    fx = get_fx_rate_ccykrw(ccy)
    avg_krw = avg_ccy * fx
    cur_krw = cur_ccy * fx
    eval_krw = cur_krw * qty
    invest_krw = avg_krw * qty
    profit_krw = eval_krw - invest_krw
    rate = (profit_krw / invest_krw * 100) if invest_krw else 0.0

    return {
        "name": name, "code": code, "market": market, "qty": qty,
        "avg_ccy": avg_ccy, "cur_ccy": cur_ccy, "ccy": ccy,
        "avg_krw": avg_krw, "cur_krw": cur_krw, "eval_krw": eval_krw,
        "profit_krw": profit_krw, "rate": rate
    }

def get_overseas_account_profit(only_changes=True) -> str:
    rows = get_overseas_present_balance()
    parsed: List[dict] = []
    dedup = {}

    for row in rows:
        p = _parse_overseas_row(row)
        if not p:
            continue
        key = (p["code"], p["market"])
        if key not in dedup or p["eval_krw"] > dedup[key]["eval_krw"]:
            dedup[key] = p

    parsed = list(dedup.values())
    parsed.sort(key=lambda x: x["eval_krw"], reverse=True)

    last_json = r.get("LAST_HOLDINGS_OVRS") if r else None
    last = json.loads(last_json) if last_json else {}

    snap = {}
    changes = []
    total_eval = total_profit = total_invest = 0.0

    def _key(it): return f"{it['name']}|{it['code']}|{it['market']}"

    for it in parsed:
        k = _key(it)
        snap[k] = it["qty"]
        total_eval += it["eval_krw"]
        invest_krw = it["avg_krw"] * it["qty"]
        total_invest += invest_krw
        total_profit += it["profit_krw"]

        old_qty = float(last.get(k, 0.0))
        if abs(it["qty"] - old_qty) > 1e-9:
            diff = it["qty"] - old_qty
            side = "🟢 매수 체결(해외)" if diff > 0 else "🔴 매도 체결(해외)"
            realized_est = 0.0
            if diff < 0:
                realized_est = abs(diff) * (it["cur_krw"] - it["avg_krw"])
            changes.append(
                f"{side} | {it['name']} ({it['code']}:{it['market']})\n"
                f"┗ 수량: {old_qty:.4f} → {it['qty']:.4f}\n"
                f"┗ 현재가(원화): {_fmt_price_won(it['cur_krw'])} | 평균단가(원화): {_fmt_price_won(it['avg_krw'])}\n"
                + (f"┗ 매도 추정 실현손익: {_fmt_amount_won(realized_est)}\n" if diff < 0 else "")
            )

    if r: r.set("LAST_HOLDINGS_OVRS", json.dumps(snap))

    if only_changes:
        return ("🌍 [해외 잔고 변동]\n" + "\n".join(changes)) if changes else ""

    if not parsed:
        return ""

    lines = []
    if changes:
        lines.append("🌍 [해외 잔고 변동]\n" + "\n".join(changes))
    lines.append(f"{'━'*28}")
    lines.append("🌍 [해외 보유 종목 수익률]")
    for it in parsed:
        icon = "🟢" if it["profit_krw"] >= 0 else "🔴"
        lines.append(
            f"{icon} {it['name']} ({it['code']}:{it['market']}) [{it['ccy']}]\n"
            f"┗ 수량: {it['qty']:.4f} | 평균: {it['avg_ccy']:.4f} {it['ccy']} ({_fmt_price_won(it['avg_krw'])})\n"
            f"┗ 현재: {it['cur_ccy']:.4f} {it['ccy']} ({_fmt_price_won(it['cur_krw'])})\n"
            f"┗ 평가: {_fmt_amount_won(it['eval_krw'])} | 손익: {_fmt_amount_won(it['profit_krw'])} ({_fmt_rate(it['rate'])})"
        )
    total_rate = (total_profit/total_invest*100) if total_invest else 0.0
    ov_icon = "🟢" if total_profit >= 0 else "🔴"
    lines.append(f"\n{ov_icon} 해외 합계: {_fmt_amount_won(total_eval)} | 손익: {_fmt_amount_won(total_profit)} ({_fmt_rate(total_rate)})")
    return "\n".join(lines)

# ================== 누적수익/자산 ==================
def _query_realized_profit_period(token, start_dt: str, end_dt: str) -> Tuple[int, float]:
    """기간별 실현손익 조회 (TTTC8715R). (실현수익금, 실현수익률) 반환."""
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-period-trade-profit"
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8715R", "custtype": "P", "Content-Type": "application/json"
    }
    acct_raw = KIS_ACCOUNT_NO.replace("-", "")
    cano, acct_cd = acct_raw[:8], acct_raw[8:]
    params = {
        "CANO": cano, "ACNT_PRDT_CD": acct_cd, "SORT_DVSN": "01", "PDNO": "",
        "INQR_STRT_DT": start_dt, "INQR_END_DT": end_dt, "CBLC_DVSN": "00",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""
    }
    data = _kis_api_request("GET", url, headers, params, timeout=15, max_retries=3, label="실현손익조회")
    if data.get("rt_cd") != "0":
        raise Exception(f"[실현손익조회 실패] {data.get('msg1', data)}")
    output2 = data.get("output2", {})
    profit = safe_int(output2.get("tot_rlzt_pfls", "0"))
    rate = safe_float(output2.get("tot_pftrt", "0"))
    return profit, rate

def get_initial_assets():
    key = f"INITIAL_ASSETS_{current_year()}"
    try:
        val = r.get(key) if r else None
        return int(val) if val else None
    except Exception as e:
        print(f"[초기 자산 조회 오류] {e}")
        return None

def save_initial_assets_if_needed(total_assets: int):
    if not r:
        return
    key = f"INITIAL_ASSETS_{current_year()}"
    try:
        if not r.exists(key):
            r.set(key, str(total_assets))
            print(f"[자동저장] {key} = {total_assets:,}원")
    except Exception as e:
        print(f"[연초 자산 저장 오류] {e}")

def _get_total_overseas_eval() -> Tuple[float, float]:
    try:
        rows = get_overseas_present_balance()
        dedup = {}
        for row in rows:
            p = _parse_overseas_row(row)
            if not p:
                continue
            key = (p["code"], p["market"])
            if key not in dedup or p["eval_krw"] > dedup[key]["eval_krw"]:
                dedup[key] = p
        total_eval = sum(it["eval_krw"] for it in dedup.values())
        total_invest = sum(it["avg_krw"] * it["qty"] for it in dedup.values())
        return total_eval, total_invest
    except Exception:
        return 0.0, 0.0

# ================== 국내 잔고/리포트 ==================
def get_account_profit(only_changes=True):
    token = get_kis_access_token()
    _ = get_realized_holdings_data()

    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = {
        "authorization": f"Bearer {token}","appkey": KIS_APP_KEY,"appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8434R","Content-Type": "application/json"
    }
    params = {
        "CANO": KIS_ACCOUNT_NO[:8], "ACNT_PRDT_CD": KIS_ACCOUNT_NO[9:], "INQR_DVSN": "02", "UNPR_DVSN": "01",
        "AFHR_FLPR_YN": "N", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "OFL_YN": "N",
        "PRCS_DVSN": "00", "CTX_AREA_FK100": "P", "CTX_AREA_NK100": ""
    }
    res = _kis_api_request("GET", url, headers, params, timeout=15, max_retries=3, label="잔고 API")
    if res.get("rt_cd") != "0":
        raise Exception(f"[잔고 API 실패] {res.get('msg1', res)}")
    output = res.get("output1", [])
    if not output:
        return "📭 보유 중인 종목이 없습니다."

    last_json = r.get("LAST_HOLDINGS") if r else None
    last = json.loads(last_json) if last_json else {}
    new_holdings = {}; parsed_items = []
    total_profit = total_eval = total_invest = 0; changes = []

    for item in output:
        try:
            qty = safe_int(item.get("hldg_qty"))
            if qty == 0: continue
            name = item.get("prdt_name","알수없음"); code = item.get("pdno","")
            avg_price = safe_float(item.get("pchs_avg_pric")); cur_price = safe_float(item.get("prpr"))
            eval_amt = safe_int(item.get("evlu_amt")); profit = safe_int(item.get("evlu_erng_amt"))
            rate = safe_float(item.get("evlu_pfls_rt"))
            if eval_amt == 0: eval_amt = int(qty * cur_price)
            invest_amt = int(qty * avg_price)
            if profit == 0: profit = eval_amt - invest_amt
            if rate == 0 and avg_price > 0: rate = (profit / invest_amt) * 100
            investor_flow = get_market_summary(token, code)
            new_holdings[name] = qty
            parsed_items.append({"name":name,"qty":qty,"avg":avg_price,"cur":cur_price,
                                 "eval":eval_amt,"profit":profit,"rate":rate,"flow":investor_flow})
            total_profit += profit; total_eval += eval_amt; total_invest += invest_amt
            old_qty = last.get(name,0)
            if qty != old_qty:
                diff = qty - old_qty; arrow = "🟢 증가(국내)" if diff>0 else "🔴 감소(국내)"
                realized_est = abs(diff)*(cur_price-avg_price) if diff<0 else 0
                changes.append(
                    f"{name} 수량 {arrow}: {old_qty} → {qty}주\n"
                    f"┗ 수익금: {profit:,}원 | 수익률: {rate:.2f}%"
                    + (f"\n┗ 매도 추정 수익: {int(realized_est):,}원" if diff<0 else "")
                )
        except Exception as e:
            print(f"[파싱 오류] {e}"); traceback.print_exc(); continue

    parsed_items.sort(key=lambda x: x.get("eval",0), reverse=True)
    if r: r.set("LAST_HOLDINGS", json.dumps(new_holdings))
    if only_changes:
        return ("📌 [국내 잔고 변동]\n" + "\n".join(changes)) if changes else ""

    # output2에서 계좌 요약 추출 (가장 정확한 데이터)
    acct_summary = res.get("output2", [{}])
    if isinstance(acct_summary, list) and acct_summary:
        acct_summary = acct_summary[0]
    cash = safe_int(acct_summary.get("dnca_tot_amt", "0"))
    kr_total_assets = safe_int(acct_summary.get("tot_evlu_amt", "0"))  # 총평가(보유+예수금)
    kr_buy_total = safe_int(acct_summary.get("pchs_amt_smtl_amt", "0"))  # 매입금액 합계
    kr_eval_total = safe_int(acct_summary.get("evlu_amt_smtl_amt", "0"))  # 평가금액 합계
    kr_pl_total = safe_int(acct_summary.get("evlu_pfls_smtl_amt", "0"))  # 평가손익 합계
    prev_total = safe_int(acct_summary.get("bfdy_tot_asst_evlu_amt", "0"))  # 전일 총자산
    day_change = safe_int(acct_summary.get("asst_icdc_amt", "0"))  # 오늘 자산 증감

    # 해외 평가
    ovrs_eval, ovrs_invest = _get_total_overseas_eval()
    ovrs_profit = ovrs_eval - ovrs_invest

    # 전체 총자산
    grand_total = kr_total_assets + ovrs_eval

    # 연초 자산 자동 저장
    save_initial_assets_if_needed(int(grand_total))

    # ── 보유 종목 리포트 ──
    now_ts = datetime.now(KST).strftime("%m/%d %H:%M")
    report = ""
    if changes: report += "📌 [국내 잔고 변동]\n" + "\n".join(changes) + "\n\n"
    report += f"{'━'*28}\n📊 [국내 보유 종목] {now_ts} 기준"
    for it in parsed_items:
        status_icon = "🟢" if it['profit'] >= 0 else "🔴"
        report += (
            f"\n{status_icon} {it['name']} ({it['qty']}주)"
            f"\n  평균 {int(it['avg']):,} → 현재 {int(it['cur']):,}원"
            f"\n  평가 {_fmt_won_short(it['eval'])} | {status_icon} {it['profit']:+,}원 ({it['rate']:+.2f}%)"
            + (f"\n  {it['flow']}" if it["flow"] else "")
        )

    # 해외 보유 있으면 표시
    if ovrs_eval > 0:
        ovrs_rate = (ovrs_profit / ovrs_invest * 100) if ovrs_invest else 0.0
        ov_icon = "🟢" if ovrs_profit >= 0 else "🔴"
        report += (
            f"\n\n{ov_icon} 해외 평가손익: {_fmt_amount_won(ovrs_profit)} ({ovrs_rate:+.2f}%)"
            f"\n┗ 평가: {_fmt_amount_won(ovrs_eval)} / 원금: {_fmt_amount_won(ovrs_invest)}"
        )

    # ── 총 자산 현황 ──
    kr_pl_icon = "🟢" if kr_pl_total >= 0 else "🔴"
    kr_pl_rate = (kr_pl_total / kr_buy_total * 100) if kr_buy_total > 0 else 0.0
    day_icon = "🟢" if day_change >= 0 else "🔴"

    report += (
        f"\n\n{'━'*28}"
        f"\n💼 [총 자산] {_fmt_won_short(grand_total)}"
        f"\n  국내 보유 {_fmt_won_short(kr_eval_total)} (원금 {_fmt_won_short(kr_buy_total)})"
        f"\n  예수금 {_fmt_won_short(cash)}"
    )
    if ovrs_eval > 0:
        report += f"\n  해외 {_fmt_won_short(ovrs_eval)}"
    report += (
        f"\n  {kr_pl_icon} 평가손익 {_fmt_won_short(kr_pl_total, sign=True)} ({kr_pl_rate:+.2f}%)"
        f"\n  {day_icon} 전일비 {_fmt_won_short(day_change, sign=True)}"
    )

    # ── 실현손익 (매도 확정, KIS API TTTC8715R) ──
    now_dt = datetime.now(KST)
    today_str = now_dt.strftime("%Y%m%d")
    year = current_year()

    try:
        ten_y_dt = now_dt - timedelta(days=3650)
        all_realized, all_realized_rate = _query_realized_profit_period(token, ten_y_dt.strftime("%Y%m%d"), today_str)
        year_realized, year_realized_rate = _query_realized_profit_period(token, year_start_date(), today_str)

        all_icon = "🟢" if all_realized >= 0 else "🔴"
        yr_icon = "🟢" if year_realized >= 0 else "🔴"

        report += (
            f"\n\n{'━'*28}"
            f"\n💰 [실현손익] 매도 확정"
            f"\n  {yr_icon} {year}년 누적: {_fmt_won_short(year_realized, sign=True)} ({year_realized_rate:+.2f}%)"
            f"\n  {all_icon} 전체 누적: {_fmt_won_short(all_realized, sign=True)} ({all_realized_rate:+.2f}%)"
        )
    except Exception as e:
        report += f"\n\n💰 실현손익 조회 오류: {e}"

    # ── 올해 자산 수익률 (KIS API 역산) ──
    # 연초 추정 자산 = 현재 총자산 - 올해 실현손익 - 현재 미실현 손익
    try:
        yr_realized_for_calc, _ = _query_realized_profit_period(token, year_start_date(), today_str)
        unrealized_now = kr_pl_total + int(ovrs_profit)
        estimated_initial = int(grand_total) - yr_realized_for_calc - unrealized_now

        # 역산한 연초 자산을 Redis에 저장 (최초 1회)
        initial_key = f"INITIAL_ASSETS_{year}"
        if r:
            existing = r.get(initial_key)
            if not existing or abs(int(existing) - int(grand_total)) < 1000:
                # 기존 값이 없거나, 오늘 저장한 부정확한 값이면 역산값으로 덮어쓰기
                r.set(initial_key, str(estimated_initial))

        ytd_profit = int(grand_total) - estimated_initial
        ytd_rate = (ytd_profit / estimated_initial * 100) if estimated_initial > 0 else 0.0
        ytd_icon = "🟢" if ytd_profit >= 0 else "🔴"
        report += (
            f"\n\n📅 [{year}년 수익률] {year}.01 ~ {now_dt.strftime('%m.%d')}"
            f"\n  연초 {_fmt_won_short(estimated_initial)} → 현재 {_fmt_won_short(grand_total)}"
            f"\n  {ytd_icon} {_fmt_won_short(ytd_profit, sign=True)} ({ytd_rate:+.2f}%)"
        )
    except Exception as e:
        report += f"\n\n📅 올해 자산 수익률 계산 오류: {e}"

    return report

def get_realized_holdings_data():
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance-rlz-pl"
    headers = {
        "authorization": f"Bearer {token}","appkey": KIS_APP_KEY,"appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8494R","custtype": "P","Content-Type": "application/json"
    }
    acct_raw = KIS_ACCOUNT_NO.replace("-", ""); cano, acct_cd = acct_raw[:8], acct_raw[8:]
    params = {
        "CANO": cano,"ACNT_PRDT_CD": acct_cd,"AFHR_FLPR_YN": "N","OFL_YN": "",
        "INQR_DVSN": "00","UNPR_DVSN": "01","FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N","PRCS_DVSN": "00","COST_ICLD_YN": "N",
        "CTX_AREA_FK100": "","CTX_AREA_NK100": ""
    }
    data = _kis_api_request("GET", url, headers, params, timeout=15, max_retries=3, label="실현손익 API")
    if data.get("rt_cd") != "0":
        raise Exception(f"[실현손익 API 실패] {data.get('msg1', data)}")
    output1 = data.get("output1", []); result = {}
    for item in output1:
        name = item.get("prdt_name",""); realized_profit = safe_int(item.get("evlu_pfls_amt"))
        result[name] = realized_profit
    return result

def get_yearly_realized_profit():
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-period-trade-profit"
    headers = {
        "authorization": f"Bearer {token}","appkey": KIS_APP_KEY,"appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8715R","custtype": "P","Content-Type": "application/json"
    }
    acct_raw = KIS_ACCOUNT_NO.replace("-", ""); cano, acct_cd = acct_raw[:8], acct_raw[8:]
    start_dt = year_start_date(); end_dt = datetime.now(KST).strftime("%Y%m%d")
    params = {
        "CANO": cano,"ACNT_PRDT_CD": acct_cd,"SORT_DVSN": "01","PDNO": "",
        "INQR_STRT_DT": start_dt,"INQR_END_DT": end_dt,"CBLC_DVSN": "00",
        "CTX_AREA_FK100": "","CTX_AREA_NK100": ""
    }
    data = _kis_api_request("GET", url, headers, params, timeout=15, max_retries=3, label="연간 실현손익")
    if data.get("rt_cd") != "0":
        raise Exception(f"[실현손익조회 실패] {data.get('msg1', data)}")
    output2 = data.get("output2", {})
    realized_profit = safe_int(output2.get("tot_rlzt_pfls","0"))
    realized_rate = safe_float(output2.get("tot_pftrt","0"))
    return realized_profit, realized_rate

# ================== ETF 신규상장 감지 (네이버 금융 API 기반) ==================
ETF_TAGS = {
    "레버리지":"레버리지","인버스":"인버스","나스닥":"나스닥","S&P":"S&P","미국":"미국",
    "2차전지":"2차전지","반도체":"반도체","배당":"배당","원유":"원유","금":"금",
    "중국":"중국","테크":"테크","코스닥":"코스닥","채권":"채권","선물":"선물",
    "ESG":"ESG","헬스케어":"헬스케어","AI":"AI","로봇":"로봇","2차전지":"배터리",
}

def _fetch_naver_etf_list() -> Dict[str, dict]:
    """네이버 금융 ETF 전종목 리스트 조회 (무료, 키 불필요)"""
    url = "https://finance.naver.com/api/sise/etfItemList.nhn"
    params = {"etfType": "0", "targetColumn": "market_sum", "sortOrder": "desc"}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=15)
        res.raise_for_status()
        data = res.json()
        items = data.get("result", {}).get("etfItemList", [])
        result = {}
        for it in items:
            code = it.get("itemcode", "")
            if code:
                result[code] = {
                    "name": it.get("itemname", ""),
                    "code": code,
                    "price": safe_int(it.get("nowVal", 0)),
                    "change_rate": safe_float(it.get("changeRate", 0)),
                    "market_cap": safe_int(it.get("marketSum", 0)),
                    "nav": safe_float(it.get("nav", 0)),
                    "volume": safe_int(it.get("quant", 0)),
                    "three_month_rate": safe_float(it.get("threeMonthEarnRate", 0)),
                }
        return result
    except Exception as e:
        print(f"[네이버 ETF API 오류] {e}")
        return {}

def _get_known_etf_codes() -> set:
    """Redis에 저장된 기존 ETF 코드 세트 반환"""
    if not r:
        return set()
    try:
        codes_json = r.get("KNOWN_ETF_CODES")
        if codes_json:
            return set(json.loads(codes_json))
    except Exception as e:
        print(f"[ETF 코드 로드 오류] {e}")
    return set()

def _save_known_etf_codes(codes: set):
    """현재 ETF 코드 세트를 Redis에 저장"""
    if r:
        try:
            r.set("KNOWN_ETF_CODES", json.dumps(list(codes)))
        except Exception as e:
            print(f"[ETF 코드 저장 오류] {e}")

def _tag_etf(name: str) -> str:
    tags = [v for k, v in ETF_TAGS.items() if k in name]
    return " | ".join(tags) if tags else ""

def detect_newly_listed_etfs() -> str:
    """네이버 ETF 목록을 이전 스냅샷과 비교해 신규 ETF 감지"""
    current_etfs = _fetch_naver_etf_list()
    if not current_etfs:
        return ""

    known_codes = _get_known_etf_codes()
    current_codes = set(current_etfs.keys())

    # 첫 실행이면 스냅샷 저장만 하고 리턴
    if not known_codes:
        _save_known_etf_codes(current_codes)
        print(f"[ETF] 초기 스냅샷 저장: {len(current_codes)}개 ETF")
        return ""

    new_codes = current_codes - known_codes
    _save_known_etf_codes(current_codes)

    if not new_codes:
        return ""

    # 중복 알림 방지
    ymd = datetime.now(KST).strftime("%Y%m%d")
    alerted_key = f"ETF_NEW_ALERTED:{ymd}"
    already_alerted = set(r.smembers(alerted_key) or []) if r else set()
    new_codes -= already_alerted
    if not new_codes:
        return ""

    msgs = []
    for code in sorted(new_codes):
        etf = current_etfs.get(code, {})
        name = etf.get("name", code)
        tags = _tag_etf(name)
        price = etf.get("price", 0)
        market_cap = etf.get("market_cap", 0)
        nav = etf.get("nav", 0)

        msg = (
            f"🆕 신규 상장 ETF 감지\n"
            f"종목명: {name} ({code})"
            + (f" [{tags}]" if tags else "") +
            f"\n┗ 현재가: {price:,}원 | 시가총액: {market_cap:,}억원"
            f"\n┗ NAV: {nav:,.1f}원"
        )
        msgs.append(msg)
        if r:
            r.sadd(alerted_key, code)
            r.expire(alerted_key, 7 * 24 * 3600)

    return "\n\n".join(msgs) if msgs else ""

def _parse_listing_date_from_dart(rcept_no: str) -> str:
    """DART 일괄신고서 '상장 및 매매에 관한 사항' 섹션에서 상장예정일 추출.
    반환: 'YYYYMMDD' 형식 문자열 또는 빈 문자열."""
    try:
        # 1) 문서 메타 페이지에서 dcmNo, eleId 추출
        meta_url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        meta_res = requests.get(meta_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        # makeToc에서 첫 번째 dcmNo 추출 (정확한 문서번호)
        dcm_match = _re_module.search(r"node\d+\['dcmNo'\]\s*=\s*['\"](\d+)", meta_res.text)
        if not dcm_match:
            return ""
        dcm_no = dcm_match.group(1)

        # 상장 관련 섹션의 eleId 찾기
        texts = _re_module.findall(r"node\d+\['text'\]\s*=\s*['\"]([^'\"]+)", meta_res.text)
        ele_ids = _re_module.findall(r"node\d+\['eleId'\]\s*=\s*['\"](\d+)", meta_res.text)
        listing_ele = "11"  # 기본값
        for t, e in zip(texts, ele_ids):
            if "상장" in t and "매매" in t:
                listing_ele = e
                break

        # 2) 상장 및 매매에 관한 사항 섹션 가져오기
        viewer_url = (
            f"https://dart.fss.or.kr/report/viewer.do"
            f"?rcpNo={rcept_no}&dcmNo={dcm_no}&eleId={listing_ele}&offset=0&length=0&dtd=dart4.xsd"
        )
        doc_res = requests.get(viewer_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        text = doc_res.text

        # 3) 상장예정일 추출 — HTML 테이블 구조 파싱
        # 패턴: "상장(예정)일</TD> <TD...>2026. 4. 28</TD>" (테이블 셀 분리)
        td_pat = _re_module.search(
            r"상장\(?예정\)?일\s*</\s*T[DH]>\s*<T[DH][^>]*>\s*(\d{4})\s*[년.\s]+\s*(\d{1,2})\s*[월.\s]+\s*(\d{1,2})",
            text, _re_module.IGNORECASE
        )
        if td_pat:
            return f"{td_pat.group(1)}{int(td_pat.group(2)):02d}{int(td_pat.group(3)):02d}"

        # 인라인 텍스트 패턴 (같은 셀 안에 날짜가 있는 경우)
        # HTML 태그 제거 후 검색
        clean = _re_module.sub(r"<[^>]+>", " ", text)
        clean = _re_module.sub(r"&nbsp;", " ", clean)
        inline_patterns = [
            r"상장\(?예정\)?일[^\d]*(\d{4})\s*[년.\s]+\s*(\d{1,2})\s*[월.\s]+\s*(\d{1,2})",
            r"상장예정일[^\d]*(\d{4})\s*[년.\s]+\s*(\d{1,2})\s*[월.\s]+\s*(\d{1,2})",
            r"상장일[^\d]*(\d{4})\s*[년.\s]+\s*(\d{1,2})\s*[월.\s]+\s*(\d{1,2})",
        ]
        for pat in inline_patterns:
            m = _re_module.search(pat, clean)
            if m:
                return f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3)):02d}"
        return ""
    except Exception as e:
        print(f"[DART 상장일 파싱 오류] {rcept_no}: {e}")
        return ""

def _fetch_dart_upcoming_etfs(days_back: int = 14) -> List[dict]:
    """DART 전자공시에서 최근 신규 ETF 일괄신고서 조회 (상장 예정) + 상장예정일 추출"""
    if not DART_API_KEY:
        return []
    import re
    url = "https://opendart.fss.or.kr/api/list.json"
    now = datetime.now(KST)
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": (now - timedelta(days=days_back)).strftime("%Y%m%d"),
        "end_de": now.strftime("%Y%m%d"),
        "pblntf_ty": "G",  # 펀드공시
        "page_count": 100,
    }
    try:
        res = requests.get(url, params=params, timeout=15).json()
        if res.get("status") != "000":
            print(f"[DART API 오류] {res.get('message','')}")
            return []
        results = []
        for item in res.get("list", []):
            title = item.get("report_nm", "")
            # 일괄신고서 + 상장지수 + 기재정정 아닌 것 = 신규 ETF 신고
            if "일괄신고서" in title and "상장지수" in title and "기재정정" not in title:
                # ETF 이름 추출
                match = re.search(r"\(([^)]*상장지수[^)]*)\)", title)
                etf_name = match.group(1) if match else title
                # 브랜드명 추출 (TIGER, KODEX, ACE 등)
                brand_match = re.search(r"(TIGER|KODEX|ACE|KBSTAR|SOL|ARIRANG|HANARO|KOSEF|PLUS|RISE|KIWOOM|파워)", etf_name)
                brand = brand_match.group(1) if brand_match else ""
                rcept_no = item.get("rcept_no", "")

                # Redis 캐시 확인 (상장예정일)
                cache_key = f"DART_LISTING_DATE:{rcept_no}"
                listing_date = ""
                if r:
                    cached = r.get(cache_key)
                    if cached:
                        listing_date = cached if isinstance(cached, str) else cached.decode()
                if not listing_date:
                    listing_date = _parse_listing_date_from_dart(rcept_no)
                    if listing_date and r:
                        r.set(cache_key, listing_date, ex=60 * 24 * 3600)  # 60일 캐시
                    time.sleep(0.3)  # DART 서버 부하 방지

                results.append({
                    "date": item.get("rcept_dt", ""),
                    "corp": item.get("corp_name", ""),
                    "etf_name": etf_name,
                    "brand": brand,
                    "title": title,
                    "rcept_no": rcept_no,
                    "listing_date": listing_date,  # YYYYMMDD 또는 ""
                })
        return results
    except Exception as e:
        print(f"[DART 조회 오류] {e}")
        return []

def get_upcoming_etf_report() -> str:
    """DART 기반 상장 예정 ETF 리포트 (정확한 상장예정일 포함)"""
    etfs = _fetch_dart_upcoming_etfs(days_back=30)
    if not etfs:
        return ""

    now_str = datetime.now(KST).strftime("%Y%m%d")
    # 이미 상장된 ETF 제외 (상장예정일이 과거인 것)
    etfs = [e for e in etfs if not e["listing_date"] or e["listing_date"] >= now_str]
    if not etfs:
        return ""

    lines = [f"{'━'*28}", f"📋 [상장 예정 ETF] DART 증권신고서 기반"]
    for e in etfs:
        dt = f"{e['date'][:4]}.{e['date'][4:6]}.{e['date'][6:]}"
        tags = _tag_etf(e["etf_name"])
        # 상장예정일 포맷
        if e["listing_date"]:
            ld = e["listing_date"]
            listing_str = f"{ld[:4]}.{ld[4:6]}.{ld[6:]}"
        else:
            listing_str = "미정"
        news = _search_etf_news(e["etf_name"])
        lines.append(
            f"\n🆕 {e['etf_name']}"
            + (f" [{tags}]" if tags else "")
            + f"\n┗ 운용사: {e['corp']} | 신고일: {dt}"
            + f"\n┗ 📅 상장예정일: {listing_str}"
        )
        if news:
            lines.append("┗ 관련 뉴스:")
            for n in news:
                lines.append(f"  · {n}")

    return "\n".join(lines)

def _get_etfs_listing_this_week() -> List[dict]:
    """금주 상장 예정인 ETF 목록 반환"""
    now = datetime.now(KST)
    mon = _monday_of_week(now.date())
    fri = _friday_of_week(now.date())
    mon_str = mon.strftime("%Y%m%d")
    fri_str = fri.strftime("%Y%m%d")
    etfs = _fetch_dart_upcoming_etfs(days_back=45)
    return [e for e in etfs if e.get("listing_date") and mon_str <= e["listing_date"] <= fri_str]

def _get_etfs_listing_today() -> List[dict]:
    """오늘 상장 예정인 ETF 목록 반환"""
    today_str = datetime.now(KST).strftime("%Y%m%d")
    etfs = _fetch_dart_upcoming_etfs(days_back=45)
    return [e for e in etfs if e.get("listing_date") == today_str]

def get_weekly_listing_etf_report() -> str:
    """금주 상장 예정 ETF 요약 리포트 (주간 첫 거래일 발송)"""
    etfs = _get_etfs_listing_this_week()
    if not etfs:
        return ""
    now = datetime.now(KST)
    mon = _monday_of_week(now.date())
    fri = _friday_of_week(now.date())
    week_str = f"{mon.strftime('%m/%d')}~{fri.strftime('%m/%d')}"

    lines = [f"{'━'*28}", f"📅 [금주 상장 ETF] {week_str}"]
    for e in etfs:
        ld = e["listing_date"]
        listing_str = f"{ld[:4]}.{ld[4:6]}.{ld[6:]}"
        # 요일 계산
        ld_date = date(int(ld[:4]), int(ld[4:6]), int(ld[6:]))
        weekday_kr = ["월", "화", "수", "목", "금", "토", "일"][ld_date.weekday()]
        tags = _tag_etf(e["etf_name"])
        news = _search_etf_news(e["etf_name"])
        lines.append(
            f"\n🆕 {e['etf_name']}"
            + (f" [{tags}]" if tags else "")
            + f"\n┗ 운용사: {e['corp']}"
            + f"\n┗ 📅 상장일: {listing_str} ({weekday_kr})"
        )
        if news:
            lines.append("┗ 관련 뉴스:")
            for n in news:
                lines.append(f"  · {n}")
    lines.append(f"\n💡 상장 당일 장 시작 전 리마인드 알림이 발송됩니다.")
    return "\n".join(lines)

def get_today_listing_etf_reminder() -> str:
    """오늘 상장 ETF 리마인드 알림 (상장 당일 08:10 발송)"""
    etfs = _get_etfs_listing_today()
    if not etfs:
        return ""
    today = datetime.now(KST).strftime("%Y-%m-%d")
    lines = [f"{'━'*28}", f"🔔 [오늘 상장 ETF 리마인드] {today}"]
    for e in etfs:
        tags = _tag_etf(e["etf_name"])
        news = _search_etf_news(e["etf_name"])
        lines.append(
            f"\n🚀 {e['etf_name']}"
            + (f" [{tags}]" if tags else "")
            + f"\n┗ 운용사: {e['corp']}"
            + f"\n┗ 📅 오늘 상장! 09:00 거래 시작"
        )
        if news:
            lines.append("┗ 관련 뉴스:")
            for n in news:
                lines.append(f"  · {n}")
    lines.append(f"\n💡 상장 초기 유동성이 낮을 수 있습니다. 거래량 확인 후 투자를 검토하세요.")
    return "\n".join(lines)

def _search_etf_news(etf_name: str, max_results: int = 3) -> List[str]:
    """Google News RSS로 ETF 관련 뉴스 검색. 마크다운 링크 형태 반환."""
    import re as _re
    headers = {"User-Agent": "Mozilla/5.0"}

    brand_match = _re.search(r"(TIGER|KODEX|ACE|KBSTAR|SOL|ARIRANG|HANARO|RISE|PLUS|KIWOOM)", etf_name)
    brand = brand_match.group(1) if brand_match else ""

    theme = _re.sub(
        r"(미래에셋|한국투자|삼성|KB|하나|신한|키움|NH|증권|상장지수|투자신탁|주식|채권혼합|액티브|신탁형|\[.*?\]|\(.*?\)|\d+Q?)",
        "", etf_name
    ).strip().replace(brand, "").strip()

    queries = [f"{brand} {theme} ETF", f"{theme} ETF 상장"]

    for query in queries:
        query = query.strip()
        if len(query) < 5:
            continue
        try:
            url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
            res = requests.get(url, headers=headers, timeout=8)
            if res.status_code != 200:
                continue
            items_raw = _re.findall(
                r"<item>.*?<title>(.*?)</title>.*?<link>(.*?)</link>",
                res.text, _re.DOTALL
            )
            results = []
            for raw_title, link in items_raw:
                title = _re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", raw_title).strip()
                link = link.strip()
                source = ""
                if " - " in title:
                    parts = title.rsplit(" - ", 1)
                    title = parts[0].strip()
                    source = parts[1].strip()
                if len(title) > 10 and "Google" not in title:
                    display = f"{title} ({source})" if source else title
                    # 마크다운 특수문자 이스케이프 (괄호)
                    safe_display = display.replace("[", "\\[").replace("]", "\\]")
                    safe_link = link.replace(")", "%29")
                    entry = f"[{safe_display}]({safe_link})"
                    results.append(entry)
                if len(results) >= max_results:
                    break
            if results:
                return results
        except Exception:
            continue
    return []

def get_new_etf_daily_report() -> str:
    """신규 상장 ETF 감지 → 상세 리포트 + 관련 뉴스"""
    if not is_trading_day():
        return ""

    current_etfs = _fetch_naver_etf_list()
    if not current_etfs:
        return ""

    known_codes = _get_known_etf_codes()
    current_codes = set(current_etfs.keys())

    # 첫 실행이면 스냅샷 저장만
    if not known_codes:
        _save_known_etf_codes(current_codes)
        return ""

    new_codes = current_codes - known_codes
    if not new_codes:
        # 스냅샷은 신규 발견 시에만 갱신 (장중 재체크 가능하도록)
        return ""

    # 신규 발견 → 스냅샷 갱신
    _save_known_etf_codes(current_codes)

    # 당일 중복 방지 (Redis)
    ymd = datetime.now(KST).strftime("%Y%m%d")
    alerted_key = f"ETF_NEW_DAILY:{ymd}"
    already = set(r.smembers(alerted_key) or []) if r else set()
    new_codes -= already
    if not new_codes:
        return ""

    today = datetime.now(KST).strftime("%Y-%m-%d")
    lines = [f"{'━'*28}", f"🆕 [신규 상장 ETF 리포트] {today}"]

    for code in sorted(new_codes):
        etf = current_etfs.get(code, {})
        name = etf.get("name", code)
        price = etf.get("price", 0)
        nav = etf.get("nav", 0)
        mcap = etf.get("market_cap", 0)
        volume = etf.get("volume", 0)
        tags = _tag_etf(name)

        lines.append(
            f"\n📌 {name} ({code})"
            + (f" [{tags}]" if tags else "")
            + f"\n┗ 현재가: {price:,}원 | NAV: {nav:,.1f}원"
            f"\n┗ 시총: {mcap:,}억원 | 거래량: {volume:,}주"
        )

        # 관련 뉴스
        news = _search_etf_news(name)
        if news:
            lines.append("┗ 관련 뉴스:")
            for n in news:
                lines.append(f"  · {n}")

        if r:
            r.sadd(alerted_key, code)
            r.expire(alerted_key, 3 * 24 * 3600)

    lines.append(f"\n💡 신규 ETF는 상장 초기 유동성이 낮을 수 있습니다. 거래량 확인 후 투자를 검토하세요.")
    return "\n".join(lines)

def _fetch_market_indices() -> str:
    """주요 시장 지수 조회 (네이버 API)"""
    headers = {"User-Agent": "Mozilla/5.0"}
    lines = []
    # KOSPI, KOSDAQ
    for code, name in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥")]:
        try:
            res = requests.get(f"https://m.stock.naver.com/api/index/{code}/basic", headers=headers, timeout=5)
            if res.status_code == 200:
                d = res.json()
                price = d.get("closePrice", "")
                change = d.get("compareToPreviousClosePrice", "")
                direction = d.get("compareToPreviousPrice", {}).get("name", "")
                rate = d.get("fluctuationsRatio", "")
                icon = "🟢" if direction == "RISING" else "🔴" if direction == "FALLING" else "⚪"
                sign = "+" if direction == "RISING" else "-" if direction == "FALLING" else ""
                lines.append(f"  {icon} {name} {price} ({sign}{change}, {sign}{rate}%)")
        except Exception:
            pass
    return "\n".join(lines) if lines else ""

def get_weekly_etf_briefing() -> str:
    """주간 브리핑: 시장 현황 + 신규 ETF (매주 첫 거래일)"""
    now = datetime.now(KST)
    mon = _monday_of_week(now.date())
    fri = _friday_of_week(now.date())
    week_str = f"{mon.strftime('%m/%d')}~{fri.strftime('%m/%d')}"

    lines = [f"{'━'*28}", f"📊 [주간 ETF 브리핑] {week_str}"]

    # 시장 지수
    indices = _fetch_market_indices()
    if indices:
        lines.append(f"\n📈 시장 현황")
        lines.append(indices)

    # 금주 상장 ETF (정확한 상장예정일)
    this_week_etfs = _get_etfs_listing_this_week()
    if this_week_etfs:
        lines.append(f"\n🗓️ 금주 상장 예정")
        for e in this_week_etfs:
            ld = e["listing_date"]
            ld_date = date(int(ld[:4]), int(ld[4:6]), int(ld[6:]))
            weekday_kr = ["월", "화", "수", "목", "금", "토", "일"][ld_date.weekday()]
            tags = _tag_etf(e["etf_name"])
            lines.append(
                f"  🆕 {e['etf_name']}"
                + (f" [{tags}]" if tags else "")
                + f"\n     {ld[4:6]}/{ld[6:]}({weekday_kr}) | {e['corp']}"
            )
    else:
        lines.append(f"\n🗓️ 금주 상장 예정: 없음")

    # 신규 ETF (이미 상장된 것)
    new_etf_msg = detect_newly_listed_etfs()
    if new_etf_msg:
        lines.append(f"\n{new_etf_msg}")

    # 상장 예정 ETF (DART 신고서 기반 — 향후 예정)
    upcoming = get_upcoming_etf_report()
    if upcoming:
        lines.append(f"\n{upcoming}")

    return "\n".join(lines)

def get_monthly_etf_report() -> str:
    """월간 ETF 수익률 리포트: 3개월 수익률 TOP/WORST (매월 첫 거래일)"""
    current_etfs = _fetch_naver_etf_list()
    if not current_etfs:
        return "📊 월간 ETF 리포트: 데이터 조회 실패"

    now = datetime.now(KST)
    month_str = now.strftime("%Y년 %m월")
    items = list(current_etfs.values())

    # 인버스/레버리지 제외한 일반 ETF만 수익률 순위
    normal_etfs = [it for it in items if not any(kw in it["name"] for kw in ["인버스", "선물인버스"])]

    # 3개월 수익률 TOP 5
    by_return = sorted(normal_etfs, key=lambda x: x.get("three_month_rate", 0), reverse=True)[:5]
    # 3개월 수익률 WORST 5
    by_loss = sorted(normal_etfs, key=lambda x: x.get("three_month_rate", 0))[:5]

    lines = [f"{'━'*28}", f"📊 [{month_str} ETF 월간 리포트]"]

    lines.append(f"\n🏆 3개월 수익률 TOP 5")
    for i, it in enumerate(by_return, 1):
        r3m = it.get("three_month_rate", 0)
        tags = _tag_etf(it["name"])
        tag_str = f" [{tags}]" if tags else ""
        lines.append(f"  {i}. 🟢 {it['name']}{tag_str}  {r3m:+.2f}%")

    lines.append(f"\n📉 3개월 수익률 WORST 5")
    for i, it in enumerate(by_loss, 1):
        r3m = it.get("three_month_rate", 0)
        tags = _tag_etf(it["name"])
        tag_str = f" [{tags}]" if tags else ""
        lines.append(f"  {i}. 🔴 {it['name']}{tag_str}  {r3m:+.2f}%")

    lines.append(f"\n📌 전체 ETF {len(items)}개 (인버스 제외 기준)")
    return "\n".join(lines)

def _monday_of_week(d: date) -> date:
    return d - timedelta(days=d.weekday())

def _friday_of_week(d: date) -> date:
    return _monday_of_week(d) + timedelta(days=4)

def _is_first_trading_day_of_week(now_dt: datetime = None) -> bool:
    now = now_dt or datetime.now(KST)
    if not is_trading_day(now):
        return False
    mon = _monday_of_week(now.date())
    d = mon
    while d < now.date():
        dt = datetime(d.year, d.month, d.day, tzinfo=KST)
        if is_trading_day(dt):
            return False
        d += timedelta(days=1)
    return True

# ================== 외국인/기관 추세 ==================
def _call_foreign_institution_total(fid_input_iscd: str = "0000",
                                   fid_div_cls: str = "1",
                                   fid_rank_sort: str = "0",
                                   fid_etc_cls: str = "1") -> List[dict]:
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/foreign-institution-total"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHPTJ04400000",
        "custtype": "P",
        "Content-Type": "application/json"
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "V",
        "FID_COND_SCR_DIV_CODE": "16449",
        "FID_INPUT_ISCD": fid_input_iscd,
        "FID_DIV_CLS_CODE": fid_div_cls,
        "FID_RANK_SORT_CLS_CODE": fid_rank_sort,
        "FID_ETC_CLS_CODE": fid_etc_cls
    }
    try:
        res = requests.get(url, headers=headers, params=params, timeout=12)
        res.raise_for_status()
        data = res.json()
        if data.get("rt_cd") != "0":
            print(f"[foreign-institution-total 실패] {data.get('msg1', data)}")
            return []
        out = data.get("Output")
        if out is None:
            return []
        if isinstance(out, list):
            return out
        elif isinstance(out, dict):
            return [out]
        else:
            return []
    except Exception as e:
        print(f"[FHPTJ04400000 오류] {e}")
        return []

def snapshot_foreign_flow_all_codes():
    if not is_trading_day():
        return
    today = datetime.now(KST).strftime("%Y%m%d")
    universe_rows = []
    for market in ("0000", "0001", "1001"):
        rows = _call_foreign_institution_total(fid_input_iscd=market,
                                               fid_div_cls="1",
                                               fid_rank_sort="0",
                                               fid_etc_cls="1")
        universe_rows.extend(rows)
        time.sleep(0.25)
    frgn_per_code = {}
    orgn_per_code = {}
    for row in universe_rows:
        code = (row.get("mksc_shrn_iscd") or "").strip()
        if not code:
            continue
        frgn_qty = safe_int(row.get("frgn_ntby_qty"))
        orgn_qty = safe_int(row.get("orgn_ntby_qty"))
        if code not in frgn_per_code or frgn_qty > frgn_per_code[code]:
            frgn_per_code[code] = frgn_qty
        if code not in orgn_per_code or orgn_qty > orgn_per_code[code]:
            orgn_per_code[code] = orgn_qty
    for code, qty in frgn_per_code.items():
        try:
            if r:
                r.hset(f"FRGN_FLOW:{code}", today, qty)
                r.expire(f"FRGN_FLOW:{code}", 120*24*3600)
        except Exception as e:
            print(f"[Redis 기록 오류] FRGN {code} / {e}")
    for code, qty in orgn_per_code.items():
        try:
            if r:
                r.hset(f"ORGN_FLOW:{code}", today, qty)
                r.expire(f"ORGN_FLOW:{code}", 120*24*3600)
        except Exception as e:
            print(f"[Redis 기록 오류] ORGN {code} / {e}")

def _get_flow_series(prefix: str, code: str, days: int = 7) -> List[Tuple[str,int]]:
    """Redis에서 수급 시계열 조회. prefix: 'FRGN_FLOW' 또는 'ORGN_FLOW'"""
    if not r: return []
    all_kv = r.hgetall(f"{prefix}:{code}") or {}
    if not all_kv: return []
    items = sorted(((k,v) for k,v in all_kv.items()), key=lambda x: x[0])
    items = items[-days:]
    return [(d, safe_int(v)) for d, v in items]

def _get_foreign_series(code: str, days: int = 7) -> List[Tuple[str,int]]:
    return _get_flow_series("FRGN_FLOW", code, days)

def _is_sustained_growth(series: List[int]) -> bool:
    if len(series) < 3:
        return False
    pos_days = sum(1 for v in series[-5:] if v > 0) if len(series)>=5 else sum(1 for v in series if v>0)
    inc3 = len(series)>=3 and (series[-3] < series[-2] < series[-1])
    return (pos_days >= 4) and inc3

def _lookup_name(code: str) -> str:
    cache_key = f"STOCK_NAME:{code}"
    if r:
        cached = r.get(cache_key)
        if cached: return cached
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100", "Content-Type": "application/json"
    }
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=8).json()
        if res.get("rt_cd") == "0":
            out = res.get("output", {}) or {}
            nm = out.get("hts_kor_isnm") or out.get("prdt_name") or code
            if r: r.set(cache_key, nm, ex=30*24*3600)
            return nm
    except Exception as e:
        print(f"[inquire-price 오류] {code} / {e}")
    return code

def _build_trend_section(prefix: str, label: str, days: int = 7, topn: int = FOREIGN_TREND_TOPN) -> str:
    """수급 추세 TOP N 생성. prefix: 'FRGN_FLOW' 또는 'ORGN_FLOW'"""
    if not r:
        return ""
    codes = []
    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{prefix}:*", count=500)
            for k in keys:
                if k.startswith(f"{prefix}:"):
                    codes.append(k.split(f"{prefix}:", 1)[1])
            if cursor == 0:
                break
    except Exception as e:
        print(f"[Redis scan 오류] {prefix} / {e}")
        return ""
    scored = []
    for code in codes:
        series_kv = _get_flow_series(prefix, code, days=days)
        if not series_kv:
            continue
        values = [v for _,v in series_kv]
        if not _is_sustained_growth(values):
            continue
        score = sum(max(0,v) for v in values) + (2*max(0, values[-1]))
        name = _lookup_name(code)
        scored.append((score, code, name, values))
    if not scored:
        return f"{label}: 조건 충족 종목 없음 (데이터 누적 중)"
    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:max(1, topn)]
    lines = [f"{label} 최근 {days}일 상승 추세 TOP"]
    for rank,(score,code,name,vals) in enumerate(top, start=1):
        # 미니 트렌드 바: ▁▂▃▅▇ 또는 ▼ 로 시각화
        trend_chars = []
        max_v = max(abs(v) for v in vals) if vals else 1
        for v in vals:
            if v <= 0:
                trend_chars.append("▼")
            else:
                level = int(v / max(max_v, 1) * 4)
                trend_chars.append(["▁","▂","▃","▅","▇"][min(level, 4)])
        trend_bar = "".join(trend_chars)
        total = sum(v for v in vals)
        lines.append(f"  {rank}. {name} ({code})")
        lines.append(f"     {trend_bar}  합계 {total:+,}주")
    return "\n".join(lines)

def build_foreign_trend_topN(days: int = 7, topn: int = FOREIGN_TREND_TOPN) -> str:
    if not r:
        return "📈 수급 추세: 저장소(Redis) 미설정"
    frgn = _build_trend_section("FRGN_FLOW", "🌍 외국인", days, topn)
    orgn = _build_trend_section("ORGN_FLOW", "🏛️ 기관", days, topn)
    parts = [p for p in [frgn, orgn] if p]
    return "\n\n".join(parts) if parts else "📈 수급 추세: 데이터 누적 중"

def build_daily_top_supply_demand(topn: int = 3) -> str:
    """당일 외국인/기관 순매수 TOP 종목"""
    if not is_trading_day():
        return ""
    rows = []
    for market in ("0000", "0001"):
        # 외국인 순매수 상위 (fid_div_cls=1, fid_rank_sort=0 = 순매수 상위)
        fetched = _call_foreign_institution_total(fid_input_iscd=market, fid_div_cls="1", fid_rank_sort="0")
        rows.extend(fetched)
        time.sleep(0.25)

    if not rows:
        return ""

    # 외국인 순매수 TOP
    frgn_list = []
    for row in rows:
        code = (row.get("mksc_shrn_iscd") or "").strip()
        name = row.get("hts_kor_isnm") or row.get("isnm") or code
        frgn_qty = safe_int(row.get("frgn_ntby_qty"))
        orgn_qty = safe_int(row.get("orgn_ntby_qty"))
        if code and frgn_qty != 0:
            frgn_list.append({"code": code, "name": name, "frgn": frgn_qty, "orgn": orgn_qty})

    if not frgn_list:
        return ""

    # 외국인 순매수 TOP 3
    frgn_buy = sorted(frgn_list, key=lambda x: x["frgn"], reverse=True)[:topn]
    # 기관 순매수 TOP 3
    orgn_buy = sorted(frgn_list, key=lambda x: x["orgn"], reverse=True)[:topn]

    lines = [f"{'━'*28}", f"📊 [당일 수급 TOP {topn}]"]
    lines.append(f"\n🌍 외국인 순매수")
    for i, it in enumerate(frgn_buy, 1):
        icon = "🟢" if it["frgn"] > 0 else "🔴"
        lines.append(f"  {i}. {icon} {it['name']}  {it['frgn']:+,}주")
    lines.append(f"\n🏛️ 기관 순매수")
    for i, it in enumerate(orgn_buy, 1):
        icon = "🟢" if it["orgn"] > 0 else "🔴"
        lines.append(f"  {i}. {icon} {it['name']}  {it['orgn']:+,}주")

    return "\n".join(lines)

# ================== 알림 작업 ==================
def _is_first_trading_day_of_month(now_dt: datetime = None) -> bool:
    """이번 달 첫 거래일인지 판별"""
    now = now_dt or datetime.now(KST)
    if not is_trading_day(now):
        return False
    d = date(now.year, now.month, 1)
    while d < now.date():
        dt = datetime(d.year, d.month, d.day, tzinfo=KST)
        if is_trading_day(dt):
            return False
        d += timedelta(days=1)
    return True

def job_weekly_etf_briefing():
    """매주 첫 거래일 08:10 — 주간 ETF 브리핑 (신규 상장 감지)"""
    now = datetime.now(KST)
    if not _is_first_trading_day_of_week(now):
        return
    # 중복 방지
    iso = now.isocalendar()
    year_week = f"{iso.year}-W{iso.week}"
    key = f"WEEKLY_ETF_SENT:{year_week}"
    if r and r.get(key):
        return
    try:
        msg = get_weekly_etf_briefing()
        if msg:
            send_alert_message(msg)
            if r:
                r.set(key, "1", ex=21 * 24 * 3600)
    except Exception as e:
        send_alert_message(f"❌ 주간 ETF 브리핑 오류: {e}")

def job_daily_new_etf_check():
    """매일 08:10 — 당일 신규 상장 ETF 상세 리포트"""
    if not is_trading_day():
        return
    try:
        msg = get_new_etf_daily_report()
        if msg:
            send_alert_message(msg)
    except Exception as e:
        send_alert_message(f"❌ 신규 ETF 체크 오류: {e}")

def job_weekly_listing_etf():
    """매주 첫 거래일 08:10 — 금주 상장 예정 ETF 알림"""
    now = datetime.now(KST)
    if not _is_first_trading_day_of_week(now):
        return
    iso = now.isocalendar()
    key = f"WEEKLY_LISTING_ETF:{iso.year}-W{iso.week}"
    if r and r.get(key):
        return
    try:
        msg = get_weekly_listing_etf_report()
        if msg:
            send_alert_message(msg)
            if r:
                r.set(key, "1", ex=14 * 24 * 3600)
    except Exception as e:
        send_alert_message(f"❌ 금주 상장 ETF 알림 오류: {e}")

def job_today_listing_etf_reminder():
    """매일 08:10 — 오늘 상장 ETF 리마인드 (요일 무관, 거래일만)"""
    if not is_trading_day():
        return
    ymd = datetime.now(KST).strftime("%Y%m%d")
    key = f"LISTING_REMIND:{ymd}"
    if r and r.get(key):
        return
    try:
        msg = get_today_listing_etf_reminder()
        if msg:
            send_alert_message(msg)
            if r:
                r.set(key, "1", ex=3 * 24 * 3600)
    except Exception as e:
        send_alert_message(f"❌ 상장 리마인드 오류: {e}")

def job_monthly_etf_report():
    """매월 첫 거래일 08:10 — 월간 ETF 수익률 리포트"""
    now = datetime.now(KST)
    if not _is_first_trading_day_of_month(now):
        return
    key = f"MONTHLY_ETF_SENT:{now.strftime('%Y-%m')}"
    if r and r.get(key):
        return
    try:
        msg = get_monthly_etf_report()
        if msg:
            send_alert_message(msg)
            if r:
                r.set(key, "1", ex=45 * 24 * 3600)
    except Exception as e:
        send_alert_message(f"❌ 월간 ETF 리포트 오류: {e}")

def job_daily_foreign_trend():
    """매일 08:20 외국인 수급 추세"""
    if not is_trading_day():
        return
    trend_msg = build_foreign_trend_topN(days=7, topn=FOREIGN_TREND_TOPN)
    if trend_msg:
        send_alert_message(trend_msg)

def job_snapshot_foreign_flow():
    try:
        snapshot_foreign_flow_all_codes()
    except Exception as e:
        send_alert_message(f"❌ 외국인 수급 스냅샷 실패: {e}")

# ================== 실시간 잔고 변동 루프 ==================
def check_holdings_change_loop():
    while not shutdown_event.is_set():
        try:
            if is_trading_day() and is_market_hour():
                rep_kr = get_account_profit(only_changes=True)
                if rep_kr:
                    send_alert_message(rep_kr)
            rep_ov = get_overseas_account_profit(only_changes=True)
            if rep_ov:
                send_alert_message(rep_ov)
        except Exception as e:
            send_alert_message(f"❌ 자동 잔고 체크 오류: {e}")
            traceback.print_exc()
        shutdown_event.wait(60)

# ================== 런 ==================
def _get_etf_volume_top3() -> str:
    """ETF 거래량 TOP 3"""
    etfs = _fetch_naver_etf_list()
    if not etfs:
        return ""
    items = sorted(etfs.values(), key=lambda x: x.get("volume", 0), reverse=True)[:3]
    lines = [f"{'━'*28}", "📈 [ETF 거래량 TOP 3]"]
    for i, it in enumerate(items, 1):
        rate = it.get("change_rate", 0)
        icon = "🟢" if rate >= 0 else "🔴"
        lines.append(f"  {i}. {icon} {it['name']}  {it['volume']:,}주 ({rate:+.2f}%)")
    return "\n".join(lines)

def get_account_profit_with_yearly_report():
    """종합 리포트 — 자산현황 + 수급 TOP + ETF 거래량"""
    main_report = get_account_profit(False)
    try:
        supply = build_daily_top_supply_demand(topn=3)
        if supply:
            main_report += "\n\n" + supply
    except Exception as e:
        main_report += f"\n\n📊 수급 TOP 조회 오류: {e}"
    try:
        etf_vol = _get_etf_volume_top3()
        if etf_vol:
            main_report += "\n\n" + etf_vol
    except Exception:
        pass
    return main_report

def _get_stock_daily_change(code: str) -> Optional[Dict]:
    """KIS inquire-price API로 종목의 당일 등락 정보 조회"""
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100", "Content-Type": "application/json"
    }
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=8).json()
        if res.get("rt_cd") != "0":
            return None
        out = res.get("output", {}) or {}
        return {
            "name": out.get("hts_kor_isnm") or code,
            "price": safe_int(out.get("stck_prpr")),              # 현재가
            "change": safe_int(out.get("prdy_vrss")),             # 전일대비
            "change_rate": safe_float(out.get("prdy_ctrt")),      # 전일대비율
            "volume": safe_int(out.get("acml_vol")),              # 누적거래량
            "trade_amount": safe_int(out.get("acml_tr_pbmn")),    # 누적거래대금
            "high": safe_int(out.get("stck_hgpr")),               # 고가
            "low": safe_int(out.get("stck_lwpr")),                # 저가
            "per": safe_float(out.get("per")),                    # PER
            "pbr": safe_float(out.get("pbr")),                    # PBR
            "w52_high": safe_int(out.get("stck_dryy_hgpr")),      # 52주 최고
            "w52_low": safe_int(out.get("stck_dryy_lwpr")),       # 52주 최저
        }
    except Exception as e:
        print(f"[inquire-price 오류] {code} / {e}")
        return None


def _get_holdings_codes() -> List[Tuple[str, str]]:
    """현재 보유종목의 (code, name) 리스트 반환"""
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8434R", "Content-Type": "application/json"
    }
    params = {
        "CANO": KIS_ACCOUNT_NO[:8], "ACNT_PRDT_CD": KIS_ACCOUNT_NO[9:], "INQR_DVSN": "02", "UNPR_DVSN": "01",
        "AFHR_FLPR_YN": "N", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "OFL_YN": "N",
        "PRCS_DVSN": "00", "CTX_AREA_FK100": "P", "CTX_AREA_NK100": ""
    }
    res = _kis_api_request("GET", url, headers, params, timeout=15, max_retries=3, label="잔고 API")
    if res.get("rt_cd") != "0":
        return []
    result = []
    for item in res.get("output1", []):
        qty = safe_int(item.get("hldg_qty"))
        if qty > 0:
            result.append((item.get("pdno", ""), item.get("prdt_name", ""), qty,
                           safe_float(item.get("pchs_avg_pric")), safe_int(item.get("evlu_amt"))))
    return result


def _get_consecutive_flow_top(prefix: str, label: str, topn: int = 5) -> str:
    """Redis 수급 시계열에서 연속 순매수 종목 TOP N 추출"""
    if not r:
        return ""
    codes = []
    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{prefix}:*", count=500)
            for k in keys:
                if k.startswith(f"{prefix}:"):
                    codes.append(k.split(f"{prefix}:", 1)[1])
            if cursor == 0:
                break
    except Exception:
        return ""

    scored = []
    for code in codes:
        series_kv = _get_flow_series(prefix, code, days=10)
        if len(series_kv) < 3:
            continue
        values = [v for _, v in series_kv]
        # 최근부터 연속 순매수 일수 계산
        consec = 0
        for v in reversed(values):
            if v > 0:
                consec += 1
            else:
                break
        if consec < 3:
            continue
        recent_total = sum(v for v in values[-consec:])
        name = _lookup_name(code)
        scored.append((consec, recent_total, code, name, values))

    if not scored:
        return ""

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    top = scored[:topn]
    lines = [f"{label} 연속 순매수 TOP"]
    for rank, (consec, total, code, name, vals) in enumerate(top, 1):
        trend_chars = []
        max_v = max(abs(v) for v in vals) if vals else 1
        for v in vals:
            if v <= 0:
                trend_chars.append("▼")
            else:
                level = int(v / max(max_v, 1) * 4)
                trend_chars.append(["▁", "▂", "▃", "▅", "▇"][min(level, 4)])
        trend_bar = "".join(trend_chars)
        lines.append(f"  {rank}. {name} — {consec}일 연속 매수")
        lines.append(f"     {trend_bar}  합계 {total:+,}주")
    return "\n".join(lines)


def _get_fx_summary() -> str:
    """주요 환율 요약"""
    lines = []
    for ccy, name in [("USD", "달러"), ("JPY", "엔(100)"), ("CNY", "위안")]:
        rate = get_fx_rate_ccykrw(ccy)
        if ccy == "JPY":
            lines.append(f"  💱 {name}: {rate * 100:,.1f}원")
        else:
            lines.append(f"  💱 {name}: {rate:,.1f}원")
    return "\n".join(lines)


def _build_stock_picks() -> str:
    """데이터 기반 다음날 주목 종목 선별

    선별 기준 (모두 팩트 데이터 기반, 추측 없음):
    1. 외국인+기관 동시 순매수 (당일)
    2. 외국인 3일+ 연속 순매수 + 당일 주가 상승
    3. 기관 3일+ 연속 순매수 + 당일 주가 상승
    4. 거래량 급증 + 외국인 순매수 (돌파 시그널)

    각 종목에 신뢰도 점수(S/A/B)를 부여:
    S: 3개 이상 조건 충족
    A: 2개 조건 충족
    B: 1개 조건 충족 (강한 신호)
    """
    if not r:
        return ""

    # 1단계: 외국인/기관 순매수 당일 데이터 수집 (KOSPI + KOSDAQ)
    all_rows = []
    for market in ("0000", "0001"):
        fetched = _call_foreign_institution_total(fid_input_iscd=market, fid_div_cls="1", fid_rank_sort="0")
        all_rows.extend(fetched)
        time.sleep(0.25)

    if not all_rows:
        return ""

    # code별 당일 수급 정리
    daily_flow = {}
    for row in all_rows:
        code = (row.get("mksc_shrn_iscd") or "").strip()
        if not code:
            continue
        name = row.get("hts_kor_isnm") or row.get("isnm") or code
        frgn = safe_int(row.get("frgn_ntby_qty"))
        orgn = safe_int(row.get("orgn_ntby_qty"))
        frgn_amt = safe_int(row.get("frgn_ntby_tr_pbmn"))  # 외국인 순매수 금액
        orgn_amt = safe_int(row.get("orgn_ntby_tr_pbmn"))  # 기관 순매수 금액
        if code not in daily_flow or abs(frgn) > abs(daily_flow[code].get("frgn", 0)):
            daily_flow[code] = {
                "name": name, "frgn": frgn, "orgn": orgn,
                "frgn_amt": frgn_amt, "orgn_amt": orgn_amt,
            }

    # 2단계: 후보 종목 평가
    candidates = []
    checked = 0
    for code, flow in daily_flow.items():
        # 기본 필터: 외국인 또는 기관 순매수가 있어야 함
        if flow["frgn"] <= 0 and flow["orgn"] <= 0:
            continue

        conditions = []
        score = 0

        # 조건1: 외국인+기관 동시 순매수
        if flow["frgn"] > 0 and flow["orgn"] > 0:
            conditions.append("외국인+기관 동시 순매수")
            score += 3

        # 조건2: 외국인 연속 순매수 (Redis 시계열)
        frgn_series = _get_flow_series("FRGN_FLOW", code, days=7)
        frgn_vals = [v for _, v in frgn_series] if frgn_series else []
        frgn_consec = 0
        for v in reversed(frgn_vals):
            if v > 0:
                frgn_consec += 1
            else:
                break
        if frgn_consec >= 3:
            conditions.append(f"외국인 {frgn_consec}일 연속 순매수")
            score += 2 + min(frgn_consec - 3, 2)  # 3일=2, 4일=3, 5일+=4

        # 조건3: 기관 연속 순매수
        orgn_series = _get_flow_series("ORGN_FLOW", code, days=7)
        orgn_vals = [v for _, v in orgn_series] if orgn_series else []
        orgn_consec = 0
        for v in reversed(orgn_vals):
            if v > 0:
                orgn_consec += 1
            else:
                break
        if orgn_consec >= 3:
            conditions.append(f"기관 {orgn_consec}일 연속 순매수")
            score += 2 + min(orgn_consec - 3, 2)

        if not conditions:
            continue

        # 조건4: 당일 시세 정보 (상위 후보만 조회 — API 호출 절약)
        if score >= 2 and checked < 30:
            checked += 1
            daily = _get_stock_daily_change(code)
            time.sleep(0.12)
            if daily:
                chg_rate = daily["change_rate"]
                if chg_rate > 0:
                    conditions.append(f"당일 +{chg_rate:.2f}% 상승")
                    score += 1

                # 52주 저가 구간이면 추가 점수
                if daily["w52_high"] and daily["w52_low"] and daily["w52_high"] > daily["w52_low"]:
                    pos = (daily["price"] - daily["w52_low"]) / (daily["w52_high"] - daily["w52_low"]) * 100
                    if pos <= 30:
                        conditions.append(f"52주 저가 구간 ({pos:.0f}%)")
                        score += 1
                    elif pos >= 85:
                        conditions.append(f"52주 고가 구간 ({pos:.0f}%)")
                        # 고가 구간은 점수 감점
                        score -= 1

                candidates.append({
                    "code": code, "name": flow["name"], "score": score,
                    "conditions": conditions, "frgn": flow["frgn"], "orgn": flow["orgn"],
                    "price": daily["price"], "change_rate": chg_rate,
                    "frgn_consec": frgn_consec, "orgn_consec": orgn_consec,
                })
            else:
                candidates.append({
                    "code": code, "name": flow["name"], "score": score,
                    "conditions": conditions, "frgn": flow["frgn"], "orgn": flow["orgn"],
                    "price": 0, "change_rate": 0,
                    "frgn_consec": frgn_consec, "orgn_consec": orgn_consec,
                })
        elif score >= 3:
            candidates.append({
                "code": code, "name": flow["name"], "score": score,
                "conditions": conditions, "frgn": flow["frgn"], "orgn": flow["orgn"],
                "price": 0, "change_rate": 0,
                "frgn_consec": frgn_consec, "orgn_consec": orgn_consec,
            })

    if not candidates:
        return ""

    # 3단계: 점수순 정렬 후 상위 7개
    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:7]

    lines = [f"{'━'*28}", "🎯 [내일 주목 종목 — 데이터 기반 선별]"]
    lines.append("  (외국인/기관 수급 + 연속매수 + 시세 분석)")

    for rank, c in enumerate(top, 1):
        # 신뢰도 등급
        if c["score"] >= 6:
            grade = "🔴S"
        elif c["score"] >= 4:
            grade = "🟠A"
        else:
            grade = "🟡B"

        lines.append(f"\n  {rank}. {grade} {c['name']} ({c['code']})")
        if c["price"]:
            lines.append(f"     종가 {c['price']:,}원 ({c['change_rate']:+.2f}%)")

        # 수급 요약
        flow_parts = []
        if c["frgn"] != 0:
            flow_parts.append(f"외국인 {c['frgn']:+,}주")
        if c["orgn"] != 0:
            flow_parts.append(f"기관 {c['orgn']:+,}주")
        if flow_parts:
            lines.append(f"     수급: {' / '.join(flow_parts)}")

        # 연속 매수 트렌드 바
        if c["frgn_consec"] >= 3:
            frgn_s = _get_flow_series("FRGN_FLOW", c["code"], days=7)
            if frgn_s:
                vals = [v for _, v in frgn_s]
                mx = max(abs(v) for v in vals) if vals else 1
                bar = "".join("▼" if v <= 0 else ["▁", "▂", "▃", "▅", "▇"][min(int(v / max(mx, 1) * 4), 4)] for v in vals)
                lines.append(f"     외국인 {c['frgn_consec']}일 연속: {bar}")

        # 선별 근거
        lines.append(f"     근거: {' | '.join(c['conditions'])}")

    lines.append(f"\n  ⚠️ 수급/시세 데이터 기반 분석이며 투자 권유가 아닙니다")
    return "\n".join(lines)


def build_closing_analysis() -> str:
    """장마감 종합 분석 — 다음날 투자 판단에 필요한 정제 데이터"""
    now_ts = datetime.now(KST).strftime("%m/%d %H:%M")
    sections = []

    # ── 헤더 ──
    sections.append(f"{'━'*28}\n🌙 [장마감 종합 분석] {now_ts}")

    # ── 1. 시장 지수 종가 ──
    try:
        indices = _fetch_market_indices()
        if indices:
            sections.append(f"\n📈 [시장 종가]\n{indices}")
    except Exception:
        pass

    # ── 2. 보유종목 당일 성적표 + 수급 ──
    try:
        holdings = _get_holdings_codes()
        if holdings:
            h_lines = [f"\n{'━'*28}", "📋 [보유종목 당일 분석]"]
            token = get_kis_access_token()
            signals = []  # (name, signal_type) — 투자 시그널 수집용

            for code, name, qty, avg_price, eval_amt in holdings:
                daily = _get_stock_daily_change(code)
                if not daily:
                    continue
                time.sleep(0.15)

                chg = daily["change"]
                chg_rate = daily["change_rate"]
                icon = "🟢" if chg >= 0 else "🔴"

                h_lines.append(f"\n{icon} {name} ({qty}주)")
                h_lines.append(f"  종가 {daily['price']:,}원 ({chg:+,}원, {chg_rate:+.2f}%)")

                # 당일 변동 폭 (고가-저가)
                if daily["high"] and daily["low"]:
                    swing = daily["high"] - daily["low"]
                    swing_pct = (swing / daily["low"] * 100) if daily["low"] else 0
                    h_lines.append(f"  변동폭 {swing:,}원 ({swing_pct:.1f}%) | 고 {daily['high']:,} / 저 {daily['low']:,}")

                # 52주 고저 대비 위치
                if daily["w52_high"] and daily["w52_low"] and daily["w52_high"] > daily["w52_low"]:
                    pos = (daily["price"] - daily["w52_low"]) / (daily["w52_high"] - daily["w52_low"]) * 100
                    pos_bar = "▓" * int(pos / 10) + "░" * (10 - int(pos / 10))
                    h_lines.append(f"  52주 위치 [{pos_bar}] {pos:.0f}%")

                    if pos >= 90:
                        signals.append((name, "52주 신고가 근접"))
                    elif pos <= 15:
                        signals.append((name, "52주 저가 구간"))

                # 외국인/기관 당일 수급
                flow = get_market_summary(token, code)
                if flow and "오류" not in flow and "없음" not in flow:
                    h_lines.append(f"  {flow}")

                    # 외국인 대량매수 시그널 파싱
                    if "외국인: 🟢" in flow:
                        signals.append((name, "외국인 순매수"))
                    if "기관: 🟢" in flow:
                        signals.append((name, "기관 순매수"))

                # Redis 수급 추세 (최근 5일)
                frgn_series = _get_flow_series("FRGN_FLOW", code, days=5)
                if frgn_series:
                    frgn_vals = [v for _, v in frgn_series]
                    consec_buy = 0
                    for v in reversed(frgn_vals):
                        if v > 0:
                            consec_buy += 1
                        else:
                            break
                    if consec_buy >= 3:
                        h_lines.append(f"  ⚡ 외국인 {consec_buy}일 연속 순매수")
                        signals.append((name, f"외국인 {consec_buy}일 연속 순매수"))

            sections.append("\n".join(h_lines))

    except Exception as e:
        sections.append(f"\n📋 보유종목 분석 오류: {e}")
        signals = []

    # ── 3. 기존 종합 리포트 (자산/실현손익/수익률) ──
    try:
        main_report = get_account_profit(False)
        # 보유종목 목록은 위에서 이미 표시했으므로, 자산 현황 부분만 추출
        asset_marker = "💼 [총 자산]"
        idx = main_report.find(asset_marker)
        if idx >= 0:
            # 자산현황 이후 부분만 사용
            asset_section = main_report[main_report.rfind("━" * 28, 0, idx):] if main_report.rfind("━" * 28, 0, idx) >= 0 else main_report[idx:]
            sections.append(f"\n{asset_section}")
        else:
            sections.append(f"\n{main_report}")
    except Exception as e:
        sections.append(f"\n💼 자산 현황 조회 오류: {e}")

    # ── 4. 당일 수급 TOP (외국인/기관 순매수 TOP 5) ──
    try:
        supply = build_daily_top_supply_demand(topn=5)
        if supply:
            sections.append(f"\n{supply}")
    except Exception as e:
        sections.append(f"\n📊 수급 TOP 조회 오류: {e}")

    # ── 5. 외국인/기관 연속 순매수 종목 ──
    try:
        frgn_consec = _get_consecutive_flow_top("FRGN_FLOW", "🌍 외국인", topn=5)
        orgn_consec = _get_consecutive_flow_top("ORGN_FLOW", "🏛️ 기관", topn=5)
        consec_parts = [p for p in [frgn_consec, orgn_consec] if p]
        if consec_parts:
            sections.append(f"\n{'━'*28}\n🔥 [스마트머니 연속 매수 추적]\n" + "\n\n".join(consec_parts))
    except Exception:
        pass

    # ── 6. ETF 거래량 TOP 3 ──
    try:
        etf_vol = _get_etf_volume_top3()
        if etf_vol:
            sections.append(f"\n{etf_vol}")
    except Exception:
        pass

    # ── 7. 환율 ──
    try:
        fx = _get_fx_summary()
        if fx:
            sections.append(f"\n{'━'*28}\n💱 [주요 환율]\n{fx}")
    except Exception:
        pass

    # ── 8. 내 보유종목 시그널 요약 ──
    try:
        if signals:
            sig_lines = [f"\n{'━'*28}", "⚡ [보유종목 시그널]"]
            seen = set()
            for name, sig_type in signals:
                key = f"{name}:{sig_type}"
                if key not in seen:
                    seen.add(key)
                    sig_lines.append(f"  • {name} — {sig_type}")
            sections.append("\n".join(sig_lines))
    except Exception:
        pass

    # ── 9. 내일 주목 종목 선별 (핵심) ──
    try:
        picks = _build_stock_picks()
        if picks:
            sections.append(f"\n{picks}")
    except Exception as e:
        sections.append(f"\n🎯 종목 선별 오류: {e}")

    return "\n".join(sections)


def run():
    print(f"[시작] KIS Discord Alert Bot (연도: {current_year()})")
    send_alert_message(f"✅ 알림 봇 시작 ({current_year()})")

    # 스케줄 등록
    schedule.every().day.at("08:30").do(lambda: is_trading_day() and send_alert_message(get_account_profit_with_yearly_report()))
    schedule.every().day.at("16:00").do(lambda: is_trading_day() and send_alert_message(build_closing_analysis()))
    schedule.every().day.at("08:10").do(job_daily_new_etf_check)     # 신규 ETF (1차)
    schedule.every().day.at("08:30").do(job_daily_new_etf_check)     # 신규 ETF (2차, 장전시간외)
    schedule.every().day.at("08:10").do(job_weekly_etf_briefing)
    schedule.every().day.at("08:10").do(job_monthly_etf_report)
    schedule.every().day.at("08:10").do(job_weekly_listing_etf)      # 금주 상장 ETF (주간 첫 거래일)
    schedule.every().day.at("08:10").do(job_today_listing_etf_reminder)  # 상장 당일 리마인드
    schedule.every().day.at("08:20").do(job_daily_foreign_trend)
    schedule.every().day.at("15:50").do(job_snapshot_foreign_flow)

    # 실시간 잔고 변동 모니터
    Thread(target=check_holdings_change_loop, daemon=True).start()

    # 최초 실행: 전체 브리핑 (시작 메시지와 함께)
    try:
        report = get_account_profit_with_yearly_report()
        if report:
            send_alert_message(report)
            print("[초기] 종합 리포트 전송 완료")
    except Exception as e:
        send_alert_message(f"❌ 종합 리포트 실패: {e}")
        print(f"[초기] 종합 리포트 실패: {e}")
    try:
        upcoming = get_upcoming_etf_report()
        if upcoming:
            send_alert_message(upcoming)
            print("[초기] 상장 예정 ETF 전송 완료")
    except Exception as e:
        print(f"[초기] 상장 예정 ETF 실패: {e}")

    # 메인 루프 (graceful shutdown 지원)
    while not shutdown_event.is_set():
        try:
            schedule.run_pending()
            shutdown_event.wait(1)
        except Exception as e:
            print(f"[스케줄 루프 오류] {e}")
            traceback.print_exc()
            shutdown_event.wait(10)

    send_alert_message("🛑 알림 봇 종료")
    print("[종료] 알림 봇 정상 종료됨")

if __name__ == "__main__":
    run()
