# kis_discord_alert.py
import os
import json
import time
import schedule
import requests
import redis
import traceback
from datetime import datetime, timedelta, date
from pytz import timezone
from dotenv import load_dotenv
from threading import Thread
from typing import List, Dict, Tuple, Optional

# ================== 환경설정 ==================
load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 환율 fallback
FALLBACK_USDKRW = float(os.getenv("FALLBACK_USDKRW", "1350"))
FALLBACK_JPYKRW = float(os.getenv("FALLBACK_JPYKRW", "9.5"))    # 1JPY=x KRW
FALLBACK_HKDKRW = float(os.getenv("FALLBACK_HKDKRW", "175"))
FALLBACK_CNYKRW = float(os.getenv("FALLBACK_CNYKRW", "190"))
FX_CACHE_TTL_SEC = int(os.getenv("FX_CACHE_TTL_SEC", "900"))

# 설정값
FOREIGN_TREND_TOPN = int(os.getenv("FOREIGN_TREND_TOPN", "15"))
KST = timezone("Asia/Seoul")

# ================== Redis ==================
try:
    r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
except Exception as e:
    print(f"Redis 연결 실패: {e}")
    r = None

# ================== 발송 ==================
def send_alert_message(content: str):
    if not content:
        return
    send_discord_message(content)
    send_telegram_message(content)

def send_discord_message(content: str):
    try:
        if DISCORD_WEBHOOK_URL:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=10)
    except Exception as e:
        print(f"[디스코드 전송 오류] {e}")
        traceback.print_exc()

def send_telegram_message(content: str):
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": content}
            res = requests.post(url, data=payload, timeout=10)
            res.raise_for_status()
    except Exception as e:
        print(f"[텔레그램 전송 오류] {e}")
        traceback.print_exc()

# ================== 휴장/장운영 ==================
HOLIDAYS_2025 = {
    "2025-01-01","2025-01-27","2025-01-28","2025-01-29","2025-01-30",
    "2025-03-03","2025-05-01","2025-05-05","2025-05-06","2025-06-06",
    "2025-08-15","2025-10-03","2025-10-06","2025-10-07","2025-10-08",
    "2025-10-09","2025-12-31",
}

def is_holiday(dt: datetime = None):
    now = dt or datetime.now(KST)
    return (now.year == 2025) and (now.strftime("%Y-%m-%d") in HOLIDAYS_2025)

def is_trading_day(dt: datetime = None):
    now = dt or datetime.now(KST)
    return now.weekday() < 5 and not is_holiday(now)

def is_market_hour(dt: datetime = None):
    now = dt or datetime.now(KST)
    return 9 <= now.hour <= 15

# ================== 공통 유틸/토큰 ==================
def get_kis_access_token():
    now_ts = time.time()
    if r:
        token = r.get("KIS_ACCESS_TOKEN")
        expire_ts = r.get("KIS_TOKEN_EXPIRE_TIME")
        if token and expire_ts and float(expire_ts) > now_ts:
            return token
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    headers = {"Content-Type": "application/json"}
    data = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    res = requests.post(url, headers=headers, data=json.dumps(data), timeout=12).json()
    if "access_token" not in res:
        raise Exception(f"[토큰 오류] {res}")
    token = res["access_token"]
    expires_in = int(res.get("expires_in", 86400))
    if r:
        r.set("KIS_ACCESS_TOKEN", token)
        r.set("KIS_TOKEN_EXPIRE_TIME", now_ts + expires_in - 60)
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
    # 장 마감 이후 집계
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
        # 필수/환경의존 파라미터를 모두 포함
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
    """
    해외 현재잔고 조회
    - 국가/시장 코드 기반(10/20/30/40) x 원화/외화
    - 미국 거래소 코드(NASD/NYSE/AMEX) 기반 x 원화/외화 (누락 보완)
    - 전부 실패/0이면 전체 폴백 1회
    """
    all_rows: List[dict] = []
    errors: List[str] = []

    # 1) 국가/시장 기반
    for tr_mket_cd, natn_cd, _desc in MARKET_COUNTRY_LIST:
        for wcrc in ("01","02"):   # 01:원화, 02:외화
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
    """
    배포본마다 다른 응답 필드를 폭넓게 대응
    -> 표준화 출력(dict)
    """
    if not row:
        return None
    name = row.get("ovrs_item_name") or row.get("prdt_name") or row.get("hts_kor_isnm") or ""
    code = row.get("ovrs_pdno") or row.get("pdno") or row.get("symb") or ""
    market = (row.get("ovrs_excg_cd") or row.get("excg_cd") or "").upper()

    # 수량
    qty = safe_float(
        row.get("ovrs_cblc_qty")
        or row.get("hldg_qty")
        or row.get("frcr_cblc_qty")
        or row.get("qty")
        or 0
    )
    if qty <= 0:
        return None

    # 평균/현재가(현지통화)
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
    dedup = {}  # (code, market) -> best row (평가금액 큰 것 선택)

    for row in rows:
        p = _parse_overseas_row(row)
        if not p:
            continue
        key = (p["code"], p["market"])
        if key not in dedup or p["eval_krw"] > dedup[key]["eval_krw"]:
            dedup[key] = p

    parsed = list(dedup.values())
    parsed.sort(key=lambda x: x["eval_krw"], reverse=True)

    # 스냅샷 불러오기/저장
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

    # 🔧 핵심 수정: 변동이 없으면 빈 문자열 반환(알림 전송 X)
    if only_changes:
        return ("🌍 [해외 잔고 변동]\n" + "\n".join(changes)) if changes else ""

    if not parsed:
        return ""  # 상세 보고 요청이어도 보유 없으면 굳이 전송하지 않음

    # 상세 리포트
    lines = []
    if changes:
        lines.append("🌍 [해외 잔고 변동]\n" + "\n".join(changes) + "\n")
    lines.append("🌍 [해외 보유 종목 수익률]")
    for it in parsed:
        icon = "🟢" if it["profit_krw"] >= 0 else "🔴"
        lines.append(
            f"{icon} {it['name']} ({it['code']}:{it['market']}) [{it['ccy']}]\n"
            f"┗ 수량: {it['qty']:.4f} | 평균단가: {it['avg_ccy']:.4f} {it['ccy']} ({_fmt_price_won(it['avg_krw'])})\n"
            f"┗ 현재가: {it['cur_ccy']:.4f} {it['ccy']} ({_fmt_price_won(it['cur_krw'])})\n"
            f"┗ 평가금액: {_fmt_amount_won(it['eval_krw'])} | 수익금: {_fmt_amount_won(it['profit_krw'])} | 수익률: {_fmt_rate(it['rate'])}"
        )
    total_rate = (total_profit/total_invest*100) if total_invest else 0.0
    lines.append(
        f"\n🌍 합계 평가금액: {_fmt_amount_won(total_eval)}"
        f"\n🌍 합계 수익금: {_fmt_amount_won(total_profit)}"
        f"\n🌍 합계 수익률: {_fmt_rate(total_rate)}"
    )
    return "\n".join(lines)

# ================== 순입금/초기가치 ==================
def get_net_deposit_2025(token, retries=2, timeout=10, sleep_sec=0.4):
    url = "https://openapi.koreainvestment.com:9443/uapi/overseas-stock/v1/trading/inquire-deposit-withdraw"
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8991R", "Content-Type": "application/json"
    }
    acct_raw = KIS_ACCOUNT_NO.replace("-", "")
    cano, acct_cd = acct_raw[:8], acct_raw[8:]
    params = {
        "CANO": cano, "ACNT_PRDT_CD": acct_cd, "INQR_STRT_DT": "20250101",
        "INQR_END_DT": datetime.now(KST).strftime("%Y%m%d"),
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "", "INQR_DVSN": "00"
    }
    deposits = 0; withdrawals = 0
    while True:
        attempt = 0; last_err = None
        while attempt <= retries:
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=timeout)
                if resp.status_code == 404:
                    return 0
                if resp.status_code != 200:
                    last_err = f"HTTP {resp.status_code} / {resp.text[:200]}"; attempt += 1; time.sleep(sleep_sec); continue
                data = resp.json()
                if data.get("rt_cd") != "0":
                    msg = data.get("msg1") or data.get("msg_cd") or str(data)[:200]
                    raise Exception(f"[입출금내역 조회 실패] {msg}")
                for row in data.get("output", []):
                    typ = (row.get("dpst_withdraw_gb") or "").strip()
                    amt = safe_int(row.get("txamt", "0"))
                    if "입금" in typ: deposits += amt
                    elif "출금" in typ: withdrawals += amt
                fk = (data.get("CTX_AREA_FK100") or "").strip()
                nk = (data.get("CTX_AREA_NK100") or "").strip()
                if fk or nk:
                    params["CTX_AREA_FK100"] = fk; params["CTX_AREA_NK100"] = nk; time.sleep(sleep_sec); break
                else:
                    return deposits - withdrawals
            except requests.exceptions.RequestException as e:
                last_err = f"network: {e}"; attempt += 1; time.sleep(sleep_sec); continue
        raise Exception(f"[순입금액 계산 오류] {last_err}")

def get_initial_assets_2025():
    try:
        val = r.get("INITIAL_ASSETS_2025") if r else None
        return int(val) if val else None
    except Exception as e:
        print(f"[초기 자산 조회 오류] {e}")
        return None

# ================== 국내 잔고/리포트 ==================
def get_account_profit(only_changes=True):
    token = get_kis_access_token()
    _ = get_realized_holdings_data()  # 기존 로직 유지

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
    res = requests.get(url, headers=headers, params=params, timeout=10).json()
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
        return ("📌 [국내 잔고 변동 내역]\n" + "\n".join(changes)) if changes else ""

    try: cash = get_current_cash_balance(token)
    except Exception as e: print(f"[예수금 조회 실패] {e}"); cash = 0
    try: net_deposit = get_net_deposit_2025(token)
    except Exception as e: print(f"[순입금 조회 실패] {e}"); net_deposit = 0

    total_assets = total_eval + cash
    display_total_eval = total_assets - net_deposit
    display_total_profit = (total_assets - net_deposit) - total_invest
    display_total_rate = (display_total_profit/ total_invest *100) if total_invest else 0.0

    report = ""
    if changes: report += "📌 [국내 잔고 변동 내역]\n" + "\n".join(changes) + "\n\n"
    report += "📊 [보유 종목 수익률 + 수급 요약 보고]"
    for it in parsed_items:
        status_icon = "🟢" if it['profit'] >= 0 else "🔴"
        report += (
            f"\n{status_icon} {it['name']}\n"
            f"┗ 수량: {it['qty']}주 | 평균단가: {int(it['avg']):,}원 | 현재가: {int(it['cur']):,}원\n"
            f"┗ 평가금액: {it['eval']:,}원 | 수익금: {it['profit']:,}원 | 수익률: {it['rate']:.2f}%"
            + (f"\n┗ {it['flow']}" if it["flow"] else "")
        )
    report += (
        f"\n\n📈 총 평가금액: {int(display_total_eval):,}원"
        f"\n💰 총 수익금: {int(display_total_profit):,}원"
        f"\n📉 총 수익률: {display_total_rate:.2f}%"
    )

    try:
        initial_assets = get_initial_assets_2025()
        current_total_assets = total_assets
        if initial_assets is None:
            report += (
                f"\n\n⚠️ 2025년 추정 수익률 계산을 위해 'INITIAL_ASSETS_2025' 값을 설정해야 합니다."
                f"\n   (예: Redis에 'SET INITIAL_ASSETS_2025 {current_total_assets:,}')"
            )
        else:
            estimated_profit_2025 = current_total_assets - initial_assets - net_deposit
            denom = (initial_assets + net_deposit)
            estimated_rate_2025 = (estimated_profit_2025/denom)*100 if denom else 0.0
            report += (
                f"\n\n📅 2025 추정 수익: {int(estimated_profit_2025):,}원"
                f"\n📅 2025 추정 수익률: {estimated_rate_2025:.2f}%"
            )
    except Exception as e:
        report += f"\n📅 2025 추정 수익률 계산 오류: {e}"
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
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        res.raise_for_status(); data = res.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"[실현손익 API 네트워크 오류] {e}")
    except ValueError:
        raise Exception("[실현손익 API 응답 오류] JSON 포맷이 아님 또는 응답이 없음")
    if data.get("rt_cd") != "0":
        raise Exception(f"[실현손익 API 실패] {data.get('msg1', data)}")
    output1 = data.get("output1", []); result = {}
    for item in output1:
        name = item.get("prdt_name",""); realized_profit = safe_int(item.get("evlu_pfls_amt"))
        result[name] = realized_profit
    return result

def get_yearly_realized_profit_2025():
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-period-trade-profit"
    headers = {
        "authorization": f"Bearer {token}","appkey": KIS_APP_KEY,"appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8715R","custtype": "P","Content-Type": "application/json"
    }
    acct_raw = KIS_ACCOUNT_NO.replace("-", ""); cano, acct_cd = acct_raw[:8], acct_raw[8:]
    start_dt = "20250101"; end_dt = datetime.now(KST).strftime("%Y%m%d")
    params = {
        "CANO": cano,"ACNT_PRDT_CD": acct_cd,"SORT_DVSN": "01","PDNO": "",
        "INQR_STRT_DT": start_dt,"INQR_END_DT": end_dt,"CBLC_DVSN": "00",
        "CTX_AREA_FK100": "","CTX_AREA_NK100": ""
    }
    res = requests.get(url, headers=headers, params=params, timeout=10).json()
    if res.get("rt_cd") != "0":
        raise Exception(f"[실현손익조회 실패] {res.get('msg1', res)}")
    output2 = res.get("output2", {})
    realized_profit = safe_int(output2.get("tot_rlzt_pfls","0"))
    realized_rate = safe_float(output2.get("tot_pftrt","0"))
    return realized_profit, realized_rate

# ================== 예탁원 상장정보(ETF) ==================
def _inquire_ksd_list_info(F_DT: str, T_DT: str, sht_cd: str = "") -> List[dict]:
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/ksdinfo/list-info"
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET,
        "tr_id": "HHKDB669107C0", "custtype": "P", "Content-Type": "application/json"
    }
    params = {"SHT_CD": sht_cd or "", "T_DT": T_DT, "F_DT": F_DT, "CTS": ""}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=12)
        res.raise_for_status(); data = res.json()
        if not isinstance(data, dict) or data.get("rt_cd") != "0":
            print(f"[상장정보일정 응답 실패] {data.get('msg1','원인 미상') if isinstance(data, dict) else '비정상 응답'}")
            return []
        return data.get("output1", []) or []
    except Exception as e:
        print(f"[상장정보일정 API 오류] {e}")
        return []

def _normalize_ymd(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    if "-" in s: return s
    if len(s) == 8: return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s

def _is_etf(name: str, stk_kind: str = "") -> bool:
    nm = (name or "").upper()
    kind = (stk_kind or "").upper()
    return ("ETF" in nm) or ("ETF" in kind)

def get_newly_listed_etfs_for_today_ksd() -> str:
    if not is_trading_day():
        return ""
    now = datetime.now(KST)
    ymd = now.strftime("%Y%m%d")
    ymd_dash = now.strftime("%Y-%m-%d")
    rows = _inquire_ksd_list_info(F_DT=ymd, T_DT=ymd)
    if not rows:
        return ""
    key = f"KSD_ETF_ALERTED_BYDATE:{ymd}"
    alerted = set(r.smembers(key) or []) if r else set()
    TAGS = {"레버리지":"레버리지","인버스":"인버스","나스닥":"나스닥","S&P":"S&P","미국":"미국",
            "2차전지":"2차전지","반도체":"반도체","배당":"배당","원유":"원유","금":"금","중국":"중국","테크":"테크","코스닥":"코스닥"}
    msgs=[]
    for it in rows:
        try:
            list_dt_norm = _normalize_ymd(it.get("list_dt",""))
            if list_dt_norm != ymd_dash: 
                continue
            name = it.get("isin_name","") or it.get("prdt_name","")
            code = it.get("sht_cd","") or it.get("pdno","")
            stk_kind = it.get("stk_kind","")
            if not _is_etf(name, stk_kind): 
                continue
            unique = f"{code}:{ymd}"
            if unique in alerted: 
                continue
            tags = [v for k,v in TAGS.items() if k in name]
            tag_str = (" | " + ", ".join(tags)) if tags else ""
            msg = (
                f"🆕 오늘 상장 ETF\n"
                f"종목명: {name} ({code}){tag_str}\n"
                f"상장일: {ymd_dash}\n"
                f"사유/종류: {it.get('issue_type','')} / {stk_kind}\n"
                f"상장주식수/총발행/발행가: {it.get('issue_stk_qty','')} / {it.get('tot_issue_stk_qty','')} / {it.get('issue_price','')}"
            )
            msgs.append(msg)
            if r:
                r.sadd(key, unique); r.expire(key, 30*24*3600)
        except Exception as e:
            print(f"[ETF 파싱 오류] {e}")
            continue
    return "\n\n".join(msgs) if msgs else ""

def _monday_of_week(d: date) -> date:
    return d - timedelta(days=d.weekday())

def _friday_of_week(d: date) -> date:
    return _monday_of_week(d) + timedelta(days=4)

def get_upcoming_listed_etfs_this_week_ksd(now_dt: datetime = None) -> str:
    now = now_dt or datetime.now(KST)
    mon = _monday_of_week(now.date())
    fri = _friday_of_week(now.date())
    F_DT = mon.strftime("%Y%m%d"); T_DT = fri.strftime("%Y%m%d")
    F_dash = mon.strftime("%Y-%m-%d"); T_dash = fri.strftime("%Y-%m-%d")
    rows = _inquire_ksd_list_info(F_DT=F_DT, T_DT=T_DT)
    if not rows:
        return f"🔎 [{F_dash} ~ {T_dash}] 이번 주 상장 예정 ETF 없음"
    etfs = []
    for it in rows:
        name = it.get("isin_name","") or it.get("prdt_name","")
        stk_kind = it.get("stk_kind","")
        if not _is_etf(name, stk_kind): 
            continue
        dt = _normalize_ymd(it.get("list_dt",""))
        if not (F_dash <= dt <= T_dash): 
            continue
        etfs.append({
            "date": dt, "code": it.get("sht_cd","") or it.get("pdno",""), "name": name,
            "issue_type": it.get("issue_type",""), "stk_kind": stk_kind,
            "issue_qty": it.get("issue_stk_qty",""), "tot_qty": it.get("tot_issue_stk_qty",""),
            "issue_price": it.get("issue_price","")
        })
    if not etfs:
        return f"🔎 [{F_dash} ~ {T_dash}] 이번 주 상장 예정 ETF 없음"
    etfs.sort(key=lambda x: x["date"])
    parts = [f"📅 이번 주 상장 예정 ETF [{F_dash} ~ {T_dash}]"]
    cur = None
    for e in etfs:
        if e["date"] != cur:
            cur = e["date"]; parts.append(f"\n{cur}")
        parts.append(
            f"- {e['name']} ({e['code']})"
            f"\n  · 사유/종류: {e['issue_type']} / {e['stk_kind']}"
            f"\n  · 상장주식수/총발행/발행가: {e['issue_qty']} / {e['tot_qty']} / {e['issue_price']}"
        )
    return "\n".join(parts)

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
    per_code = {}
    for row in universe_rows:
        code = (row.get("mksc_shrn_iscd") or "").strip()
        if not code:
            continue
        qty = safe_int(row.get("frgn_ntby_qty"))
        if code not in per_code or qty > per_code[code]:
            per_code[code] = qty
    for code, qty in per_code.items():
        try:
            if r:
                r.hset(f"FRGN_FLOW:{code}", today, qty)
                r.expire(f"FRGN_FLOW:{code}", 120*24*3600)
        except Exception as e:
            print(f"[Redis 기록 오류] {code} / {e}")

def _get_foreign_series(code: str, days: int = 7) -> List[Tuple[str,int]]:
    if not r: return []
    all_kv = r.hgetall(f"FRGN_FLOW:{code}") or {}
    if not all_kv: return []
    items = sorted(((k,v) for k,v in all_kv.items()), key=lambda x: x[0])
    items = items[-days:]
    return [(d, safe_int(v)) for d, v in items]

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

def build_foreign_trend_topN(days: int = 7, topn: int = FOREIGN_TREND_TOPN) -> str:
    if not r:
        return "📈 외국인 수급 추세: 저장소(Redis) 미설정"
    codes = []
    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match="FRGN_FLOW:*", count=500)
            for k in keys:
                if k.startswith("FRGN_FLOW:"):
                    codes.append(k.split("FRGN_FLOW:", 1)[1])
            if cursor == 0:
                break
    except Exception as e:
        print(f"[Redis scan 오류] {e}")
        return "📈 외국인 수급 추세: 데이터 없음(스캔 실패)"
    scored = []
    for code in codes:
        series_kv = _get_foreign_series(code, days=days)
        if not series_kv:
            continue
        values = [v for _,v in series_kv]
        if not _is_sustained_growth(values):
            continue
        score = sum(max(0,v) for v in values) + (2*max(0, values[-1]))
        name = _lookup_name(code)
        scored.append((score, code, name, values))
    if not scored:
        return "📈 외국인 수급 추세: 조건 충족 종목 없음(데이터 누적 중)"
    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:max(1, topn)]
    lines = ["📈 최근 7일 외국인 수급 '상승 추세' 종목 TOP"]
    for rank,(score,code,name,vals) in enumerate(top, start=1):
        lastN = ", ".join(f"{v:+,}" for v in vals)
        lines.append(f"{rank}. {name} ({code}) | 점수: {score:,}\n   일별순매수: [{lastN}]")
    return "\n".join(lines)

# ================== 알림 작업 ==================
def job_weekly_if_first_trading_day_0805():
    now = datetime.now(KST)
    if not _is_first_trading_day_of_week(now):
        return
    iso = now.isocalendar()
    year_week = f"{iso.year}-W{iso.week}"
    key = f"WEEKLY_ETF_SENT:{year_week}"
    already = r.get(key) if r else None
    if already:
        return
    msg = get_upcoming_listed_etfs_this_week_ksd(now)
    if msg:
        send_alert_message(msg)
        if r:
            r.set(key, "1", ex=21*24*3600)

def job_daily_today_etf_plus_foreign_0820():
    if not is_trading_day():
        return
    etf_msg = get_newly_listed_etfs_for_today_ksd()
    trend_msg = build_foreign_trend_topN(days=7, topn=FOREIGN_TREND_TOPN)
    if etf_msg and trend_msg:
        send_alert_message(etf_msg + "\n\n" + trend_msg)
    elif etf_msg:
        send_alert_message(etf_msg + "\n\n" + "📈 외국인 수급 추세 데이터 없음(누적 대기)")

def job_snapshot_foreign_flow_1541():
    try:
        snapshot_foreign_flow_all_codes()
    except Exception as e:
        send_alert_message(f"❌ 외국인 수급 스냅샷 실패: {e}")

# ================== 실시간 잔고 변동 루프 ==================
def check_holdings_change_loop():
    while True:
        try:
            # 국내: 장중에만 체크
            if is_trading_day() and is_market_hour():
                rep_kr = get_account_profit(only_changes=True)
                if rep_kr:
                    send_alert_message(rep_kr)
            # 해외: 24h 체크(빈 문자열 반환이면 전송X)
            rep_ov = get_overseas_account_profit(only_changes=True)
            if rep_ov:
                send_alert_message(rep_ov)
        except Exception as e:
            send_alert_message(f"❌ 자동 잔고 체크 오류: {e}")
            traceback.print_exc()
        time.sleep(60)

# ================== 런 ==================
def get_account_profit_with_yearly_report():
    main_report = get_account_profit(False)
    try:
        profit, rate = get_yearly_realized_profit_2025()
        yearly = (
            "\n\n📅 [2025 누적 리포트 (실현 손익 기준)]\n"
            f"💵 실현 수익금: {profit:,}원\n"
            f"📈 누적 수익률: {rate:.2f}%"
        )
    except Exception as e:
        yearly = f"\n📅 [2025 누적 리포트 (실현 손익 기준)]\n❌ 누적 수익 조회 실패: {e}"
    return main_report + yearly

def run():
    send_alert_message("✅ 알림 봇 시작")

    # 초기 1회: 국내 누적 리포트
    try:
        profit, rate = get_yearly_realized_profit_2025()
        summary = "📅 [2025 누적 리포트 (실현 손익 기준)]\n" f"💵 실현 수익금: {profit:,}원\n" f"📈 누적 수익률: {rate:.2f}%"
        send_alert_message(summary)
    except Exception as e:
        send_alert_message(f"❌ 누적 리포트 조회 실패: {e}")

    # 스케줄
    schedule.every().day.at("08:30").do(lambda: is_trading_day() and send_alert_message(get_account_profit(False)))
    schedule.every().day.at("16:00").do(lambda: is_trading_day() and send_alert_message(get_account_profit_with_yearly_report()))
    schedule.every().day.at("08:10").do(job_weekly_if_first_trading_day_0805)
    schedule.every().day.at("08:20").do(job_daily_today_etf_plus_foreign_0820)
    schedule.every().day.at("15:50").do(job_snapshot_foreign_flow_1541)

    # 실시간 잔고 변동 모니터
    Thread(target=check_holdings_change_loop, daemon=True).start()

    # 최초 실행 테스트(스팸 방지: 해외 보유 없으면 보내지 않음)
    try:
        weekly_once = get_upcoming_listed_etfs_this_week_ksd()
        if weekly_once: send_alert_message("🧪[테스트] " + weekly_once)
        today_etf = get_newly_listed_etfs_for_today_ksd()
        trend = build_foreign_trend_topN(days=7, topn=FOREIGN_TREND_TOPN)
        ovrs_snapshot = get_overseas_account_profit(only_changes=False)
        combo = []
        if today_etf: combo.append(today_etf)
        if trend: combo.append(trend)
        if ovrs_snapshot: combo.append(ovrs_snapshot)  # 보유 없으면 빈문자열이라 추가 X
        if combo:
            send_alert_message("🧪[테스트] " + "\n\n".join(combo))
    except Exception as e:
        send_alert_message(f"❌ 테스트 전송 실패: {e}")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        send_alert_message("🛑 알림 봇 종료(수동)")
    except Exception as e:
        send_alert_message(f"❌ 알림 루프 오류: {e}")
        traceback.print_exc()
        time.sleep(10)

if __name__ == "__main__":
    run()
