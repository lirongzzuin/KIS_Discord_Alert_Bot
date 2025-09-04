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
from typing import List, Dict, Tuple

# ================== 환경설정 ==================
load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 설정값
FOREIGN_TREND_TOPN = int(os.getenv("FOREIGN_TREND_TOPN", "15"))  # 외국인 추세 TOP N
KST = timezone("Asia/Seoul")
only_changes = True  # 실시간 잔고 변동만 알림 여부

# ================== Redis ==================
try:
    r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
except Exception as e:
    print(f"Redis 연결 실패: {e}")
    r = None

# ================== 공통 발송 ==================
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

# ================== 거래일/휴일 ==================
# 2025 대한민국 증시 휴장일(한국거래소 공지/캘린더 기반 보수 셋업)
HOLIDAYS_2025 = {
    "2025-01-01",  # 신정
    "2025-01-27", "2025-01-28", "2025-01-29", "2025-01-30",  # 설 연휴
    "2025-03-03",  # 삼일절 대체공휴일
    "2025-05-01",  # 근로자의 날(휴장)
    "2025-05-05",  # 어린이날
    "2025-05-06",  # 대체/불교기념일(연휴 충돌 가능)
    "2025-06-06",  # 현충일
    "2025-08-15",  # 광복절
    "2025-10-03",  # 개천절
    "2025-10-06", "2025-10-07", "2025-10-08",  # 추석 연휴
    "2025-10-09",  # 한글날
    "2025-12-31",  # 연말 휴장
}

def is_weekday(dt: datetime = None):
    now = dt or datetime.now(KST)
    return now.weekday() < 5

def is_holiday(dt: datetime = None):
    now = dt or datetime.now(KST)
    if now.year == 2025 and now.strftime("%Y-%m-%d") in HOLIDAYS_2025:
        return True
    return False

def is_trading_day(dt: datetime = None):
    now = dt or datetime.now(KST)
    return now.weekday() < 5 and not is_holiday(now)

def is_market_hour(dt: datetime = None):
    now = dt or datetime.now(KST)
    return 9 <= now.hour <= 15

# ================== KIS 공통 ==================
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

# ================== 기존 잔고/리포트 로직(유지) ==================
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
                txt = (resp.text or "").strip()
                if not txt:
                    last_err = "empty body"; attempt += 1; time.sleep(sleep_sec); continue
                try:
                    data = resp.json()
                except ValueError as ve:
                    last_err = f"invalid json: {ve} / body[:200]={txt[:200]}"; attempt += 1; time.sleep(sleep_sec); continue
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

def get_account_profit(only_changes=True):
    token = get_kis_access_token()
    _ = get_realized_holdings_data()  # 기존 로직 유지(호출)

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
                diff = qty - old_qty; arrow = "🟢 증가" if diff>0 else "🔴 감소"
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
        return "📌 [잔고 변동 내역]\n" + "\n".join(changes) if changes else ""

    try: cash = get_current_cash_balance(token)
    except Exception as e: print(f"[예수금 조회 실패] {e}"); cash = 0
    try: net_deposit = get_net_deposit_2025(token)
    except Exception as e: print(f"[순입금 조회 실패] {e}"); net_deposit = 0

    total_assets = total_eval + cash
    display_total_eval = total_assets - net_deposit
    display_total_profit = (total_assets - net_deposit) - total_invest
    display_total_rate = (display_total_profit/ total_invest *100) if total_invest else 0.0

    report = ""
    if changes: report += "📌 [잔고 변동 내역]\n" + "\n".join(changes) + "\n\n"
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
            if denom != 0: estimated_rate_2025 = (estimated_profit_2025/denom)*100
            else: estimated_rate_2025 = float('inf') if estimated_profit_2025>0 else float('-inf') if estimated_profit_2025<0 else 0.0
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
    """
    예탁원정보(상장정보일정) [국내주식-150], TR: HHKDB669107C0
    """
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

def _is_etf(name: str) -> bool:
    return "ETF" in (name or "").upper()

def get_newly_listed_etfs_for_today_ksd() -> str:
    """
    오늘 상장되는 ETF 요약(장 시작 전 발송용)
    - Redis로 동일일자/동일종목 중복 전송 방지
    """
    if not is_trading_day():
        return ""
    now = datetime.now(KST)
    ymd = now.strftime("%Y%m%d"); ymd_dash = now.strftime("%Y-%m-%d")
    rows = _inquire_ksd_list_info(F_DT=ymd, T_DT=ymd)
    if not rows: return ""
    key = f"KSD_ETF_ALERTED_BYDATE:{ymd}"
    alerted = set(r.smembers(key) or []) if r else set()
    TAGS = {"레버리지":"레버리지","인버스":"인버스","나스닥":"나스닥","S&P":"S&P","미국":"미국",
            "2차전지":"2차전지","반도체":"반도체","배당":"배당","원유":"원유","금":"금","중국":"중국","테크":"테크","코스닥":"코스닥"}
    msgs=[]
    for it in rows:
        try:
            if (it.get("list_dt") or "") != ymd_dash: continue
            name = it.get("isin_name",""); code = it.get("sht_cd","")
            if not _is_etf(name): continue
            unique = f"{code}:{ymd}"
            if unique in alerted: continue
            tags = [v for k,v in TAGS.items() if k in name]
            tag_str = (" | " + ", ".join(tags)) if tags else ""
            msg = (
                f"🆕 오늘 상장 ETF\n"
                f"종목명: {name} ({code}){tag_str}\n"
                f"상장일: {ymd_dash}\n"
                f"사유/종류: {it.get('issue_type','')} / {it.get('stk_kind','')}\n"
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
    """
    이번 주(월~금) 상장 예정 ETF 주간 요약
    - 주간 첫 거래일에 1회 발송 권장
    """
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
        name = it.get("isin_name","")
        if not _is_etf(name): continue
        dt = (it.get("list_dt") or "").strip()
        if not (F_dash <= dt <= T_dash): continue
        etfs.append({
            "date": dt, "code": it.get("sht_cd",""), "name": name,
            "issue_type": it.get("issue_type",""), "stk_kind": it.get("stk_kind",""),
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
    # 월요일부터 어제까지 중 '거래일'이 하나라도 있었으면 첫 거래일이 아님
    d = mon
    while d < now.date():
        dt = datetime(d.year, d.month, d.day, tzinfo=KST)
        if is_trading_day(dt):
            return False
        d += timedelta(days=1)
    return True

# ================== 국내주식-037 외국인/기관 매매가집계 ==================
def _call_foreign_institution_total(fid_input_iscd: str = "0000",
                                   fid_div_cls: str = "1",
                                   fid_rank_sort: str = "0",
                                   fid_etc_cls: str = "1") -> List[dict]:
    """
    국내기관_외국인 매매종목가집계(FHPTJ04400000)
    - fid_input_iscd: 0000 전체, 0001 코스피, 1001 코스닥
    - fid_div_cls: 0 수량정렬, 1 금액정렬(기본)
    - fid_rank_sort: 0 순매수상위, 1 순매도상위
    - fid_etc_cls: 0 전체, 1 외국인, 2 기관계, 3 기타
    """
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
        # 리스트/단건 통일
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
    """
    매 영업일 15:41경:
    - KOSPI(0001), KOSDAQ(1001), 전체(0000)에서 '외국인 순매수 상위' 금액정렬 조회
    - 결과 코드별 'frgn_ntby_qty'(순매수 수량)를 해당 일자 키로 저장
    - Redis 해시: FRGN_FLOW:{code} -> {YYYYMMDD: frgn_ntby_qty}
    """
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
        time.sleep(0.25)  # 과호출 방지

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
                r.expire(f"FRGN_FLOW:{code}", 120*24*3600)  # 120일 보관
        except Exception as e:
            print(f"[Redis 기록 오류] {code} / {e}")

# 시계열 조회/스코어링
def _get_foreign_series(code: str, days: int = 7) -> List[Tuple[str,int]]:
    if not r: return []
    all_kv = r.hgetall(f"FRGN_FLOW:{code}") or {}
    if not all_kv: return []
    items = sorted(((k,v) for k,v in all_kv.items()), key=lambda x: x[0])
    items = items[-days:]
    return [(d, safe_int(v)) for d, v in items]

def _is_sustained_growth(series: List[int]) -> bool:
    # 최근 5일 중 양(+)의 순매수일이 4일 이상 + 최근 3일 연속 증가
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
    """
    Redis에 쌓인 '전 종목' 외국인 순매수(일별) 기록을 기반으로
    최근 N일 상승추세 충족 종목을 점수화해 TOP N 출력
    """
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
    """08:05: 이번 주 첫 거래일이면 주간 상장 ETF 한번만 발송"""
    now = datetime.now(KST)
    if not _is_first_trading_day_of_week(now):
        return
    year_week = f"{now.isocalendar().year}-W{now.isocalendar().week}"
    key = f"WEEKLY_ETF_SENT:{year_week}"
    already = r.get(key) if r else None
    if already:
        return
    msg = get_upcoming_listed_etfs_this_week_ksd(now)
    if msg:
        send_alert_message(msg)
        if r:
            r.set(key, "1", ex=21*24*3600)  # 3주 보관

def job_daily_today_etf_plus_foreign_0820():
    """08:20: 오늘 상장 ETF + 외국인 수급 추세 종목 묶음 발송"""
    if not is_trading_day():
        return
    etf_msg = get_newly_listed_etfs_for_today_ksd()
    trend_msg = build_foreign_trend_topN(days=7, topn=FOREIGN_TREND_TOPN)
    if etf_msg and trend_msg:
        send_alert_message(etf_msg + "\n\n" + trend_msg)
    elif etf_msg:
        send_alert_message(etf_msg + "\n\n" + "📈 외국인 수급 추세 데이터 없음(누적 대기)")
    # ETF가 없어도 추세만 보고 싶다면 아래 주석을 해제
    # else:
    #     send_alert_message(trend_msg)

def job_snapshot_foreign_flow_1541():
    """15:41: 외국인 순매수(일) 스냅샷 적재(전 종목, 공식 API)"""
    try:
        snapshot_foreign_flow_all_codes()
    except Exception as e:
        send_alert_message(f"❌ 외국인 수급 스냅샷 실패: {e}")

# ================== 실시간 잔고 변동(기존) ==================
def check_holdings_change_loop():
    while True:
        try:
            if is_trading_day() and is_market_hour():
                report = get_account_profit(only_changes=True)
                if report:
                    send_alert_message(report)
        except Exception as e:
            send_alert_message(f"❌ 자동 잔고 체크 오류: {e}")
            traceback.print_exc()
        time.sleep(60)

# ================== 런 ==================
def run():
    send_alert_message("✅ 알림 봇 시작")

    # 1) 누적 리포트(기존 유지)
    try:
        profit, rate = get_yearly_realized_profit_2025()
        summary = "📅 [2025 누적 리포트 (실현 손익 기준)]\n" f"💵 실현 수익금: {profit:,}원\n" f"📈 누적 수익률: {rate:.2f}%"
        send_alert_message(summary)
    except Exception as e:
        send_alert_message(f"❌ 누적 리포트 조회 실패: {e}")

    # 2) 스케줄 구성
    # (기존 리포트)
    schedule.every().day.at("08:30").do(lambda: is_trading_day() and send_alert_message(get_account_profit(False)))
    schedule.every().day.at("16:00").do(lambda: is_trading_day() and send_alert_message(get_account_profit_with_yearly_report()))
    # (신규: ETF/수급)
    schedule.every().day.at("08:10").do(job_weekly_if_first_trading_day_0805)   # 주간 ETF(첫 거래일만)
    schedule.every().day.at("08:20").do(job_daily_today_etf_plus_foreign_0820)  # 당일 ETF + 외인 추세
    schedule.every().day.at("15:50").do(job_snapshot_foreign_flow_1541)         # 일별 스냅샷 적재

    # 3) 실시간 잔고 변동(기존)
    Thread(target=check_holdings_change_loop, daemon=True).start()

    # 4) ▶ 최초 실행 테스트(1회만): 주간 ETF + 오늘자 ETF/수급 묶음 즉시 전송
    try:
        weekly_once = get_upcoming_listed_etfs_this_week_ksd()
        if weekly_once: send_alert_message("🧪[테스트 1회] " + weekly_once)
        today_etf = get_newly_listed_etfs_for_today_ksd()
        trend = build_foreign_trend_topN(days=7, topn=FOREIGN_TREND_TOPN)
        combo = (today_etf + "\n\n" + trend) if today_etf else trend
        if combo:
            send_alert_message("🧪[테스트 1회] " + combo)
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

# 필요: get_account_profit_with_yearly_report (기존 유지용)
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

if __name__ == "__main__":
    run()
