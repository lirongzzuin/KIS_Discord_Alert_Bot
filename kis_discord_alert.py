import os
import json
import time
import schedule
import requests
import redis
import traceback
from datetime import datetime
from pytz import timezone
from dotenv import load_dotenv
from threading import Thread

load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

only_changes = True  # 실시간 감지 시 잔고 변동 사항만 보낼지 여부

try:
    r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
except Exception as e:
    print(f"Redis 연결 실패: {e}")
    r = None

def send_alert_message(content):
    send_discord_message(content)
    send_telegram_message(content)


def send_discord_message(content):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
    except Exception as e:
        print(f"[디스코드 전송 오류] {e}")
        traceback.print_exc()

def send_telegram_message(content):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": content
        }
        res = requests.post(url, data=payload)
        res.raise_for_status()
    except Exception as e:
        print(f"[텔레그램 전송 오류] {e}")
        traceback.print_exc()

def is_weekday():
    return datetime.now(timezone('Asia/Seoul')).weekday() < 5

def is_market_hour():
    now = datetime.now(timezone('Asia/Seoul'))
    return 9 <= now.hour <= 15

def get_kis_access_token():
    now = time.time()
    if r:
        token = r.get("KIS_ACCESS_TOKEN")
        expire_ts = r.get("KIS_TOKEN_EXPIRE_TIME")
        if token and expire_ts and float(expire_ts) > now:
            return token

    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    headers = {"Content-Type": "application/json"}
    data = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET
    }
    res = requests.post(url, headers=headers, data=json.dumps(data)).json()
    if "access_token" not in res:
        raise Exception(f"[토큰 오류] {res}")

    token = res["access_token"]
    expires_in = int(res.get("expires_in", 86400))
    if r:
        r.set("KIS_ACCESS_TOKEN", token)
        r.set("KIS_TOKEN_EXPIRE_TIME", now + expires_in - 60)
    return token

def parse_int_field(value):
    value = (value or "").replace(",", "").strip()
    try:
        return int(value)
    except ValueError:
        return 0

def get_market_summary(token, stock_code):
    now = datetime.now(timezone('Asia/Seoul'))
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
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code
    }
    try:
        res = requests.get(url, headers=headers, params=params).json()
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

def safe_int(val):
    try:
        return int(str(val).replace(",", "").strip())
    except:
        return 0

def safe_float(val):
    try:
        return float(str(val).replace(",", "").strip())
    except:
        return 0.0

def get_current_cash_balance(token):
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-psbl-order"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8908R",
        "Content-Type": "application/json"
    }

    acct_raw = KIS_ACCOUNT_NO.replace("-", "")
    cano, acct_cd = acct_raw[:8], acct_raw[8:]

    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acct_cd,
        "PDNO": "",
        "ORD_UNPR": "0",
        "ORD_DVSN": "00",
        "CMA_EVLU_AMT_ICLD_YN": "N",
        "OVRS_ICLD_YN": "N" # 추가: 해외주식포함여부 (N으로 설정)
    }

    res = requests.get(url, headers=headers, params=params).json()
    if res.get("rt_cd") != "0":
        # 오류 메시지를 더 자세히 출력하여 디버깅에 도움을 줍니다.
        raise Exception(f"[현금 조회 실패] {res.get('msg1', res)}")

    output = res.get("output", {})
    return safe_int(output.get("dnca_tot_amt", "0"))

def get_initial_assets_2025():
    try:
        val = r.get("INITIAL_ASSETS_2025") if r else None
        return int(val) if val else None # 값이 없거나 비어있으면 None 반환
    except Exception as e:
        print(f"[초기 자산 조회 오류] {e}")
        return None # 오류 발생 시에도 None 반환

def get_net_deposit_2025(token):
    url = "https://openapi.koreainvestment.com:9443/uapi/overseas-stock/v1/trading/inquire-deposit-withdraw"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8991R",
        "Content-Type": "application/json"
    }

    acct_raw = KIS_ACCOUNT_NO.replace("-", "")
    cano, acct_cd = acct_raw[:8], acct_raw[8:]

    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acct_cd,
        "INQR_STRT_DT": "20250101",
        "INQR_END_DT": datetime.now().strftime("%Y%m%d"),
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
        "INQR_DVSN": "00"
    }

    try:
        res = requests.get(url, headers=headers, params=params).json()
        if res.get("rt_cd") != "0":
            raise Exception(f"[입출금내역 조회 실패] {res.get('msg1', res)}")
        deposits = withdrawals = 0
        for row in res.get("output", []):
            typ = row.get("dpst_withdraw_gb", "")
            amt = safe_int(row.get("txamt", "0"))
            if "입금" in typ:
                deposits += amt
            elif "출금" in typ:
                withdrawals += amt
        return deposits - withdrawals
    except Exception as e:
        print(f"[순입금액 계산 오류] {e}")
        return 0

def get_account_profit(only_changes=True):
    token = get_kis_access_token()
    realized_holdings = get_realized_holdings_data()

    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8434R",
        "Content-Type": "application/json"
    }
    params = {
        "CANO": KIS_ACCOUNT_NO[:8],
        "ACNT_PRDT_CD": KIS_ACCOUNT_NO[9:],
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "AFHR_FLPR_YN": "N",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "OFL_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "P",
        "CTX_AREA_NK100": ""
    }
    res = requests.get(url, headers=headers, params=params).json()

    if res.get("rt_cd") != "0":
        raise Exception(f"[잔고 API 실패] {res.get('msg1', res)}")

    output = res.get("output1", [])
    if not output:
        return "📭 보유 중인 종목이 없습니다."

    last_json = r.get("LAST_HOLDINGS") if r else None
    last = json.loads(last_json) if last_json else {}

    new_holdings = {}
    parsed_items = []
    total_profit = total_eval = total_invest = 0
    changes = []

    for item in output:
        try:
            qty = safe_int(item["hldg_qty"])
            if qty == 0:
                continue

            name = item.get("prdt_name", "알수없음")
            code = item.get("pdno", "")
            avg_price = safe_float(item.get("pchs_avg_pric"))
            cur_price = safe_float(item.get("prpr"))

            eval_amt = safe_int(item.get("evlu_amt"))
            profit = safe_int(item.get("evlu_erng_amt"))
            rate = safe_float(item.get("evlu_pfls_rt"))

            if eval_amt == 0:
                eval_amt = int(qty * cur_price)
            if profit == 0:
                invest_amt = int(qty * avg_price)
                profit = eval_amt - invest_amt
            if rate == 0 and avg_price > 0:
                rate = (profit / (qty * avg_price)) * 100

            investor_flow = get_market_summary(token, code)

            new_holdings[name] = qty
            parsed_items.append({
                "name": name, "qty": qty, "avg": avg_price, "cur": cur_price,
                "eval": eval_amt, "profit": profit, "rate": rate, "flow": investor_flow
            })

            total_profit += profit
            total_eval += eval_amt
            total_invest += qty * avg_price

            old_qty = last.get(name, 0)
            if qty != old_qty:
                diff = qty - old_qty
                arrow = "🟢 증가" if diff > 0 else "🔴 감소"
                # 매도 추정 수익 계산 로직 수정: 매도 시점에 실제 실현 손익을 반영하도록
                # 이 부분은 KIS API의 실현 손익 데이터를 활용하는 get_realized_holdings_data 함수와 연계하여 더 정확하게 계산할 수 있습니다.
                # 현재는 단순히 매도 수량 * (현재가 - 평균단가)로 추정합니다.
                realized_est = abs(diff) * (cur_price - avg_price) if diff < 0 else 0
                changes.append(
                    f"{name} 수량 {arrow}: {old_qty} → {qty}주\n"
                    f"┗ 수익금: {profit:,}원 | 수익률: {rate:.2f}%"
                    + (f"\n┗ 매도 추정 수익: {int(realized_est):,}원" if diff < 0 else "")
                )
        except Exception as e:
            print(f"[파싱 오류] {e}")
            traceback.print_exc()
            continue

    parsed_items.sort(key=lambda x: x.get("eval", 0), reverse=True)
    if r:
        r.set("LAST_HOLDINGS", json.dumps(new_holdings))

    if only_changes:
        return "📌 [잔고 변동 내역]\n" + "\n".join(changes) if changes else ""

    report = ""
    if changes:
        report += "📌 [잔고 변동 내역]\n" + "\n".join(changes) + "\n\n"

    report += "📊 [보유 종목 수익률 + 수급 요약 보고]"
    for item in parsed_items:
        report += f"\n📌 {item['name']}\n"
        report += f"┗ 수량: {item['qty']}주 | 평균단가: {int(item['avg']):,}원 | 현재가: {int(item['cur']):,}원\n"
        report += f"┗ 평가금액: {item['eval']:,}원 | 수익금: {item['profit']:,}원 | 수익률: {item['rate']:.2f}%"
        if item["flow"]:
            report += f"\n┗ {item['flow']}"

    total_rate = (total_profit / total_invest * 100) if total_invest else 0.0
    report += f"\n\n📈 총 평가금액: {total_eval:,}원\n💰 총 수익금: {total_profit:,}원\n📉 총 수익률: {total_rate:.2f}%"

    # 2025 추정 수익률 계산
    try:
        cash = get_current_cash_balance(token)
        initial_assets = get_initial_assets_2025()
        net_deposit = get_net_deposit_2025(token)

        current_total_assets = total_eval + cash

        if initial_assets is None:
            # INITIAL_ASSETS_2025 값이 설정되지 않았을 때 안내 메시지
            report += f"\n\n⚠️ 2025년 추정 수익률 계산을 위해 'INITIAL_ASSETS_2025' 값을 설정해야 합니다."
            report += f"\n   (예: Redis에 'SET INITIAL_ASSETS_2025 {current_total_assets}' 명령어로 현재 총 자산({current_total_assets:,}원)을 초기 자산으로 설정할 수 있습니다.)"
        else:
            # 현재 총 자산 - 초기 자산 - 순입금액 = 추정 수익
            estimated_profit_2025 = current_total_assets - initial_assets - net_deposit
            estimated_rate_2025 = 0.0
            if initial_assets + net_deposit != 0: # 0으로 나누는 오류 방지
                estimated_rate_2025 = (estimated_profit_2025 / (initial_assets + net_deposit)) * 100
            elif estimated_profit_2025 != 0: # 초기 자산+순입금액이 0인데 수익이 있다면 무한대
                estimated_rate_2025 = float('inf') if estimated_profit_2025 > 0 else float('-inf')

            report += f"\n\n📅 2025 추정 수익: {int(estimated_profit_2025):,}원"
            report += f"\n📅 2025 추정 수익률: {estimated_rate_2025:.2f}%"
            if initial_assets == 0 and current_total_assets > 0:
                report += f"\n   (참고: 'INITIAL_ASSETS_2025' 값이 0으로 설정되어 있어 추정 수익률이 정확하지 않을 수 있습니다. 위 안내를 참고하여 설정해주세요.)"

    except Exception as e:
        report += f"\n📅 2025 추정 수익률 계산 오류: {e}"

    return report

def get_realized_holdings_data():
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance-rlz-pl"

    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8494R",
        "custtype": "P",  # 개인
        "Content-Type": "application/json"
    }

    acct_raw = KIS_ACCOUNT_NO.replace("-", "")
    cano, acct_cd = acct_raw[:8], acct_raw[8:]

    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acct_cd,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "00",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "COST_ICLD_YN": "N",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }

    res = requests.get(url, headers=headers, params=params).json()
    if res.get("rt_cd") != "0":
        raise Exception(f"[실현손익 API 실패] {res.get('msg1', res)}")

    output1 = res.get("output1", [])
    result = {}
    for item in output1:
        name = item.get("prdt_name", "")
        realized_profit = safe_int(item.get("evlu_pfls_amt"))
        result[name] = realized_profit
    return result

def get_yearly_realized_profit_2025():
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-period-trade-profit"

    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8715R",
        "custtype": "P",  # 개인 고객
        "Content-Type": "application/json"
    }

    acct_raw = KIS_ACCOUNT_NO.replace("-", "")
    cano, acct_cd = acct_raw[:8], acct_raw[8:]
    start_dt = "20250101"
    end_dt = datetime.now().strftime("%Y%m%d")

    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": acct_cd,
        "SORT_DVSN": "01",         # 과거순
        "PDNO": "",                # 전체 종목
        "INQR_STRT_DT": start_dt,
        "INQR_END_DT": end_dt,
        "CBLC_DVSN": "00",         # 전체
        "CTX_AREA_FK100": "",      # 최초조회
        "CTX_AREA_NK100": ""
    }

    res = requests.get(url, headers=headers, params=params).json()

    if res.get("rt_cd") != "0":
        raise Exception(f"[실현손익조회 실패] {res.get('msg1', res)}")

    output2 = res.get("output2", {})
    realized_profit = safe_int(output2.get("tot_rlzt_pfls", "0"))
    realized_rate = safe_float(output2.get("tot_pftrt", "0"))

    return realized_profit, realized_rate

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

last_status_report_hour = None
HOLIDAYS = ["2024-01-01", "2024-02-09", "2024-02-12", "2024-03-01", "2024-05-01", "2024-05-05", "2024-05-06", "2024-06-06", "2024-08-15", "2024-09-16", "2024-09-17", "2024-09-18", "2024-10-03", "2024-10-09", "2024-12-25"]

def is_holiday():
    today = datetime.now(timezone('Asia/Seoul')).strftime("%Y-%m-%d")
    return today in HOLIDAYS

def is_trading_day():
    return is_weekday() and not is_holiday()

def check_holdings_change_loop():
    global last_status_report_hour
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

def run():
    send_alert_message("✅ 체결/수익률 알림 봇이 시작되었습니다.")
    try:
        # 봇 시작 시 2025년 누적 리포트 (실현 손익 기준)를 먼저 보냅니다.
        profit, rate = get_yearly_realized_profit_2025()
        summary = (
            "📅 [2025 누적 리포트 (실현 손익 기준)]\n"
            f"💵 실현 수익금: {profit:,}원\n"
            f"📈 누적 수익률: {rate:.2f}%"
        )
        send_alert_message(summary)
    except Exception as e:
        send_alert_message(f"❌ 누적 리포트 조회 실패: {e}")
        traceback.print_exc()

    schedule.every().day.at("08:30").do(lambda: is_trading_day() and send_alert_message(get_account_profit(False)))
    schedule.every().day.at("16:00").do(lambda: is_trading_day() and send_alert_message(get_account_profit_with_yearly_report()))

    Thread(target=check_holdings_change_loop, daemon=True).start()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        send_alert_message("🛑 디스코드 잔고 알림 봇 실행 종료됨 (수동 중지)")
    except Exception as e:
        send_alert_message(f"❌ 알림 루프 오류: {e}")
        traceback.print_exc()
        time.sleep(10)

if __name__ == "__main__":
    run()