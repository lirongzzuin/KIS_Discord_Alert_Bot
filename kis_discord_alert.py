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

def get_account_profit(only_changes=True):
    token = get_kis_access_token()
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
        raise Exception(f"API 응답 실패: {res}")

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

            # Fallback: 수익금, 수익률 직접 계산 (API값이 0일 경우)
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
                realized = abs(diff) * (cur_price - avg_price)
                changes.append(
                    f"{name} 수량 {arrow}: {old_qty} → {qty}주\n"
                    f"┗ 수익금: {profit:,}원 | 수익률: {rate:.2f}%"
                    + (f"\n┗ 매도 추정 수익: {int(realized):,}원" if diff < 0 else "")
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
    return report

def get_realized_profit_2025():
    token = get_kis_access_token()
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8001R",  # 실전계좌
        "Content-Type": "application/json"
    }

    start_dt = "20250101"
    end_dt = datetime.now().strftime("%Y%m%d")
    total_realized_profit = 0

    params = {
        "CANO": KIS_ACCOUNT_NO[:8],
        "ACNT_PRDT_CD": KIS_ACCOUNT_NO[9:],
        "INQR_STRT_DT": start_dt,
        "INQR_END_DT": end_dt,
        "SLL_BUY_DVSN_CD": "00",  # 전체: 00, 매도: 01, 매수: 02
        "INQR_DVSN": "01",  # 조회 구분: 역순 01, 정순 02
        "PDNO": "",         # 종목코드(없으면 전체)
        "CCLD_DVSN": "00",  # 체결 구분: 전체
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_3": "00", 
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }

    res = requests.get(url, headers=headers, params=params).json()
    if res.get("rt_cd") != "0":
        raise Exception(f"[실현손익 API 실패] {res}")

    for item in res.get("output1", []):
        # 매도체결만 추출하여 실현손익 계산
        if item["sll_buy_dvsn_cd"] == "01":  # 매도
            sell_amt = safe_int(item.get("cntr_amt"))  # 매도금액
            buy_amt = safe_int(item.get("pchs_amt"))   # 매입금액
            profit = sell_amt - buy_amt
            total_realized_profit += profit

    return total_realized_profit

def get_account_profit_with_yearly_report():
    main_report = get_account_profit(False)
    try:
        yearly_profit = get_realized_profit_2025()
        yearly_summary = f"\n📅 [2025 누적 리포트]\n💵 실현 수익금: {yearly_profit:,}원"
    except Exception as e:
        yearly_summary = f"\n📅 [2025 누적 리포트]\n❌ 누적 수익 조회 실패: {e}"
    return main_report + yearly_summary

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
                now = datetime.now(timezone('Asia/Seoul'))
                report = get_account_profit(only_changes=True if only_changes else False)
                if report:
                    send_alert_message(report)
                elif now.minute == 0 and now.hour % 2 == 1 and now.hour != last_status_report_hour:
                    send_alert_message(get_account_profit(only_changes=False))
                    last_status_report_hour = now.hour
        except Exception as e:
            send_alert_message(f"❌ 자동 잔고 체크 오류: {e}")
            traceback.print_exc()
        time.sleep(60)

def run():
    send_alert_message("✅ 체결/수익률 알림 봇이 시작되었습니다.")
    try:
        send_alert_message(get_account_profit(only_changes=False))
    except Exception as e:
        send_alert_message(f"❌ 리포트 오류: {e}")
        traceback.print_exc()

    schedule.every().day.at("08:30").do(lambda: send_alert_message(get_account_profit(False)))
    schedule.every().day.at("09:30").do(lambda: send_alert_message(get_account_profit(False)))
    schedule.every().day.at("13:30").do(lambda: send_alert_message(get_account_profit(False)))
    schedule.every().day.at("15:30").do(lambda: send_alert_message(get_account_profit(False)))
    schedule.every().day.at("16:00").do(lambda: send_alert_message(get_account_profit_with_yearly_report()))


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