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

# 환경 변수 로드
load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Redis 연결
try:
    r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
except Exception as e:
    print(f"Redis 연결 실패: {e}")
    r = None

def send_discord_message(content):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
    except Exception as e:
        print(f"[디스코드 전송 오류] {e}")
        traceback.print_exc()

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

def get_market_summary(token, stock_code):
    now = datetime.now(timezone('Asia/Seoul'))
    if now.hour < 15 or (now.hour == 15 and now.minute < 30):
        return ""  # 장중에는 생략

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
            frgn = int(output.get("frgn_ntby_qty", "0").replace(",", "").strip() or 0)
            inst = int(output.get("orgn_ntby_qty", "0").replace(",", "").strip() or 0)
            frgn_str = f"🟢 매수 {frgn:+,}주" if frgn > 0 else f"🔴 매도 {frgn:+,}주"
            inst_str = f"🟢 매수 {inst:+,}주" if inst > 0 else f"🔴 매도 {inst:+,}주"
            return f"외국인: {frgn_str} | 기관: {inst_str}"
        return "수급 정보 없음 또는 제공되지 않음"
    except Exception as e:
        return f"수급 정보 오류: {e}"

def get_account_profit():
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
        "CTX_AREA_NK100": ""  # 반드시 포함해야 함
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

    for item in output:
        try:
            name = item["prdt_name"]
            code = item["pdno"]
            qty = int(item["hldg_qty"])
            avg_price = float(item["pchs_avg_pric"])
            cur_price = float(item["prpr"])
            eval_amt = int(qty * cur_price)
            invest_amt = int(qty * avg_price)
            profit = eval_amt - invest_amt
            rate = (profit / invest_amt * 100) if invest_amt else 0.0
            investor_flow = get_market_summary(token, code)

            new_holdings[name] = qty
            parsed_items.append({
                "name": name,
                "code": code,
                "qty": qty,
                "avg": avg_price,
                "cur": cur_price,
                "eval": eval_amt,
                "profit": profit,
                "rate": rate,
                "flow": investor_flow
            })

            total_profit += profit
            total_eval += eval_amt
            total_invest += invest_amt
        except Exception as e:
            parsed_items.append({"name": item.get("prdt_name", "알 수 없음"), "flow": f"수익률 계산 오류: {e}", "eval": 0})

    parsed_items.sort(key=lambda x: x.get("eval", 0), reverse=True)

    changes = []
    if last:
        for name, qty in new_holdings.items():
            old_qty = last.get(name, 0)
            if qty != old_qty:
                diff = qty - old_qty
                arrow = "🟢 증가" if diff > 0 else "🔴 감소"
                matched = next((i for i in parsed_items if i['name'] == name), None)
                if matched:
                    changes.append(
                        f"{name} 수량 {arrow}: {old_qty} → {qty}주\n"
                        f"┗ 수익금: {matched['profit']:,}원 | 수익률: {matched['rate']:.2f}%"
                    )

    if r:
        r.set("LAST_HOLDINGS", json.dumps(new_holdings))

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

def run():
    send_discord_message("✅ 디스코드 체결/수익률 알림 봇이 시작되었습니다.")
    try:
        send_discord_message(get_account_profit())
    except Exception as e:
        send_discord_message(f"❌ 리포트 오류: {e}")
        traceback.print_exc()

    schedule.every().day.at("09:30").do(lambda: send_discord_message(get_account_profit()))
    schedule.every().day.at("12:00").do(lambda: send_discord_message(get_account_profit()))
    schedule.every().day.at("13:30").do(lambda: send_discord_message(get_account_profit()))
    schedule.every().day.at("15:30").do(lambda: send_discord_message(get_account_profit()))

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            send_discord_message(f"❌ 알림 루프 오류: {e}")
            traceback.print_exc()
            time.sleep(10)

if __name__ == "__main__":
    run()
