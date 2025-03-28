import os
import json
import time
import threading
import requests
import schedule
import websocket
import ssl
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

KIS_ACCESS_TOKEN = None
KIS_APPROVAL_KEY = None
last_order_ids = set()

# 디스코드 메시지 전송
def send_discord_message(content):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
    except Exception as e:
        print(f"[디스코드 전송 실패] {e}")

# REST API 토큰 발급
def get_kis_access_token():
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    headers = {"Content-Type": "application/json"}
    data = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET
    }
    res = requests.post(url, headers=headers, data=json.dumps(data)).json()
    if "access_token" in res:
        return res["access_token"]
    raise Exception("액세스 토큰 발급 실패")

# 웹소켓 approval_key 발급
def get_approval_key():
    url = "https://openapi.koreainvestment.com:9443/oauth2/Approval"
    headers = {"Content-Type": "application/json"}
    data = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "secretkey": KIS_APP_SECRET
    }
    res = requests.post(url, headers=headers, data=json.dumps(data)).json()
    if "approval_key" in res:
        return res["approval_key"]
    raise Exception("approval_key 발급 실패")

# 외국인/기관 수급 정보
def get_market_summary(stock_code):
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-investor"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {KIS_ACCESS_TOKEN}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010900"
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code
    }
    try:
        res = requests.get(url, headers=headers, params=params).json()
        latest = res.get("output", [{}])[0]
        frgn = int(latest.get("frgn_ntby_qty", 0))
        inst = int(latest.get("orgn_ntby_qty", 0))
        return f"외국인: {frgn:+,}주 | 기관: {inst:+,}주"
    except:
        return "수급 정보 오류"

# 잔고 수익률 + 수급 보고
def get_account_profit():
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {KIS_ACCESS_TOKEN}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8434R"
    }
    params = {
        "CANO": KIS_ACCOUNT_NO[:8],
        "ACNT_PRDT_CD": KIS_ACCOUNT_NO[9:],
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    res = requests.get(url, headers=headers, params=params).json()
    if res.get("rt_cd") != "0":
        return "❌ 보유 종목 수익률 조회 실패"

    items = []
    total_profit, total_eval_amt, total_invest_amt = 0, 0, 0

    for item in res.get("output1", []):
        prdt_name = item["prdt_name"]
        stock_code = item["pdno"]
        hold_qty = int(item["hldg_qty"])
        avg_price = float(item["pchs_avg_pric"])
        current_price = float(item["prpr"])
        eval_amt = int(hold_qty * current_price)
        invest_amt = int(hold_qty * avg_price)
        profit_amt = eval_amt - invest_amt
        profit_rate = ((current_price - avg_price) / avg_price) * 100
        summary = get_market_summary(stock_code)

        total_profit += profit_amt
        total_eval_amt += eval_amt
        total_invest_amt += invest_amt

        items.append({
            "prdt_name": prdt_name,
            "hold_qty": hold_qty,
            "avg_price": int(avg_price),
            "current_price": int(current_price),
            "eval_amt": eval_amt,
            "profit_amt": profit_amt,
            "profit_rate": profit_rate,
            "summary": summary
        })

    items.sort(key=lambda x: x["eval_amt"], reverse=True)
    total_profit_rate = (total_profit / total_invest_amt * 100) if total_invest_amt else 0.0

    msg = ["\n📊 [보유 종목 수익률 + 수급 요약 보고]"]
    for item in items:
        msg.append(
            f"\n📌 {item['prdt_name']}\n"
            f"┗ 수량: {item['hold_qty']}주 | 평균단가: {item['avg_price']:,}원 | 현재가: {item['current_price']:,}원\n"
            f"┗ 평가금액: {item['eval_amt']:,}원 | 수익금: {item['profit_amt']:,}원 | 수익률: {item['profit_rate']:.2f}%\n"
            f"┗ {item['summary']}"
        )
    msg.append(
        f"\n📈 총 평가금액: {total_eval_amt:,}원\n💰 총 수익금: {total_profit:,}원\n📉 총 수익률: {total_profit_rate:.2f}%"
    )
    return "\n".join(msg)

# 웹소켓 핸들러
def on_open(ws):
    print("🟢 웹소켓 연결 성공")
    payload = {
        "header": {
            "approval_key": KIS_APPROVAL_KEY,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
            "tr_id": "H0STCNI0"
        },
        "body": {
            "input": {
                "tr_id": KIS_ACCOUNT_NO,
                "tr_key": "ALL"
            }
        }
    }
    ws.send(json.dumps(payload))

def on_message(ws, message):
    try:
        data = json.loads(message)
        if "body" in data:
            body = data["body"]
            pdno = body.get("pdno", "-")
            qty = body.get("qty", "-")
            prun = body.get("prun", "-")
            odno = body.get("odno", "-")
            msg = f"[실시간 체결 알림]\n종목코드: {pdno} | 수량: {qty}주 | 단가: {prun}원\n주문번호: {odno}"
            send_discord_message(msg)
    except Exception as e:
        print(f"[웹소켓 메시지 처리 오류] {e}")

def on_error(ws, error):
    print(f"[웹소켓 오류] {error}")
    send_discord_message(f"❌ 웹소켓 오류: {error}")

def on_close(ws, *_):
    print("📴 웹소켓 종료")
    send_discord_message("📴 웹소켓 연결 종료")

def start_websocket():
    url = "wss://openapivts.koreainvestment.com:29443/websocket"  # 실전 계좌면 openapi.koreainvestment.com
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

# 실행 메인 함수
def run():
    global KIS_ACCESS_TOKEN, KIS_APPROVAL_KEY
    try:
        KIS_ACCESS_TOKEN = get_kis_access_token()
        KIS_APPROVAL_KEY = get_approval_key()
        send_discord_message("✅ 디스코드 체결/수익률 알림 봇 시작됨")
        send_discord_message(get_account_profit())
    except Exception as e:
        send_discord_message(f"❌ 인증 실패: {e}")
        return

    schedule.every(1).hours.do(lambda: send_discord_message(get_account_profit()))

    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            send_discord_message("🛑 알림 봇 종료됨")
            break
        except Exception as e:
            send_discord_message(f"❌ 알림 봇 예외 발생: {e}")
            break

if __name__ == "__main__":
    run()
