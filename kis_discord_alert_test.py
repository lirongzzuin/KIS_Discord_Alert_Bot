import os
import json
import time
import schedule
import requests
import redis
import threading
import websocket
from datetime import datetime
from pytz import timezone
from dotenv import load_dotenv
from base64 import b64decode
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# .env 로드
load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
APPROVAL_KEY = None

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

# 토큰 발급
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

# WebSocket Approval Key 발급
def get_approval_key():
    url = "https://openapi.koreainvestment.com:9443/oauth2/Approval"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "secretkey": KIS_APP_SECRET
    }
    res = requests.post(url, headers=headers, data=json.dumps(body)).json()
    if "approval_key" not in res:
        raise Exception(f"[approval_key 오류] {res}")
    return res["approval_key"]

# AES256 복호화
def aes_cbc_base64_dec(key, iv, cipher_text):
    cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
    return bytes.decode(unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size))

# 시장 수급 정보
def get_market_summary(token, stock_code):
    now = datetime.now(timezone('Asia/Seoul'))
    market_closed = now.hour > 15 or (now.hour == 15 and now.minute >= 30)

    if not market_closed:
        return None

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
            frgn_raw = output.get("frgn_ntby_qty", "").replace(",", "").strip()
            inst_raw = output.get("orgn_ntby_qty", "").replace(",", "").strip()
            frgn = int(frgn_raw) if frgn_raw.replace("-", "").isdigit() else 0
            inst = int(inst_raw) if inst_raw.replace("-", "").isdigit() else 0
            frgn_str = f"🟢 매수 {frgn:+,}주" if frgn > 0 else f"🔴 매도 {frgn:+,}주"
            inst_str = f"🟢 매수 {inst:+,}주" if inst > 0 else f"🔴 매도 {inst:+,}주"
            return f"외국인: {frgn_str} | 기관: {inst_str}"
        return "수급 정보 없음 또는 제공되지 않음"
    except Exception as e:
        return f"수급 정보 오류: {e}"

# 잔고 수익률 리포트
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
        raise Exception(f"API 응답 실패: {res}")

    output = res.get("output1", [])
    if not output:
        return "📭 보유 중인 종목이 없습니다."

    items = []
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
            summary = get_market_summary(token, code)
            total_profit += profit
            total_eval += eval_amt
            total_invest += invest_amt
            items.append(
                f"\n📌 {name}\n"
                f"┗ 수량: {qty}주 | 평균단가: {int(avg_price):,}원 | 현재가: {int(cur_price):,}원\n"
                f"┗ 평가금액: {eval_amt:,}원 | 수익금: {profit:,}원 | 수익률: {rate:.2f}%"
                + (f"\n┗ {summary}" if summary else "")
            )
        except Exception as e:
            items.append(f"\n⚠️ {item.get('prdt_name', '알 수 없음')} 수익률 계산 오류: {e}")

    total_rate = (total_profit / total_invest * 100) if total_invest else 0.0
    items.append(f"\n📈 총 평가금액: {total_eval:,}원\n💰 총 수익금: {total_profit:,}원\n📉 총 수익률: {total_rate:.2f}%")
    return "\n📊 [보유 종목 수익률 + 수급 요약 보고]" + "".join(items)

# WebSocket 실시간 체결 감지
def on_message(ws, message):
    try:
        data = json.loads(message)
        enc_data = data.get("body", {}).get("body", {}).get("output")
        if enc_data:
            aes_key = data["header"]["encrypt_key"]
            aes_iv = aes_key[:16]
            decrypted = aes_cbc_base64_dec(aes_key, aes_iv, enc_data)
            fields = decrypted.split("^")
            if len(fields) > 10:
                side = "매수" if fields[4] == "02" else "매도"
                stock_name = fields[18]
                quantity = fields[9]
                price = fields[10]
                send_discord_message(
                    f"[실시간 {side} 체결]\n"
                    f"종목명: {stock_name}\n"
                    f"수량: {quantity}주\n"
                    f"단가: {price}원"
                )
                # 체결 후 잔고 갱신
                send_discord_message(get_account_profit())
    except Exception as e:
        send_discord_message(f"❌ 실시간 체결 처리 오류: {e}")

def on_open(ws):
    param = {
        "header": {
            "approval_key": APPROVAL_KEY,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        },
        "body": {
            "input": {
                "tr_id": "H0STCNT0",
                "tr_key": KIS_ACCOUNT_NO.replace("-", "")
            }
        }
    }
    ws.send(json.dumps(param))

def start_websocket():
    global APPROVAL_KEY
    try:
        APPROVAL_KEY = get_approval_key()
        url = "wss://openapi.koreainvestment.com:9443/websocket"
        ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open)
        ws.run_forever()
    except Exception as e:
        send_discord_message(f"❌ WebSocket 연결 실패: {e}")

# 메인 루프
def run():
    send_discord_message("✅ 디스코드 체결/수익률 알림 봇이 시작되었습니다.")
    try:
        send_discord_message(get_account_profit())
    except Exception as e:
        send_discord_message(f"❌ 초기 리포트 오류: {e}")

    schedule.every().day.at("09:30").do(lambda: send_discord_message(get_account_profit()))
    schedule.every().day.at("12:00").do(lambda: send_discord_message(get_account_profit()))
    schedule.every().day.at("13:30").do(lambda: send_discord_message(get_account_profit()))
    schedule.every().day.at("16:00").do(lambda: send_discord_message(get_account_profit()))

    # 실시간 체결 감시
    threading.Thread(target=start_websocket, daemon=True).start()

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            send_discord_message("🛑 디스코드 알림 봇 종료됨")
            break
        except Exception as e:
            send_discord_message(f"❌ 알림 봇 실행 중 예외 발생: {e}")
            time.sleep(30)

if __name__ == "__main__":
    run()
