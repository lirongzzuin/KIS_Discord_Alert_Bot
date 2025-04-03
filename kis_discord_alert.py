import os
import json
import time
import schedule
import requests
import redis
from datetime import datetime
from pytz import timezone
from dotenv import load_dotenv

# .env 로드
load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

# 디스코드 전송
def send_discord_message(content):
    try:
        res = requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
        res.raise_for_status()
    except Exception as e:
        print(f"[디스코드 전송 오류] {e}")

# 토큰 캐싱 및 발급
def get_kis_access_token():
    now = time.time()
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
    r.set("KIS_ACCESS_TOKEN", token)
    r.set("KIS_TOKEN_EXPIRE_TIME", now + expires_in - 60)
    return token

# 수급 정보
def get_market_summary(token, stock_code):
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

            return f"외국인 순매수: {frgn:+,}주 | 기관 순매수: {inst:+,}주"
        return "수급 정보 없음 또는 아직 제공되지 않음"
    except Exception as e:
        return f"수급 정보 오류: {e}"

# 수익률 및 수급 리포트
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
                f"┗ 평가금액: {eval_amt:,}원 | 수익금: {profit:,}원 | 수익률: {rate:.2f}%\n"
                f"┗ {summary}"
            )
        except Exception as e:
            items.append(f"\n⚠️ {item.get('prdt_name', '알 수 없음')} 수익률 계산 오류: {e}")
            continue

    total_rate = (total_profit / total_invest * 100) if total_invest else 0.0
    items.append(
        f"\n📈 총 평가금액: {total_eval:,}원\n💰 총 수익금: {total_profit:,}원\n📉 총 수익률: {total_rate:.2f}%"
    )
    return "\n📊 [보유 종목 수익률 + 수급 요약 보고]" + "".join(items)

# 체결 내역 감지용: 마지막 체결된 주문번호 추적
last_order_ids = set()

# 체결 내역 확인
def check_order_and_notify():
    try:
        token = get_kis_access_token()
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "TTTC8001R",
            "Content-Type": "application/json"
        }
        params = {
            "CANO": KIS_ACCOUNT_NO[:8],
            "ACNT_PRDT_CD": KIS_ACCOUNT_NO[9:],
            "INQR_STRT_DT": datetime.now().strftime("%Y%m%d"),
            "INQR_END_DT": datetime.now().strftime("%Y%m%d"),
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "INQR_DVSN_1": "1",
            "PDNO": "",
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }

        res = requests.get(url, headers=headers, params=params).json()
        if res.get("rt_cd") != "0":
            return

        orders = res.get("output", [])
        for order in orders:
            odno = order.get("odno")
            if odno and odno not in last_order_ids:
                type_str = "매수" if order.get("sll_buy_dvsn_cd") == "02" else "매도"
                msg = (
                    f"[{type_str} 체결 알림]\n"
                    f"종목명: {order['prdt_name']}\n"
                    f"수량: {order['ord_qty']}주\n"
                    f"단가: {order['ord_unpr']}원"
                )
                send_discord_message(msg)
                last_order_ids.add(odno)
    except Exception as e:
        send_discord_message(f"❌ 체결 알림 오류: {e}")

# 메인 실행
def run():
    send_discord_message("✅ 디스코드 체결/수익률 알림 봇이 시작되었습니다.")

    # 최초 실행 시 리포트
    try:
        send_discord_message(get_account_profit())
    except Exception as e:
        send_discord_message(f"❌ 초기 리포트 오류: {e}")

    # 체결 내역은 계속 감지
    schedule.every(1).minutes.do(check_order_and_notify)

    # 수익률/수급 리포트는 하루 3번만 전송
    schedule.every().day.at("09:00").do(lambda: send_discord_message(get_account_profit()))
    schedule.every().day.at("12:00").do(lambda: send_discord_message(get_account_profit()))
    schedule.every().day.at("16:00").do(lambda: send_discord_message(get_account_profit()))

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
