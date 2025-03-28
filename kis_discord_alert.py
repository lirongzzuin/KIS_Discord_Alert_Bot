import requests
import json
import time
import schedule
from dotenv import load_dotenv
import os

# .env에서 변수 로드
load_dotenv()
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
KIS_ACCESS_TOKEN = None

# 디스코드 메시지 전송
def send_discord_message(content):
    data = {"content": content}
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=data)
        response.raise_for_status()
    except Exception as e:
        print(f"[디스코드 메시지 실패] {e}")

# 토큰 발급
def get_kis_access_token():
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    headers = {"Content-Type": "application/json"}
    data = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        result = response.json()
        print("🔍 [KIS 응답 내용]:", json.dumps(result, indent=2, ensure_ascii=False))
        if "access_token" in result:
            return result["access_token"]
        else:
            send_discord_message(f"❌ 액세스 토큰 발급 실패\n{json.dumps(result, indent=2, ensure_ascii=False)}")
            raise Exception("액세스 토큰을 받아오지 못했습니다.")
    except Exception as e:
        send_discord_message(f"❌ 예외 발생: {e}")
        raise

# 체결 내역 조회
def get_order_list():
    global KIS_ACCESS_TOKEN
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {KIS_ACCESS_TOKEN}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "TTTC8001R"
    }
    params = {
        "CANO": KIS_ACCOUNT_NO[:8],
        "ACNT_PRDT_CD": KIS_ACCOUNT_NO[9:],
        "INQR_STRT_DT": time.strftime("%Y%m%d"),
        "INQR_END_DT": time.strftime("%Y%m%d"),
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
    try:
        res = requests.get(url, headers=headers, params=params).json()
        if res.get("rt_cd") != "0":
            print(f"❌ 체결 내역 조회 실패: {res}")
            return []
        return res.get("output", [])
    except Exception as e:
        print(f"❌ 체결 내역 조회 오류: {e}")
        return []

# 보유 종목 수익률 계산
def get_account_profit():
    global KIS_ACCESS_TOKEN
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

    try:
        res = requests.get(url, headers=headers, params=params).json()
        if res.get("rt_cd") != "0":
            return "❌ 보유 종목 수익률 조회 실패"

        items = []
        total_profit = 0
        total_eval_amt = 0
        total_invest_amt = 0

        for item in res["output1"]:
            prdt_name = item["prdt_name"]
            hold_qty = int(item["hldg_qty"])
            avg_price = float(item["pchs_avg_pric"])
            current_price = float(item["prpr"])
            eval_amt = int(hold_qty * current_price)
            invest_amt = int(hold_qty * avg_price)
            profit_amt = eval_amt - invest_amt
            profit_rate = ((current_price - avg_price) / avg_price) * 100

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
                "profit_rate": profit_rate
            })

        items.sort(key=lambda x: x["eval_amt"], reverse=True)

        msg_lines = ["📊 [보유 종목 수익률 보고]"]
        for item in items:
            msg_lines.append(
                f"\n📌 {item['prdt_name']}"
                f"\n┗ 수량: {item['hold_qty']}주 | 평균단가: {item['avg_price']:,}원 | 현재가: {item['current_price']:,}원"
                f"\n┗ 평가금액: {item['eval_amt']:,}원 | 수익금: {item['profit_amt']:,}원 | 수익률: {item['profit_rate']:.2f}%"
            )

        total_profit_rate = (total_profit / total_invest_amt * 100) if total_invest_amt > 0 else 0.0
        msg_lines.append(
            f"\n📈 총 평가금액: {total_eval_amt:,}원"
            f"\n💰 총 수익금: {total_profit:,}원"
            f"\n📉 총 수익률: {total_profit_rate:.2f}%"
        )
        return "\n".join(msg_lines)
    except Exception as e:
        return f"❌ 보유 종목 조회 중 오류 발생: {e}"

# 체결 알림 감지
last_order_ids = set()

def check_and_notify_order():
    global last_order_ids
    orders = get_order_list()
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

# 수익률 보고 전송
def report_profit():
    profit_msg = get_account_profit()
    send_discord_message(profit_msg)

# 메인 실행 흐름
def run():
    global KIS_ACCESS_TOKEN
    KIS_ACCESS_TOKEN = get_kis_access_token()
    send_discord_message("✅ 디스코드 체결/수익률 알림 봇이 시작되었습니다.")
    report_profit()

    schedule.every(10).seconds.do(check_and_notify_order)
    schedule.every(1).hours.do(report_profit)

    print("🔔 디스코드 체결 + 수익률 알림 봇 실행 중...")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("\n[종료 요청됨] 알림 봇 종료 중...")
            send_discord_message("🛑 알림 봇이 종료되었습니다.")
            break
        except Exception as e:
            send_discord_message(f"❌ 알림 봇 실행 중 예외 발생: {e}")
            break

if __name__ == "__main__":
    run()
