# 📈 한국투자증권 체결 및 수익률 디스코드 알림 봇

한국투자증권(OpenAPI)과 디스코드 웹훅을 연동하여  
- 매수/매도 **체결 알림**  
- 보유 종목의 **수익률/수익금 리포트**  
를 **디스코드 채널로 실시간 전송**하는 Python 봇입니다.

---

## ✅ 기능 요약

- 한국투자증권 체결 내역 조회 (REST API)
- 보유 종목 평가금액/수익률 자동 계산
- 디스코드 채널로 알림 전송
- `.env` 파일로 민감 정보 안전하게 관리
- `schedule`을 활용한 주기적 실행 자동화

---

## 📦 설치 및 실행 방법

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. `.env` 파일 작성

루트 디렉터리에 `.env` 파일을 생성하고 다음 값을 입력합니다:

```env
KIS_APP_KEY=your_kis_app_key
KIS_APP_SECRET=your_kis_app_secret
KIS_ACCOUNT_NO=12345678-01
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/your_webhook
```

- `KIS_APP_KEY`, `KIS_APP_SECRET`: 한국투자증권 API 키
- `KIS_ACCOUNT_NO`: `계좌번호-상품코드` 형식 (예: `12345678-01`)
- `DISCORD_WEBHOOK_URL`: 디스코드 웹훅 주소

### 3. `.gitignore` 설정

`.env`가 GitHub에 업로드되지 않도록 `.gitignore`에 다음을 추가합니다:

```
.env
__pycache__/
```

### 4. 실행

```bash
python main.py
```

---

## ⏱ 동작 스케줄

- 체결 내역 확인: **3분마다 한 번**
- 보유 종목 수익률 리포트: **2시간마다 한 번**

---

## 📝 메시지 예시

### 체결 알림

```
[매수 체결 알림]
종목명: 삼성전자
수량: 10주
단가: 72,000원
```

### 수익률 리포트

```
📊 [보유 종목 수익률 보고]
삼성전자 | 평가금액: 1,020,000원 | 수익금: +20,000원 | 수익률: 2.00%
네이버 | 평가금액: 950,000원 | 수익금: -50,000원 | 수익률: -5.00%

📈 총 평가금액: 1,970,000원
💰 총 수익금: -30,000원
```

---

## ⚠️ 주의사항

- 한국투자증권 API 사용을 위해서는 **사전 신청** 및 **앱 등록**이 필요합니다.
- 체결 알림은 웹소켓이 아닌 REST 방식이며, 100% 실시간은 아닙니다 (3분 단위 폴링).
- 본 코드는 **실전 계좌** 기준입니다 (`tr_id`: `TTTC8001R`, `TTTC8434R`).

---

## 🛠 기술 스택

- Python
- requests
- schedule
- python-dotenv
- Discord Webhook
- 한국투자증권 OpenAPI (REST)

---

## 📌 향후 개선 아이디어

- 웹소켓 방식 체결 감지 구현 (API 제공 시)
- 텔레그램/슬랙 연동 확장
- Flask 등 서버화 → Docker 배포 자동화

---

