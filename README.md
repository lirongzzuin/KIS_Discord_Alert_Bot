# KIS Trading Alert Bot

한국투자증권 OpenAPI 기반 투자 알림 봇.  
보유 종목 변동, 자산 리포트, ETF 브리핑, 수급 분석을 **Discord + Telegram**으로 자동 전송합니다.

---

## 어떤 알림을 받을 수 있나요?

### 📌 매수/매도 체결 알림 (실시간)
주식을 사거나 팔면 **60초 이내에 알림**이 옵니다.
- 국내 주식: 장중(09~15시)에만 감지
- 해외 주식: 24시간 감지 (미국/일본/홍콩/중국)
- 수량 변화가 없으면 알림을 보내지 않아 스팸 방지

### 📊 매일 받는 리포트
**매일 아침(08:30)과 장 마감(16:00)에 종합 리포트**를 받습니다.

리포트에 포함되는 정보:
- **보유 종목별 수익률** — 평균 매입가 vs 현재가, 손익금, 수익률
- **외국인/기관 수급** — 종목별 외국인·기관 순매수량 (장 마감 후)
- **총 자산 현황** — 보유종목 평가 + 예수금 = 총자산
- **보유 평가손익** — 투자원금 대비 현재 평가 차이
- **전일 대비 변동** — 어제와 오늘 자산 차이
- **실현손익** — 매도해서 확정된 순수익 (올해 / 최근 10년)
- **수급 TOP 3** — 당일 외국인·기관 순매수 상위 종목

### 📈 ETF 브리핑
- **매일 08:10**: 신규 상장 ETF 감지 시 당일 상세 리포트 (현재가, NAV, 시총, 거래량 + 관련 뉴스)
- **매주 첫 거래일**: 시장 지수 + 신규 ETF + **상장 예정 ETF** (DART 증권신고서 기반, 관련 뉴스 포함)
- **매월 첫 거래일**: 3개월 수익률 TOP/WORST 10 + 시가총액 TOP 10
- **장 마감(16:00)**: ETF 거래량 TOP 3

### 🌍 외국인/기관 수급 (매일)
- **08:20**: 최근 7일간 외국인·기관 순매수 **상승 추세** 종목 TOP
- **08:30/16:00**: 당일 외국인·기관 순매수 TOP 3

---

## 스케줄 요약

| 시간 | 내용 |
|---|---|
| 08:10 | 신규 ETF 상장 리포트 (매일, 감지 시) |
| 08:10 | 주간 브리핑: 시장 지수 + 신규 ETF (매주 첫 거래일) |
| 08:10 | 월간 ETF 리포트 (매월 첫 거래일) |
| 08:20 | 외국인/기관 수급 추세 TOP |
| 08:30 | 종합 리포트 + 수급 TOP 3 + ETF 거래량 TOP 3 |
| 15:50 | 외국인/기관 수급 스냅샷 저장 |
| 16:00 | 종합 리포트 + 수급 TOP 3 + ETF 거래량 TOP 3 |
| 매 60초 | 잔고 변동 감지 (매수/매도 알림) |

---

## 설치 및 실행

### 필요한 것
- Python 3.10+
- KIS OpenAPI 앱키/시크릿 ([한국투자증권 OpenAPI](https://apiportal.koreainvestment.com/))
- Discord Webhook URL 또는 Telegram Bot Token + Chat ID
- (선택) Redis — Upstash 무료 티어 추천

### 설치
```bash
git clone https://github.com/lirongzzuin/KIS_Discord_Alert_Bot.git
cd KIS_Discord_Alert_Bot
pip install -r requirements.txt
cp .env.example .env  # 환경변수 편집
python kis_discord_alert.py
```

### 환경변수 (.env)
```env
# 필수
KIS_APP_KEY=한국투자증권_앱키
KIS_APP_SECRET=한국투자증권_시크릿
KIS_ACCOUNT_NO=12345678-01
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
TELEGRAM_BOT_TOKEN=봇토큰
TELEGRAM_CHAT_ID=-100채널ID

# 선택
REDIS_URL=redis://default:password@host:6379
```

---

## fly.io 배포 (무료)

```bash
# 1. 앱 생성
flyctl apps create kis-discord-alert-bot --org personal

# 2. Redis 생성
flyctl redis create --name kis-alert-redis --region nrt --no-replicas --enable-eviction -o personal

# 3. 시크릿 등록
flyctl secrets set \
  KIS_APP_KEY="..." KIS_APP_SECRET="..." KIS_ACCOUNT_NO="..." \
  DISCORD_WEBHOOK_URL="..." TELEGRAM_BOT_TOKEN="..." TELEGRAM_CHAT_ID="..." \
  REDIS_URL="..." --app kis-discord-alert-bot

# 4. 배포
flyctl deploy

# 5. 로그 확인
flyctl logs --app kis-discord-alert-bot --no-tail | tail -20
```

---

## 기술 스택

- **Python 3.11** + schedule + requests + redis + holidays
- **KIS OpenAPI** — 잔고, 실현손익, 수급, 환율 조회
- **네이버 금융 API** — ETF 전종목 조회 (신규 상장 감지)
- **Discord Webhook + Telegram Bot API** — 알림 전송
- **Upstash Redis** — 스냅샷, 캐시, 수급 추세 데이터
- **fly.io** — 무료 클라우드 배포 (shared-cpu-1x, 256MB)

---

## 라이선스

개인/학습 목적 사용 및 수정 가능. 상업적 이용은 별도 협의 필요.  
Copyright © 2025-2026 Younggyun Lee.
