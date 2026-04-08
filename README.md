# KIS Trading Alert Bot (Discord & Telegram)

한국투자증권 OpenAPI 기반의 실시간 투자 알림 봇.  
보유 종목 변동, 수익률 리포트, ETF 시장 브리핑, 외국인 수급 추세 등을 Discord + Telegram으로 자동 전송합니다.

---

## 주요 기능

### 실시간 잔고 변동 감지
- **국내**: 장중(09~15시) 60초마다 보유 수량 변화 감지 → 매수/매도 체결 알림
- **해외**: 24시간 60초마다 보유 변화 감지 (미국/일본/홍콩/중국)
- 스냅샷 기반 디듀플리케이션으로 중복 알림 방지

### 종합 리포트 (매일 08:30 / 16:00)
- 보유 종목별 수익률 + 외국인/기관 수급 요약
- **총 자산 현황**: 국내 + 해외 + 예수금
- **총 누적 수익**: 미실현 평가손익 + 전체기간 실현손익 (KIS API 최대 10년)
- **올해 누적 수익**: 미실현 + 올해 실현손익 (매도 확정 기준)

### ETF 브리핑
- **주간** (매주 첫 거래일 08:10): 신규 상장 ETF 감지 + 거래량 TOP 5
- **월간** (매월 첫 거래일 08:10): 3개월 수익률 TOP/WORST 10 + 시가총액 TOP 10
- 네이버 금융 ETF API 기반 (무료, 키 불필요)

### 외국인 수급 추세 (매일 08:20)
- Redis에 일자별 외국인 순매수 스냅샷 누적
- 상승 추세 스코어링 → TOP N 리포트

### 안정성
- Redis 미사용 시에도 핵심 기능 동작 (스냅샷/캐시 기능만 제한)
- Discord 2000자 / Telegram 4096자 자동 분할
- Discord Rate Limit 자동 대응
- Docker SIGTERM 핸들링 (fly.io graceful shutdown)
- 한국 공휴일 자동 판별 (`holidays` 패키지)

---

## 스케줄

| 시간(KST) | 기능 | 주기 |
|---|---|---|
| 08:10 | 주간 ETF 브리핑 (신규 상장 감지) | 매주 첫 거래일 |
| 08:10 | 월간 ETF 수익률 리포트 | 매월 첫 거래일 |
| 08:20 | 외국인 수급 추세 TOP N | 매일 거래일 |
| 08:30 | 국내 보유 종목 상세 리포트 | 매일 거래일 |
| 15:50 | 외국인 수급 스냅샷 저장 | 매일 거래일 |
| 16:00 | 종합 리포트 (자산현황 + 누적수익) | 매일 거래일 |
| 매 60초 | 국내(장중)/해외(24h) 잔고 변동 감지 | 실시간 |

---

## 설치

### 요구 사항
- Python 3.10+
- (선택) Redis 6+ (Upstash 무료 티어 추천)
- KIS OpenAPI 앱키, Discord Webhook, Telegram Bot/Chat ID

### 의존성
```bash
pip install -r requirements.txt
```

### 환경변수 (.env)
```env
# KIS 인증 (필수)
KIS_APP_KEY=your_kis_app_key
KIS_APP_SECRET=your_kis_app_secret
KIS_ACCOUNT_NO=12345678-01

# 알림 채널 (최소 1개 필수)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxxx/xxxx
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Redis (선택 — 스냅샷/캐시/수급추세에 사용)
REDIS_URL=redis://default:password@host:6379

# 환율 폴백 (선택)
FALLBACK_USDKRW=1350
FALLBACK_JPYKRW=9.5
FALLBACK_HKDKRW=175
FALLBACK_CNYKRW=190
FX_CACHE_TTL_SEC=900

# 외국인 추세 TopN (선택)
FOREIGN_TREND_TOPN=15
```

### 실행
```bash
python kis_discord_alert.py
```

---

## fly.io 배포

### 1. fly.io CLI 설치 및 로그인
```bash
brew install flyctl
flyctl auth login
```

### 2. 앱 생성 (최초 1회)
```bash
cd KIS_Discord_Alert_Bot
flyctl apps create kis-discord-alert-bot --org personal
```

### 3. Redis 생성 (선택)
```bash
flyctl redis create --name kis-alert-redis --region nrt --no-replicas --enable-eviction -o personal
```
- ProdPack 질문에 **No** 선택
- 출력된 Redis URL 복사

### 4. 시크릿 등록
```bash
flyctl secrets set \
  KIS_APP_KEY="..." \
  KIS_APP_SECRET="..." \
  KIS_ACCOUNT_NO="12345678-01" \
  DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..." \
  TELEGRAM_BOT_TOKEN="..." \
  TELEGRAM_CHAT_ID="..." \
  REDIS_URL="redis://..." \
  --app kis-discord-alert-bot
```

### 5. 배포
```bash
flyctl deploy
```

### 6. 확인
```bash
flyctl logs --app kis-discord-alert-bot --no-tail | tail -20
flyctl status --app kis-discord-alert-bot
```

---

## 기술 세부사항

### KIS API 활용
| API (tr_id) | 용도 |
|---|---|
| `TTTC8434R` | 국내 잔고/평가 |
| `TTTC8494R` | 국내 실현손익 |
| `TTTC8715R` | 기간별 누적 실현손익 (최대 10년) |
| `TTTC8908R` | 예수금/주문가능금액 |
| `CTRP6504R` | 해외 현재잔고 |
| `HHDFS00000300` | 환율 조회 |
| `FHKST01010900` | 외국인/기관 수급 |
| `FHPTJ04400000` | 외국인/기관 종합 |

### 누적 수익 계산 방식
- **미실현 손익** = 현재 보유 평가금액 - 투자원금 (국내 + 해외)
- **실현 손익** = KIS API `TTTC8715R` 기간별 조회 (매도 확정)
- **총 누적** = 미실현 + 전체기간 실현 (최대 10년)
- **올해 누적** = 미실현 + 올해 실현
- Redis 의존 없이 KIS API만으로 계산

### ETF 신규 상장 감지
- 네이버 금융 ETF API에서 전종목 코드 세트를 일일 비교
- 이전 스냅샷에 없던 코드가 등장하면 신규 상장으로 판별
- Redis에 스냅샷 저장 (Redis 없으면 감지 불가)

---

## 라이선스

개인/학습 목적 사용 및 수정 가능. 상업적 이용은 별도 협의 필요.  
Copyright © 2025-2026 Younggyun Lee.
