# KIS Trading Alert Bot (Discord & Telegram)

한국투자증권 OpenAPI 기반의 실시간 투자 알림 봇.  
보유 종목 변동, 수익률 리포트, ETF 시장 브리핑, 외국인/기관 수급 등을 Discord + Telegram으로 자동 전송합니다.

---

## 주요 기능

### 실시간 잔고 변동 감지
- **국내**: 장중(09~15시) 60초마다 보유 수량 변화 감지 → 매수/매도 체결 알림
- **해외**: 24시간 60초마다 보유 변화 감지 (미국/일본/홍콩/중국)
- 스냅샷 기반 디듀플리케이션으로 중복 알림 방지

### 종합 리포트 (매일 08:30 / 16:00)
- 보유 종목별 수익률 + 외국인/기관 수급 요약
- **총 자산**: KIS API `output2` 기반 정확한 총자산 (보유평가 + 예수금)
- **보유 평가손익**: 투자원금 대비 현재 평가 (매입가 기준)
- **전일 대비**: 자산 증감
- **실현손익**: 올해/전체(최대 10년) 매도 확정 수익 (KIS API `TTTC8715R`)
- **연초 대비 수익률**: Redis 연초 자산 스냅샷 기반 자동 계산
- **수급 TOP 3**: 당일 외국인/기관 순매수 상위 종목

### ETF 브리핑
- **주간** (매주 첫 거래일 08:10): 신규 상장 ETF 감지 + 거래량 TOP 5
- **월간** (매월 첫 거래일 08:10): 3개월 수익률 TOP/WORST 10 + 시가총액 TOP 10
- 네이버 금융 ETF API 기반 (무료, 키 불필요)

### 외국인 수급 추세 (매일 08:20)
- Redis에 일자별 외국인 순매수 스냅샷 누적
- 상승 추세 스코어링 → TOP N 리포트

### 안정성
- Redis 미사용 시에도 핵심 기능 동작 (스냅샷/캐시 기능만 제한)
- Discord 2000자 / Telegram 4096자 자동 분할 + Rate Limit 대응
- Docker SIGTERM/SIGINT 핸들링 (fly.io graceful shutdown)
- 한국 공휴일 자동 판별 (`holidays` 패키지, 연도 하드코딩 없음)

---

## 스케줄

| 시간(KST) | 기능 | 주기 |
|---|---|---|
| 08:10 | 주간 ETF 브리핑 (신규 상장 감지) | 매주 첫 거래일 |
| 08:10 | 월간 ETF 수익률 리포트 | 매월 첫 거래일 |
| 08:20 | 외국인 수급 추세 TOP N | 매일 거래일 |
| 08:30 | 종합 리포트 (보유종목 + 자산 + 수급 TOP) | 매일 거래일 |
| 15:50 | 외국인 수급 스냅샷 저장 | 매일 거래일 |
| 16:00 | 종합 리포트 (보유종목 + 자산 + 실현손익 + 수급 TOP) | 매일 거래일 |
| 매 60초 | 국내(장중)/해외(24h) 잔고 변동 감지 | 실시간 |

---

## 리포트 예시

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💼 [총 자산] 109,087,723원
┗ 보유종목: 84,891,300원 (원금: 85,004,612원)
┗ 예수금: 21,377,053원
┗ 🔴 보유 평가손익: -113,312원 (-0.13%)
┗ 🟢 전일 대비: +4,822,655원

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💰 [실현손익] 매도 확정 수익
┗ 🟢 2026년 (2026.01.01~04.08): 21,784,321원 (+35.58%)
┗ 🟢 전체 (2016.04.10~04.08): 24,430,042원 (+7.36%)
```

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
TELEGRAM_CHAT_ID=-100xxxxxxxxxx

# Redis (선택 — 스냅샷/캐시/수급추세에 사용)
REDIS_URL=redis://default:password@host:6379
```

### 실행
```bash
python kis_discord_alert.py
```

---

## fly.io 배포

```bash
# 1. 앱 생성
flyctl apps create kis-discord-alert-bot --org personal

# 2. Redis 생성 (선택)
flyctl redis create --name kis-alert-redis --region nrt --no-replicas --enable-eviction -o personal

# 3. 시크릿 등록
flyctl secrets set \
  KIS_APP_KEY="..." \
  KIS_APP_SECRET="..." \
  KIS_ACCOUNT_NO="..." \
  DISCORD_WEBHOOK_URL="..." \
  TELEGRAM_BOT_TOKEN="..." \
  TELEGRAM_CHAT_ID="..." \
  REDIS_URL="..." \
  --app kis-discord-alert-bot

# 4. 배포
flyctl deploy

# 5. 확인
flyctl logs --app kis-discord-alert-bot --no-tail | tail -20
```

---

## 기술 세부사항

### KIS API 활용
| API (tr_id) | 용도 |
|---|---|
| `TTTC8434R` | 국내 잔고/평가 (output1: 종목별, output2: 계좌요약) |
| `TTTC8715R` | 기간별 실현손익 (매도 확정, 최대 10년) |
| `CTRP6504R` | 해외 현재잔고 |
| `HHDFS00000300` | 환율 조회 |
| `FHKST01010900` | 종목별 외국인/기관 수급 |
| `FHPTJ04400000` | 외국인/기관 순매수 종합 (수급 TOP) |

### 수익 계산 방식
- **보유 평가손익** = 현재 평가금액 - 매입원금 (KIS output2 `evlu_pfls_smtl_amt`)
- **실현손익** = 매도 확정 순수익 (KIS `TTTC8715R`, 원금 제외)
- **올해 실현** = 2026.01.01~ 기간 조회
- **전체 실현** = 최대 10년 기간 조회 (API 제한)
- **연초 대비** = Redis 자동 저장된 연초 총자산 vs 현재 총자산

---

## 라이선스

개인/학습 목적 사용 및 수정 가능. 상업적 이용은 별도 협의 필요.  
Copyright © 2025-2026 Younggyun Lee.
