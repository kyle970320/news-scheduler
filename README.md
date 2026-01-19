# 📰 주식 뉴스 수집 및 AI 분석 스케줄러

주식 관련 뉴스를 실시간으로 수집하고, Google Gemini AI를 활용해 감정 분석 및 스코어링을 수행한 후 Supabase 데이터베이스에 저장하는 백엔드 스케줄러입니다.

## 🎯 주요 기능

### 1. 뉴스 수집 (`fetch.mjs`)
- **시간 기반 수집**: 최근 1시간 이내 발행된 뉴스를 자동으로 수집
- **중복 제거**: `article_url` 기준으로 중복 뉴스 자동 필터링
- **데이터 정리**: 2일 이상 된 오래된 뉴스 자동 삭제

### 2. AI 기반 감정 분석 (`run.mjs`)
- **Insight 단위 분석**: 뉴스의 각 Insight를 개별적으로 분석
- **Gemini AI 스코어링**: Google Gemini 2.0 Flash 모델을 사용한 감정 점수 산출 (-100 ~ +100)
- **신뢰도 계산**: 
  - 모델 신뢰도 (`confidence_model`)
  - 룰 기반 신뢰도 (`confidence_rule`): 소스 신뢰도, 이벤트 타입, 감정 강도 등을 종합 고려
- **롤업 처리**: Insight 단위 결과를 기사 단위로 집계
- **호재/악재 필터링**: 강한 호재(score ≥ 60) 및 악재(score ≤ -60) 자동 식별

### 3. 소스 분류
뉴스 소스를 다음과 같이 자동 분류:
- `wire`: PR Newswire, Business Wire 등
- `major_press`: Reuters, Bloomberg, WSJ 등 주요 글로벌 언론
- `regional_press`: Nikkei, Guardian, BBC 등 지역 언론
- `financial_portal`: Yahoo Finance, Benzinga 등 금융 포털
- `company_ir`: 기업 IR 페이지
- `blog_or_forum`: Medium, Reddit 등 커뮤니티

### 4. 이벤트 분류
키워드 기반으로 뉴스 이벤트 타입 자동 분류:
- `ma`: 인수합병
- `fda`: FDA 승인/시험 관련
- `lawsuit`: 소송
- `earnings`: 실적 발표
- `guidance`: 실적 전망
- `partnership`: 파트너십
- `regulatory`: 규제 관련

### 5. Discord 알림
- **일반 알림**: 뉴스 갱신 시 기본 알림 발송
- **강한 호재/악재 알림**: 기준을 통과한 뉴스만 선별하여 상세 정보와 함께 알림

### 6. 회로 차단기 (Circuit Breaker)
- Google API 할당량 초과 시 자동으로 스코어링 비활성화
- 다음 날 자정(PST)까지 자동 비활성화 유지
- Supabase의 `google_flag` 테이블을 통한 상태 관리

## 📊 데이터베이스 스키마

### `news` 테이블

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| `id` | UUID | 기본 키 |
| `title` | TEXT | 뉴스 제목 |
| `description` | TEXT | 뉴스 요약/설명 |
| `article_url` | TEXT | 원본 기사 URL (UNIQUE) |
| `keywords` | TEXT[] | 키워드 배열 |
| `published_utc` | TIMESTAMPTZ | 발행 시간 |
| `tickers` | TEXT[] | 관련 종목 티커 배열 |
| `insights` | JSONB | 원본 Insight 데이터 |
| `sentiment_score` | INTEGER | 감정 점수 (-100 ~ +100) |
| `sentiment_confidence_model` | FLOAT | 모델 신뢰도 (0 ~ 1) |
| `sentiment_confidence_rule` | FLOAT | 룰 기반 신뢰도 (0 ~ 1) |
| `sentiment_reasoning` | TEXT | 감정 분석 근거 |
| `sentiment_insights` | JSONB | Insight 단위 분석 결과 |
| `created_at` | TIMESTAMPTZ | 생성 시간 |

### `google_flag` 테이블 (회로 차단기용)

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| `key` | TEXT | 플래그 키 (예: "scoring") |
| `value` | JSONB | 플래그 값 (예: `{disabled_until: "2024-01-01T00:00:00Z"}`) |
| `updated_at` | TIMESTAMPTZ | 업데이트 시간 |

## 🔄 스케줄링

스케줄러를 실행하려면 cron 또는 다른 작업 스케줄러를 사용하세요:

```bash
# 매시간 실행 (crontab 예시)
0 * * * * cd /path/to/newsScheduler && node run.mjs
```

## 📈 동작 흐름

1. **뉴스 수집**: 최근 1시간 이내 발행된 뉴스 수집 (최대 300건)
2. **중복 확인**: Supabase에서 이미 존재하는 기사 URL 확인
3. **데이터 매핑**: API 응답을 데이터베이스 스키마에 맞게 변환
4. **AI 스코어링** (활성화된 경우):
   - Neutral Insight는 스킵
   - Positive/Negative Insight만 배치로 Gemini에 전송
   - 각 Insight에 대한 감정 점수 및 신뢰도 계산
   - 소스 및 이벤트 타입 기반 룰 보정
   - Insight 결과를 기사 단위로 롤업
5. **데이터베이스 저장**: Upsert를 통한 중복 방지 저장
6. **알림 발송**: 강한 호재/악재가 있는 경우 Discord 알림 발송
7. **데이터 정리**: 2일 이상 된 오래된 뉴스 삭제

## 🛡️ 에러 처리

- **API 할당량 초과**: 자동으로 스코어링 비활성화 및 다음 날까지 유지
- **일시적 오류**: Exponential backoff를 통한 자동 재시도 (최대 4회)
- **중복 데이터**: `article_url` 기준 Upsert로 자동 처리

## 📝 참고사항

- Neutral sentiment를 가진 Insight는 AI 모델 호출 없이 자동으로 0점 처리됩니다.
- 배치 스코어링으로 API 호출 비용을 최적화합니다.
- 소스 신뢰도와 이벤트 타입을 고려한 보정 알고리즘으로 정확도를 향상시킵니다.
- Discord 메시지는 2000자 제한을 고려하여 자동으로 분할 전송됩니다.

