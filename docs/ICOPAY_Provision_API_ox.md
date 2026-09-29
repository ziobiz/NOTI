# ox (OxPay Financial) — NOTI ↔ ICOPAY 계약

병칭 **ox** = OxPay Financial. 가맹·구매자 노출은 항상 ICOPAY 중립.

## 고정 입구 (슬롯 없음 · ElementPay와 동일 패턴)

| 용도 | URL |
|---|---|
| 서버 Webhook | `POST https://noti.icopay.net/noti/ox` (alias: `/noti/webhook/ox`) |
| 브라우저 Result | `GET\|POST https://noti.icopay.net/noti/result/ox` |

## Provision

```http
POST /api/v1/icopay/merchants/provision
Authorization: Bearer <NOTI_PROVISION_API_KEY>
```

```json
{
  "merchantId": "6000000050",
  "pgKind": "ox",
  "internalTargetId": "ONTL_HQ_THB",
  "callbackUrl": "https://merchant.example.com/webhook",
  "resultUrl": "https://merchant.example.com/pay-result",
  "options": {
    "enableRelay": true,
    "enableInternal": true,
    "relayFormat": "json",
    "resultDeliveryMode": "auto"
  }
}
```

응답에 `oxWebhookUrl` / `oxResultUrl` 포함. GET/PUT/DELETE: `?pgKind=ox`.

## Ingress 설정

`config/ox-ingress.example.json` → `config/ox-ingress.json`  
`icopayNotifyUrl` = ICOPAY `…/pg-notify/{token}/OX`

## 가맹 통보 스키마

`lib/oxNoti.js` → `mapOxToMerchantNotifyBody`  
JPAY/EP와 동일: `returncode` / `orderid` / `transaction_id` / `amount` / `PaymentStatus` …

## ICOPAY

- `PgVendor.OX`, ingress `/OX`
- Merchant Hosted Create Payment (카드 인라인 1회, HPP 금지)
- 노티생성 UI `pgKind=ox`
