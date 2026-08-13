# ElementPay Provision (NOTI)

See also: [NOTI_ElementPay_가맹통보_추가개발요청.md](./NOTI_ElementPay_가맹통보_추가개발요청.md), [ICOPAY_Provision_API_JPAY_v0.1.md](./ICOPAY_Provision_API_JPAY_v0.1.md).

## Endpoint

Same as JPAY provision, with `pgKind: "elementpay"`.

```http
POST /api/v1/icopay/merchants/provision
Authorization: Bearer <NOTI_PROVISION_API_KEY>
Content-Type: application/json
```

```json
{
  "merchantId": "6000000050",
  "pgKind": "elementpay",
  "internalTargetId": "ONTL_HQ_THB",
  "callbackUrl": "https://merchant.example.com/webhook",
  "resultUrl": "https://merchant.example.com/pay-result",
  "options": {
    "enableRelay": true,
    "enableInternal": true,
    "relayFormat": "json",
    "resultDeliveryMode": "auto"
  },
  "icopayMeta": {
    "compId": "6000000050",
    "compName": "Sample Merchant"
  }
}
```

- **No** `jpaySlotNo` / `pgCallbackUrl` / `icopayJpayNotifyUrl` (EP fixed webhook is `POST /noti/elementpay`).
- GET/PUT/DELETE: `?pgKind=elementpay`.

## Ingress config

Copy `config/elementpay-ingress.example.json` → `config/elementpay-ingress.json` and set `icopayNotifyUrl` to ICOPAY `…/pg-notify/{token}/ELEMENTPAY`.

ICOPAY should return `X-Icopay-Comp-Id` on check/pay responses so NOTI can relay merchant Callback.
