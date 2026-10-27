# CRYPTO — ICOPAY → NOTI → 가맹

계약 원본(동기화): ICOPAY 저장소 `docs/ICOPAY_CRYPTO_NOTI_계약.md`

| 입구 | 용도 |
|------|------|
| `POST /noti/crypto` | ICOPAY 입금 확정 미러 → 가맹 Callback |
| `GET\|POST /noti/result/crypto` | 브라우저 Result 릴레이 |

가맹점 **신규등록 → CRYPTO** 로 `merchantPgKind=crypto` 등록을 권장합니다. Comp-Id로 기존 JPAY/EP/ox 가맹 `callbackUrl`에도 매칭됩니다. 슬롯 없음. 노티거래내역·로그허브에 **CRYPTO** 탭이 있습니다.
