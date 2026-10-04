# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Ngọc Minh / 2A202602653
**Repo:** https://github.com/NgNMinh/K4-Track02-Day17-NguyenNgocMinh-2A202602653-DataPipelineEngineering
**Commit bài nộp:** b6cb79c
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):**
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver (upsert) | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `test_silver_tickets_one_row_per_ticket` fail: `n=32, n_ids=12` — nhiều hàng cho cùng `ticket_id`; T-91 có 3 hàng với 3 trạng thái khác nhau. | `test_rerun_old_day_three_times_keeps_gold_checksum` fail: `gold_feature_daily` checksum thay đổi sau re-run ngày 12 (`c50b8851` → `21d1035e`); u05/2026-08-12 từ 2 events thành 5. | `test_cdc_delete_becomes_tombstone` fail: T-97 vẫn có `is_deleted=False` và PII chưa được xoá dù batch ngày 15 gửi op='d'. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT INTO` thuần túy — mỗi batch append thêm hàng mới, không ghi đè. Không có MERGE theo khoá `ticket_id`. | `LOOKBACK_DAYS=0` → mỗi run chỉ recompute partition của chính ngày đó. Events `event_time=2026-08-12` đến trễ (batch ngày 13–15) không được recompute vào partition ngày 12 khi re-run. P99 lateness thực tế = 3 ngày. | `ticket_changes_sql` lấy `ticket_id` từ `j->'value'->'after'->>'ticket_id'`. Khi op='d', Debezium set `after=null`, nên `ticket_id=NULL` → bị filter bởi `WHERE ticket_id IS NOT NULL`. Delete event bị bỏ qua hoàn toàn. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py` — thay `INSERT INTO silver_tickets` bằng `MERGE INTO silver_tickets ON ticket_id` với điều kiện `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`. LSN đảm bảo batch cũ không ghi đè trạng thái mới hơn. | `pipeline/config.py` — đặt `LOOKBACK_DAYS = 3` (= `ceil(p99_lateness)` đo từ Bronze). `build_feature_daily` đã dùng giá trị này để xoá và recompute đủ các partition. | `pipeline/staging.py` — thay `j->'value'->'after'->>'ticket_id'` bằng `COALESCE(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id')`. |
| **Khái niệm trên slide** | Upsert / MERGE theo khoá tự nhiên — "ghi theo khoá giữa nhiều batch" vs "dedup trong một batch". | Late data & lookback window — partition overwrite phải bao đủ độ trễ P99. | CDC delete → tombstone — `after=null` là ký hiệu xoá của Debezium; cần đọc `before` để giữ khoá. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3` ngày (p50=0.00, p95=2.90, p99=3.00, max=3, n=43 events)
  → `LOOKBACK_DAYS = ceil(3.0) = 3`
  → Ví dụ cụ thể: u05 có 3 events với `event_time=2026-08-12 21:xx` nhưng `_batch_id=2026-08-15` (trễ 3 ngày). Nếu `LOOKBACK_DAYS=0`, re-run ngày 12 xoá partition 2026-08-12 rồi insert lại chỉ thấy 2 events (landed batch 12), bỏ mất 3 events trễ → checksum thay đổi.
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE đảm bảo mỗi `ticket_id` chỉ có một hàng hiện tại và điều kiện `s._lsn > t._lsn` ngăn batch cũ ghi đè trạng thái mới; overwrite-partition cho feature daily vì events là fact bất biến — ta chỉ cần recompute lại cửa sổ đủ rộng để hấp thụ late data.
- Tombstone thay vì xoá hẳn hàng trong Silver: Silver là lớp "sự thật duy nhất" — xoá hẳn sẽ làm mất lịch sử cho Gold training set và RAG; tombstone giữ khoá với `is_deleted=True` để tầng Gold có thể lọc ra mà vẫn tái tạo được từ Bronze.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: bất biến — classifier được train trên snapshot `v2026-08-12` phải nhất quán mãi mãi dù pipeline chạy lại; `SnapshotImmutableError` bảo vệ tính toàn vẹn này.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu đến vài chục nghìn hàng/ngày — DuckDB xử lý tốt trên một máy, không cần overhead của cluster Spark; dbt cung cấp lineage và test tích hợp sẵn.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?

   Hai nguyên tắc không thể cùng thoả mãn 100%: snapshot bất biến là nền tảng của reproducibility ML, nhưng GDPR "right to erasure" là nghĩa vụ pháp lý. Cách xử lý thực tế: tách riêng lưu trữ PII ra một kho mã hoá riêng (key-per-subject encryption); khi xoá, chỉ cần huỷ khoá của subject đó — snapshot cũ vẫn "bất biến về cấu trúc" nhưng PII trở thành cipher text không giải được. Với dữ liệu không mã hoá sẵn như bài này, cần có quy trình "crypto-erase": backfill lại tất cả snapshot chứa subject đó bằng giá trị null/masked, ghi log audit trail, và chấp nhận rằng một số snapshot cũ sẽ thay đổi checksum — đây là chi phí cần trả để tuân thủ pháp lý.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

   Regex chỉ bắt được PII có cấu trúc cố định. Tên người là "semi-structured" — cần NER (Named Entity Recognition) ở tầng Bronze-to-Silver, không phải Silver-to-Gold, vì càng sớm càng an toàn. Thực tế: (1) dùng một NER model nhẹ (underthesea cho tiếng Việt) để tag PERSON entities rồi replace bằng `<NAME>`; (2) thêm blocklist tên phổ biến kết hợp với context (trước/sau từ như "tôi là", "họ tên"); (3) đo bằng một bộ test PII riêng — seed thêm một số ticket chứa tên biết trước, sau đó assert không còn tên gốc trong Silver. Acceptance criterion: 0 tên rò ra trong `silver_tickets.body` và `silver_transcripts.text` trên test set.

## 5. Output (dán nguyên văn)

```text
$ .venv/Scripts/python.exe -m scripts.verify

=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

```text
$ .venv/Scripts/python.exe -m pytest

..................................                                          [100%]
34 passed in 2.58s
```

```text
$ .venv/Scripts/python.exe -m scripts.rerun_check

# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
$ .venv/Scripts/python.exe main.py --lateness

event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
$ .venv/Scripts/python.exe -m scripts.parity

=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
