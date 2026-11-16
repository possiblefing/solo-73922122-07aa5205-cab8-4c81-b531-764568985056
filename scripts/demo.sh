#!/usr/bin/env bash
# 端到端演示：发布 -> 幂等重试 -> 冲突拒绝 -> 取件 -> 两类证明 -> 离线核验 -> 篡改拒绝。
# 用法：BASE=http://127.0.0.1:8001 bash scripts/demo.sh
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8001}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/day1.csv" <<'CSV'
station,time,ph,do
ST01,2026-11-16T08:00,7.2,8.1
CSV
cat > "$WORK/day2.csv" <<'CSV'
station,time,ph,do
ST02,2026-11-16T08:00,7.0,8.4
CSV

echo "== 1. 首次发布 =="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day1.csv" | python3 -m json.tool

echo "== 2. 同单同输入重试（期望 reused=true）=="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day1.csv" | python3 -m json.tool

echo "== 3. 同单异输入（期望 HTTP 409）=="
curl -s -w "\nHTTP %{http_code}\n" -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day2.csv"

echo "== 4. 第二件发布 =="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST02 -F sample_date=2026-11-16 -F order_no=ORD-002 \
  -F file=@"$WORK/day2.csv" | python3 -m json.tool

echo "== 5. 下载并比对原件 =="
curl -s "$BASE/releases/ST01/2026-11-16/ORD-001/download" -o "$WORK/back.csv"
cmp "$WORK/day1.csv" "$WORK/back.csv" && echo "字节一致"

echo "== 6. 纳入证明与离线核验 =="
curl -s "$BASE/releases/ST01/2026-11-16/ORD-001/inclusion-proof" \
  > "$WORK/inclusion.json"
python3 -m app.verify inclusion \
  --proof "$WORK/inclusion.json" --file "$WORK/day1.csv"

echo "== 7. 追加证明（1 号 -> 最新）与离线核验 =="
curl -s "$BASE/checkpoints/1" > "$WORK/cp1.json"
curl -s "$BASE/checkpoints/latest" > "$WORK/cp-latest.json"
curl -s "$BASE/checkpoints/1/append-proof" > "$WORK/append.json"
python3 -m app.verify append \
  --proof "$WORK/append.json" \
  --old-checkpoint "$WORK/cp1.json" \
  --new-checkpoint "$WORK/cp-latest.json"

echo "== 8. 篡改原件（期望离线拒绝，退出码 1）=="
cp "$WORK/day1.csv" "$WORK/tampered.csv"
printf 'X' >> "$WORK/tampered.csv"
python3 -m app.verify inclusion \
  --proof "$WORK/inclusion.json" --file "$WORK/tampered.csv" || true
