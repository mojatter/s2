#!/bin/sh
set -eu

ENDPOINT="http://s2:9000/s3api"

aws_s3() {
  aws s3 --endpoint-url "$ENDPOINT" "$@"
}

aws_s3api() {
  aws s3api --endpoint-url "$ENDPOINT" "$@"
}

# Wait for s2-server to be ready
echo "==> Waiting for s2-server..."
i=0
while [ "$i" -lt 30 ]; do
  if aws_s3api list-buckets >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
if [ "$i" -eq 30 ]; then
  echo "FAIL: s2-server did not become ready"
  exit 1
fi
echo "    s2-server is ready."

passed=0
failed=0

run_test() {
  name="$1"
  shift
  printf "  TEST %s ... " "$name"
  if "$@" >/dev/null 2>&1; then
    echo "PASS"
    passed=$((passed + 1))
  else
    echo "FAIL"
    failed=$((failed + 1))
  fi
}

echo "==> Running E2E tests..."

# Bucket operations
run_test "CreateBucket" aws_s3api create-bucket --bucket test-bucket

run_test "ListBuckets" sh -c '
  out=$(aws s3api --endpoint-url "'"$ENDPOINT"'" list-buckets --query "Buckets[].Name" --output text)
  echo "$out" | grep -q "test-bucket"
'

run_test "HeadBucket" aws_s3api head-bucket --bucket test-bucket

# Object operations
run_test "PutObject" sh -c 'echo -n "hello e2e" | aws s3 --endpoint-url "'"$ENDPOINT"'" cp - s3://test-bucket/hello.txt'

run_test "GetObject" sh -c '
  out=$(aws s3 --endpoint-url "'"$ENDPOINT"'" cp s3://test-bucket/hello.txt -)
  [ "$out" = "hello e2e" ]
'

run_test "HeadObject" aws_s3api head-object --bucket test-bucket --key hello.txt

run_test "CopyObject" aws_s3api copy-object \
  --bucket test-bucket \
  --key copied.txt \
  --copy-source test-bucket/hello.txt

run_test "GetCopiedObject" sh -c '
  out=$(aws s3 --endpoint-url "'"$ENDPOINT"'" cp s3://test-bucket/copied.txt -)
  [ "$out" = "hello e2e" ]
'

# List objects
run_test "ListObjects" sh -c '
  out=$(aws s3api --endpoint-url "'"$ENDPOINT"'" list-objects-v2 --bucket test-bucket --query "Contents[].Key" --output text)
  echo "$out" | grep -q "hello.txt"
  echo "$out" | grep -q "copied.txt"
'

# Delete operations
run_test "DeleteObject" aws_s3api delete-object --bucket test-bucket --key copied.txt

run_test "DeleteObjects" aws_s3api delete-objects --bucket test-bucket \
  --delete '{"Objects":[{"Key":"hello.txt"}],"Quiet":true}'

# Multipart upload
run_test "MultipartUpload" sh -c '
  EP="'"$ENDPOINT"'"

  upload_id=$(aws s3api --endpoint-url "$EP" create-multipart-upload \
    --bucket test-bucket --key large.bin --query "UploadId" --output text)

  printf "part1data" > /tmp/part1.bin
  printf "part2data" > /tmp/part2.bin

  etag1=$(aws s3api --endpoint-url "$EP" upload-part \
    --bucket test-bucket --key large.bin --upload-id "$upload_id" \
    --part-number 1 --body /tmp/part1.bin --query "ETag" --output text)

  etag2=$(aws s3api --endpoint-url "$EP" upload-part \
    --bucket test-bucket --key large.bin --upload-id "$upload_id" \
    --part-number 2 --body /tmp/part2.bin --query "ETag" --output text)

  aws s3api --endpoint-url "$EP" complete-multipart-upload \
    --bucket test-bucket --key large.bin --upload-id "$upload_id" \
    --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$etag1},{\"PartNumber\":2,\"ETag\":$etag2}]}"

  out=$(aws s3 --endpoint-url "$EP" cp s3://test-bucket/large.bin -)
  [ "$out" = "part1datapart2data" ]
'

# Listing nonexistent / trailing-slash prefixes (regression for ListAfter bug)
run_test "ListNonexistentPrefix" sh -c '
  EP="'"$ENDPOINT"'"
  out=$(aws s3 --endpoint-url "$EP" ls s3://test-bucket/no-such-dir/ 2>&1)
  # Should succeed and produce no output
  [ -z "$out" ]
'

run_test "ListPrefixWithTrailingSlash" sh -c '
  EP="'"$ENDPOINT"'"
  echo -n "x" | aws s3 --endpoint-url "$EP" cp - s3://test-bucket/sub/a.txt
  echo -n "y" | aws s3 --endpoint-url "$EP" cp - s3://test-bucket/sub/b.txt
  out=$(aws s3 --endpoint-url "$EP" ls s3://test-bucket/sub/)
  echo "$out" | grep -q "a.txt"
  echo "$out" | grep -q "b.txt"
'

# Cleanup
run_test "DeleteBucket" sh -c '
  aws s3 --endpoint-url "'"$ENDPOINT"'" rm s3://test-bucket --recursive
  aws s3api --endpoint-url "'"$ENDPOINT"'" delete-bucket --bucket test-bucket
'

echo ""
echo "==> Results: $passed passed, $failed failed"
[ "$failed" -eq 0 ]
