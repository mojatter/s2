#!/bin/sh
set -eu

ENDPOINT="http://s2:9000"
ENDPOINT_MEM="http://s2-mem:9000"

aws_s3() {
  aws s3 --endpoint-url "$ENDPOINT" "$@"
}

aws_s3api() {
  aws s3api --endpoint-url "$ENDPOINT" "$@"
}

wait_for_server() {
  name="$1"
  ep="$2"
  echo "==> Waiting for $name..."
  i=0
  while [ "$i" -lt 30 ]; do
    if aws s3api --endpoint-url "$ep" list-buckets >/dev/null 2>&1; then
      echo "    $name is ready."
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "FAIL: $name did not become ready"
  exit 1
}

wait_for_server "s2 (osfs)" "$ENDPOINT"
wait_for_server "s2-mem" "$ENDPOINT_MEM"

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

# Non-directory-aligned S3 prefix matching (e.g. "im" matches "images/")
run_test "ListPartialPrefixMatchesSubdir" sh -c '
  EP="'"$ENDPOINT"'"
  echo -n "1" | aws s3 --endpoint-url "$EP" cp - s3://test-bucket/images/a.png
  echo -n "2" | aws s3 --endpoint-url "$EP" cp - s3://test-bucket/images/b.png
  # list-objects-v2 with prefix=im delimiter=/ should return CommonPrefix images/
  out=$(aws s3api --endpoint-url "$EP" list-objects-v2 \
    --bucket test-bucket --prefix im --delimiter / \
    --query "CommonPrefixes[].Prefix" --output text)
  [ "$out" = "images/" ]
'

run_test "ListDirAndPartialFilename" sh -c '
  EP="'"$ENDPOINT"'"
  # prefix=images/a delimiter=/ should return only images/a.png in Contents
  out=$(aws s3api --endpoint-url "$EP" list-objects-v2 \
    --bucket test-bucket --prefix images/a --delimiter / \
    --query "Contents[].Key" --output text)
  [ "$out" = "images/a.png" ]
'

# Metadata operations
run_test "PutObjectWithMetadata" sh -c '
  EP="'"$ENDPOINT"'"
  printf "meta body" > /tmp/meta.txt
  aws s3api --endpoint-url "$EP" put-object \
    --bucket test-bucket --key meta.txt --body /tmp/meta.txt \
    --metadata key1=val1,key2=val2
  out=$(aws s3api --endpoint-url "$EP" head-object --bucket test-bucket --key meta.txt --query "Metadata" --output json)
  echo "$out" | grep -q "val1"
  echo "$out" | grep -q "val2"
'

run_test "CopyObjectPreservesMetadata" sh -c '
  EP="'"$ENDPOINT"'"
  aws s3api --endpoint-url "$EP" copy-object \
    --bucket test-bucket --key meta-copy.txt \
    --copy-source test-bucket/meta.txt
  out=$(aws s3api --endpoint-url "$EP" head-object --bucket test-bucket --key meta-copy.txt --query "Metadata" --output json)
  echo "$out" | grep -q "val1"
  echo "$out" | grep -q "val2"
'

run_test "CopyObjectReplaceMetadata" sh -c '
  EP="'"$ENDPOINT"'"
  aws s3api --endpoint-url "$EP" copy-object \
    --bucket test-bucket --key meta-replaced.txt \
    --copy-source test-bucket/meta.txt \
    --metadata-directive REPLACE \
    --metadata newkey=newval
  out=$(aws s3api --endpoint-url "$EP" head-object --bucket test-bucket --key meta-replaced.txt --query "Metadata" --output json)
  echo "$out" | grep -q "newval"
  # Original metadata should NOT be present
  ! echo "$out" | grep -q "val1"
'

# Presigned URL (query-string SigV4)
run_test "PresignedGetObject" sh -c '
  EP="'"$ENDPOINT"'"
  echo -n "presigned body" | aws s3 --endpoint-url "$EP" cp - s3://test-bucket/presigned.txt
  url=$(aws s3 presign --endpoint-url "$EP" s3://test-bucket/presigned.txt --expires-in 300)
  out=$(curl -sS -f "$url")
  [ "$out" = "presigned body" ]
'

run_test "PresignedGetObject_TamperedSignatureRejected" sh -c '
  EP="'"$ENDPOINT"'"
  url=$(aws s3 presign --endpoint-url "$EP" s3://test-bucket/presigned.txt --expires-in 300)
  tampered=$(echo "$url" | sed "s/X-Amz-Signature=[0-9a-f]*/X-Amz-Signature=deadbeef/")
  status=$(curl -sS -o /dev/null -w "%{http_code}" "$tampered")
  [ "$status" = "403" ]
'

# === Memfs backend ===
# Verify that the memfs default upload cap (16 MiB) is enforced end-to-end,
# and that the streaming CompleteMultipartUpload assembly works on the
# in-memory backend.

run_test "Memfs_CreateBucket" sh -c '
  aws s3api --endpoint-url "'"$ENDPOINT_MEM"'" create-bucket --bucket mem-bucket
'

run_test "Memfs_SmallUploadSucceeds" sh -c '
  dd if=/dev/zero of=/tmp/small.bin bs=1024 count=1024 2>/dev/null
  aws s3api --endpoint-url "'"$ENDPOINT_MEM"'" put-object \
    --bucket mem-bucket --key small.bin --body /tmp/small.bin
'

run_test "Memfs_LargeUploadRejected" sh -c '
  dd if=/dev/zero of=/tmp/large.bin bs=1048576 count=17 2>/dev/null
  # Expect the put-object call to fail because 17 MiB > 16 MiB default cap.
  ! aws s3api --endpoint-url "'"$ENDPOINT_MEM"'" put-object \
    --bucket mem-bucket --key large.bin --body /tmp/large.bin
'

run_test "Memfs_MultipartUploadStreams" sh -c '
  EP="'"$ENDPOINT_MEM"'"
  set -e
  # 10 MiB stays under the 16 MiB memfs cap while still exceeding the AWS CLI
  # multipart_threshold (8 MiB), so this exercises the streaming
  # CompleteMultipartUpload assembly on the in-memory backend.
  dd if=/dev/urandom of=/tmp/mp.bin bs=1048576 count=10 2>/dev/null
  aws s3 --endpoint-url "$EP" cp /tmp/mp.bin s3://mem-bucket/mp.bin
  aws s3 --endpoint-url "$EP" cp s3://mem-bucket/mp.bin /tmp/mp.out
  src=$(sha256sum /tmp/mp.bin | cut -d" " -f1)
  dst=$(sha256sum /tmp/mp.out | cut -d" " -f1)
  [ "$src" = "$dst" ]
'

# Cleanup
run_test "DeleteBucket" sh -c '
  aws s3 --endpoint-url "'"$ENDPOINT"'" rm s3://test-bucket --recursive
  aws s3api --endpoint-url "'"$ENDPOINT"'" delete-bucket --bucket test-bucket
'

echo ""
echo "==> Results: $passed passed, $failed failed"
[ "$failed" -eq 0 ]
