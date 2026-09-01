# Multi-User Auth & IAM-Style Policies

S2 Server supports multiple principals, each with an optional AWS-IAM-shaped `Policy` that scopes what that principal may do. This sits alongside the legacy single `user`/`password` pair described in the main [README](../README.md#authentication) — both mechanisms can be used together.

## Config

Add a `users` array to the JSON config file. There is no environment variable equivalent; multi-user auth is config-file only.

```json
{
  "listen": ":9000",
  "type": "osfs",
  "root": "/var/lib/s2",
  "users": [
    {
      "access_key_id": "readonlykey",
      "secret_access_key": "readonlysecret",
      "policy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Sid": "ReadOnly",
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetObject"],
            "Resource": ["arn:aws:s3:::*"]
          }
        ]
      }
    }
  ]
}
```

Each entry:

| Field | Description |
|-------|-------------|
| `access_key_id` | SigV4 Access Key ID / Basic Auth username. `"*"` is reserved for the anonymous principal (see [below](#anonymous-public-read-access)) |
| `secret_access_key` | SigV4 Secret Access Key / Basic Auth password |
| `policy` | Optional. Omit for full access (see below) |

**Legacy `user`/`password` coexists with `users`.** `LookupUser` checks `users` first, then falls back to the top-level `user`/`password` pair (or the `S2_SERVER_USER`/`S2_SERVER_PASSWORD` env vars — see [README: Environment variables](../README.md#environment-variables) for the full precedence rules), synthesized as a full-access principal with no policy attached. You can keep a legacy full-access credential for scripts/CI while adding scoped `users` entries for everything else.

A principal with **no `policy` field** (or the legacy `user`/`password` pair) has full access — this matches S2's pre-multi-user behavior, so existing single-credential configs keep working unchanged. A principal with a `policy` that has **no matching statement** for a given action is denied by default (deny-by-default once a policy is attached).

**Policy changes require a server restart.** Config (including `users` and every `policy`) is loaded once at startup; there is no hot-reload or `SIGHUP` handling. Editing the config file or environment variables while the server is running has no effect until you restart it.

## Policy grammar

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "OptionalLabel",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::mybucket/*"
    }
  ]
}
```

- **`Version`** — optional; if set, must be exactly `"2012-10-17"`.
- **`Statement`** — one or more rules, evaluated in AWS order: an explicit `Deny` on any matching statement wins immediately, regardless of order or any matching `Allow`.
- **`Effect`** — exactly `"Allow"` or `"Deny"` (case-sensitive).
- **`Action`** / **`Resource`** — a single string or a JSON array of strings. Both accept a wildcard `*`, matched literally (no other glob syntax — no `?`, no regex).
- **`Condition`** — parsed but not evaluated; `Validate` rejects any statement that sets a non-empty one, so a policy that *looks* like it restricts by IP/time/etc. never silently grants more than it appears to.

**`Resource` must be `"*"` or start with `arn:aws:s3:::`.** A pattern without that prefix (e.g. `"mybucket/*"` instead of `"arn:aws:s3:::mybucket/*"`) is rejected at load time — before this check existed, such a typo silently made the statement match nothing, turning an intended `Deny` into a permanent no-op.

## Supported actions

| Action | Applies to |
|--------|-----------|
| `s3:ListAllMyBuckets` | `GET /` (ListBuckets) — see [caveat](#s3listallmybuckets-is-not-a-full-gate) below |
| `s3:CreateBucket` | `PUT /{bucket}` |
| `s3:DeleteBucket` | `DELETE /{bucket}` |
| `s3:ListBucket` | `HEAD /{bucket}`, `GET /{bucket}` (ListObjectsV2), and per-bucket filtering of `GET /` |
| `s3:GetBucketLocation` | `GET /{bucket}?location` |
| `s3:GetObject` | `GET`/`HEAD /{bucket}/{key}`, and the copy-source side of CopyObject |
| `s3:PutObject` | `PUT /{bucket}/{key}` (including CopyObject's destination, UploadPart, and the multipart upload lifecycle) |
| `s3:DeleteObject` | `DELETE /{bucket}/{key}`, batch `DeleteObjects`, and recursive folder delete in the Web Console |

`Resource` ARNs follow the standard S3 shape: `arn:aws:s3:::bucket` for bucket-level actions, `arn:aws:s3:::bucket/key` for object-level ones.

## Example: scoped read/write

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetObject"],
      "Resource": ["arn:aws:s3:::uploads", "arn:aws:s3:::uploads/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::uploads/incoming/*"
    },
    {
      "Effect": "Deny",
      "Action": "s3:DeleteObject",
      "Resource": "arn:aws:s3:::uploads/incoming/protected.txt"
    }
  ]
}
```

This grants read access to the whole `uploads` bucket, write/delete access scoped to `uploads/incoming/`, and carves out one file that can never be deleted regardless of the broader `Allow` above (an explicit `Deny` always wins).

## Anonymous public read access

A `users` entry whose `access_key_id` is exactly `"*"` is the anonymous principal. S3 API requests that carry neither an `Authorization` header nor presigned-URL query parameters are evaluated against this entry's `policy` instead of being rejected — the same `Allowed`/`Authorized` check used for every other principal.

```json
{
  "users": [
    {
      "access_key_id": "*",
      "policy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::public-assets/*"
          }
        ]
      }
    }
  ]
}
```

There is no separate "public bucket" flag — the presence of a `"*"` entry is itself the toggle, and `Resource` scoping controls which buckets/prefixes are public, exactly like any other principal's policy.

Constraints specific to this entry, enforced at config-load time:

- `secret_access_key` must be empty — anonymous requests never present a secret.
- `policy` is required (the usual "no `policy` = full access" convention does not apply to `"*"`, since that would turn one config line into an all-buckets, all-actions public grant).
- Every `Action` in the policy must be `s3:GetObject` or `s3:ListBucket`. Write/delete actions are not supported for the anonymous principal.
- The Web Console (Basic Auth) never consults this entry — anonymous browsing of the bucket sidebar is out of scope. Anonymous access only applies to the S3 API.

### Allowing anonymous directory listing

`s3:ListBucket` reveals every key, size, and timestamp under a bucket — a materially different exposure than `s3:GetObject`, which requires knowing a key in advance. It's opt-in per statement: omit it and the anonymous principal can only fetch objects whose key it already knows.

`s3:ListBucket` and `s3:GetObject` also check different `Resource` ARNs — a bucket-level ARN (no trailing `/*`) for `ListBucket`, an object-level ARN (`bucket/*`) for `GetObject` — so granting one does not implicitly grant the other, and a policy needs both `Resource` forms to allow both actions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::public-assets/*", "arn:aws:s3:::public-assets"]
    }
  ]
}
```

**Granting `s3:ListBucket` also exposes the bucket's name and creation date via unauthenticated `GET /`.** `GET /` (ListBuckets) is filtered per-bucket by the same `s3:ListBucket` check (see [below](#s3listallmybuckets-is-not-a-full-gate)), and this applies to the anonymous principal too: once any bucket is anonymously listable, anyone can discover it exists — without knowing its name in advance — by hitting the bucket-less root endpoint. If you want per-bucket listing without that root-level disclosure, add an explicit `Deny` on `s3:ListAllMyBuckets` (this is the one exception to the `s3:GetObject`/`s3:ListBucket`-only restriction above — it's permitted specifically so it can be denied):

```json
{
  "Effect": "Deny",
  "Action": "s3:ListAllMyBuckets",
  "Resource": "arn:aws:s3:::*"
}
```

## Known gaps and quirks

These are current, intentional limitations — not roadmap items to expect imminently. Worth knowing before porting an AWS-authored policy over.

### `s3:ListAllMyBuckets` is not a full gate

Real AWS S3 hard-gates `GET /` (ListBuckets) on an explicit `s3:ListAllMyBuckets` **Allow** — no Allow, no listing, full stop. S2 does not: `GET /` always returns whatever `s3:ListBucket` allows on a per-bucket basis (so a principal scoped to a handful of buckets isn't locked out of the endpoint entirely, closer to how MinIO's console filters bucket visibility). An explicit **Deny** on `s3:ListAllMyBuckets` is still honored and hard-blocks the S3 API's `GET /` — but **only** on the S3 API. The Web Console's `GET /` (the bucket sidebar) does not check for that Deny; it only applies the per-bucket `s3:ListBucket` filter. If you need to fully block bucket enumeration for a principal on both surfaces, deny `s3:ListBucket` on `arn:aws:s3:::*` instead — that's honored consistently everywhere.

### Recursive folder delete (Web Console) can stop partway through, silently

Deleting a folder recursively from the Web Console authorizes each descendant object individually. If a `Deny` applies to one of them, the sweep **stops there** — objects already deleted stay deleted, but nothing past that point is touched — and the response is still a plain success with no indication that the delete was partial. Automation driving the console's delete endpoint directly (rather than the S3 `DeleteObjects` API below) should not treat success as "folder fully cleared."

### Batch delete and recursive delete report partial failure differently

The S3 API's `DeleteObjects` (`POST /{bucket}?delete`) follows AWS's documented multi-status behavior: each denied key is skipped and reported individually in the response's error list, and the rest of the batch still proceeds. The Web Console's recursive folder delete does not do this — see above. These are two independent code paths and intentionally diverge; don't expect them to behave the same way under partial denial.

## See also

- [README: Authentication](../README.md#authentication) — the legacy single-credential setup this builds on.
- [`s2test/e2e/users-config.json`](../s2test/e2e/users-config.json) — a working example exercised by the end-to-end test suite.
