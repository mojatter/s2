# Upgrading s2-server

This guide is for s2-server operators. It collects, version by version, what an upgrade changes on disk, what to do before the new version starts, and whether you can go back. Library users and Storage implementers keep reading the Upgrading section of each [release](https://github.com/mojatter/s2/releases) and the godoc.

Release notes are not edited after the fact, so a later release sometimes corrects an earlier one. This file is the corrected record: where it and an old release note disagree, this file wins. It covers v0.14.0 and later.

## Every upgrade

- Take a backup of the storage root, with the server stopped, before any version whose section says the data at rest changes. On `osfs` copy `$S2_SERVER_ROOT` with modification times kept (`cp -a` or `rsync -a`): they are the objects' LastModified and the multipart generations in `.buckets/` (`.meta/` before v0.18.2). On a cloud backend, use the provider's own copy or versioning.
- Steps marked **while stopped** must run with no s2-server process on the root, and before the new version starts for the first time.
- Multipart uploads in progress across an upgrade may have to be started again; each section says when.

## Jumping several versions

The steps below take a root from any version since v0.14.0 to the current one in one go. Skip a step whose condition does not hold. Order matters: the rename in step 2 must happen before any version at or after v0.18.2 starts for the first time.

1. **While stopped**, if you come from before v0.17.0: remove the multipart parts a pre-v0.16.0 server left behind, `for b in "${S2_SERVER_ROOT:?}"/*/; do rm -rf "${b}__s2mp__" "${b}.meta/__s2mp__"; done` on `osfs`, or delete `<bucket>/__s2mp__/` under the configured root with the provider's own tool on a cloud backend. Uploads in progress are lost either way.
2. **While stopped**, if `$S2_SERVER_ROOT/.meta` is a directory and `$S2_SERVER_ROOT/.buckets` does not exist yet (a root v0.17.0, v0.18.0 or v0.18.1 served): `mv "${S2_SERVER_ROOT:?}/.meta" "${S2_SERVER_ROOT:?}/.buckets"`. See [v0.18.2](#v0182) for why the order matters.
3. If you use policies: from before v0.16.0, grant `s3:ListBucketMultipartUploads` and `s3:ListMultipartUploadParts` where `s3:ListBucket` used to cover the multipart listings; from before v0.18.1, console folder creation also needs `s3:PutObject` on `<bucket>/<key>/.keep`; from before v0.20.1, `PUT /{bucket}/` and `DELETE /{bucket}/` need `s3:CreateBucket` and `s3:DeleteBucket`.
4. Start the new version.
5. Once the new version has run, do not roll back below v0.20.0. Restore the backup instead.
6. The steps above are only what must happen before the new version starts; read each version section you crossed for what clients see differently.

## Versions

Each section lists whether the data at rest changes, what to do, and, where known, what a downgrade does. A version not listed here changes nothing on disk and needs no step; what its responses change is in its release notes.

### v0.20.2

- **Data at rest:** on `osfs` and `memfs`, `Delete`, the source side of a move and a recursive delete remove the directories they leave empty, up to the bucket. An empty directory made outside s2 goes away when a delete runs under it. Nothing else changes; downgrading to v0.20.1 is safe.
- A write and a delete running at the same time under the same prefix can make the write fail, because the delete may remove the directory the write is using. Retry the write. [#317](https://github.com/mojatter/s2/issues/317) tracks the fix.
- On `osfs` and `memfs`, with `a.txt` stored, GET and HEAD of `a.txt/sub` answer `404` and DELETE `204` instead of `500`. PUT or copy to `a.txt/sub`, or to a key that is a directory, including an empty one an older version left behind, answers `400 InvalidArgument` instead of `500`.
- ListBuckets and the console's bucket list no longer stop at the first page of buckets. The console's folder view and search are still bounded and now say so when there may be more.

### v0.20.1

- **Data at rest:** no change. Downgrading to v0.20.0 is safe and brings the old S3 API behaviour back.
- DeleteBucket through the S3 API answers `409 BucketNotEmpty` while the bucket holds objects. Empty it first (`aws s3 rm s3://b --recursive`, or `aws s3 rb --force`), or delete it from the console, which still removes the contents.
- UploadPartCopy answers `501 NotImplemented`, so server-side copies above the client's multipart threshold (8 MB for the AWS CLI) fail. Raise the threshold or copy through a download and upload until [#307](https://github.com/mojatter/s2/issues/307) lands.
- Subresource requests s2 does not implement (tagging, ACL, policy, versioning, lifecycle, ...) and `DELETE` with `?versionId` answer `501 NotImplemented` instead of a success that lost data.
- `PUT /{bucket}/` and `DELETE /{bucket}/` are authorized as `s3:CreateBucket` and `s3:DeleteBucket`, like the form without the slash.

### v0.20.0

- **Data at rest:** on `osfs`, the metadata file of a nested key moves from `<bucket>/.meta/<dir>/<name>` to `<bucket>/<dir>/.meta/<name>`. Nothing is migrated at startup: old files keep reading, and each object moves the next time it is written. That fallback stays until v1.0.0, and a migration command ships before then ([#301](https://github.com/mojatter/s2/issues/301)).
- **Take a backup first. Downgrading to v0.19.x is not safe** once v0.20.0 has run: v0.19.x does not look at the new location, so nested objects v0.20.0 wrote lose their Content-Type and metadata, and their ETag changes form, without any error. Restore the backup instead of rolling back.
- A regular file named `.meta` inside a folder, a key a pre-v0.18.1 server could store, blocks writes to the objects beside it with an error naming the file; reads still work. Delete the file from the root directory. A key stored as `<dir>/.meta/<name>` is now read as the metadata file of `<dir>/<name>`; if it does not parse as one, reading `<dir>/<name>` fails with an error naming it, so fix or delete it on disk.

### v0.18.2

- **Data at rest:** the per-bucket state directory moves from `$S2_SERVER_ROOT/.meta/` to `$S2_SERVER_ROOT/.buckets/`. Only an `osfs` root that v0.17.0, v0.18.0 or v0.18.1 served has the old one.
- **While stopped**, before v0.18.2 or later starts for the first time and only while `.buckets/` does not exist: `mv "${S2_SERVER_ROOT:?}/.meta" "${S2_SERVER_ROOT:?}/.buckets"`. `mv` keeps modification times, which is what the markers record, so multipart uploads in flight across the upgrade still complete. Once `.buckets/` exists, do not run it: `mv` then fails or moves `.meta` inside `.buckets/`, and the old markers do nothing either way.
- Without the rename, each bucket gets a fresh generation on its first multipart request, and uploads started before then can neither complete nor be aborted. The expiry sweep frees their parts within `S2_SERVER_MULTIPART_MAX_AGE` (24 hours by default); with a negative value, remove those uploads' directories under `<root>/.multipart/` by hand. `rm -rf "${S2_SERVER_ROOT:?}/.meta"` then removes the inert old markers and nothing else.
- Downgrading to v0.18.0 or v0.18.1 after the rename: move `.buckets/` back to `.meta/` while stopped, or those versions read no marker, start fresh generations, and strand the uploads in flight in the same way.

### v0.18.1

- **Data at rest:** no change. Upgrade: this release closes a path-escape exposure reachable by authenticated callers, present in every earlier version on every backend, including the published binary and image ([GHSA-h5xj-g6fx-mjwc](https://github.com/mojatter/s2/security/advisories/GHSA-h5xj-g6fx-mjwc)).
- Request paths with `.`, `..` or empty elements, and `PUT` or `DELETE` on a key ending in `/`, answer `400 InvalidArgument` instead of being normalized.
- A key under `.meta` that a pre-v0.18.1 server stored can no longer be read or listed. Remove it by deleting the enclosing folder or bucket, or from the root directory on `osfs`.
- Creating a console folder needs `s3:PutObject` on both `<bucket>/<key>` and `<bucket>/<key>/.keep`.

### v0.18.0

- **Data at rest:** on `osfs`, metadata files are written as `{"etag","content_type","metadata"}`. The older flat form keeps reading until v1.0.0; nothing to migrate. On `s3` and `gcs`, the Content-Type moves from the `s2-content-type` metadata key to the provider's own attribute; objects written before keep reading.
- **Take a backup first. Downgrading below v0.18.0 is not safe** once v0.18.0 has run: on `osfs`, v0.17.0 and earlier fail to decode the new file, so every object written since answers an error; on `s3` and `gcs` they answer a guessed Content-Type for those objects instead of the stored one. Restore the backup instead of rolling back.
- Every object's ETag on an `osfs` root changes, since ListObjects used to answer the MD5 of the empty string for all of them. Clients that compare ETags re-verify or re-transfer once.
- An object stored without a Content-Type gets one guessed from its key's extension at read time, on `osfs` and `gcs` roots. This supersedes the v0.14.0 and v0.15.0 notes about Content-Type.
- The multipart ETag is the MD5 of the whole body; see [Known differences from S3](#known-differences-from-s3).
- Multipart uploads in progress across the upgrade may fail to complete; start them again.

### v0.17.0

- **Data at rest:** `$S2_SERVER_ROOT/.meta/` appears, holding one empty file per bucket. v0.18.2 renames it to `.buckets/`.
- Multipart parts a pre-v0.16 server left under `<bucket>/__s2mp__/` become visible in listings. Remove them as in [step 1](#jumping-several-versions) above, or from the console by deleting the `__s2mp__` folder.
- Multipart uploads in progress at upgrade time answer `NoSuchUpload`; start them again.

### v0.16.0

- **Data at rest:** multipart uploads in progress live under `$S2_SERVER_ROOT/.multipart/` instead of `<bucket>/__s2mp__/`. Uploads in progress at upgrade time answer `NoSuchUpload`; start them again, and remove the old parts as in [step 1](#jumping-several-versions) above.
- Uploads older than `S2_SERVER_MULTIPART_MAX_AGE` (24 hours by default) are refused and their parts removed. A negative value keeps them forever.
- Policies: `s3:ListBucket` no longer covers the multipart listings. Grant `s3:ListBucketMultipartUploads` for ListMultipartUploads and `s3:ListMultipartUploadParts` for ListParts; the anonymous user cannot have either.
- ListBuckets and the console skip top-level entries whose name starts with `.`.

### v0.15.0

- **Data at rest:** no change.
- `NextContinuationToken` became an opaque token. On a cloud backend, a client paginating across the upgrade sends a stale token and gets `500` until it restarts its listing. One-time.

### v0.14.0

- **Data at rest:** PutObject starts storing the request's Content-Type as the `s2-content-type` metadata key; nothing to migrate.
- Anonymous read access is new and opt-in through a `"*"` users entry. A `users` entry that already had `access_key_id: "*"` no longer loads unless it has no secret and a read-only policy; see [docs/users-policy.md](users-policy.md#anonymous-public-read-access).

## Known differences from S3

- **Multipart ETag:** the ETag of an object assembled from parts is the MD5 of the whole body, not the `md5-of-md5s-N` form. rclone configured with `provider = AWS` reports "Etag differ"; use `provider = Other` or `--s3-use-multipart-etag=false`.
- **`osfs` and `memfs` store a key at its path:** `a` and `a/b` cannot both exist. [docs/backends.md](backends.md#object-names) has the details.
- [README.md#limitations](../README.md#limitations) lists the S3 features s2 does not implement.
