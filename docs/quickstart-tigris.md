# Quickstart — Tigris

Run Elide against a real S3-compatible backend. Uses [Tigris](https://www.tigrisdata.com) as the object store; the same config shape works for AWS S3, MinIO, R2, and other S3-compatible services.

This guide uses **static credentials** shared between coordinator and volume — the volume subprocess inherits the coordinator's AWS env vars. Per-volume scoped credentials via macaroons are a separate feature (see *Proposed: S3 credential distribution via macaroons* in [architecture.md](architecture.md)).

## Prerequisites

- Rust toolchain (`cargo`) and binaries built per [quickstart.md](quickstart.md).
- A Tigris account ([console.storage.dev](https://console.storage.dev)) with:
  - A bucket (e.g. `elide-test`)
  - An access key pair — **Admin** scope on the bucket is sufficient for a single-credentials test. For the eventual volume/coordinator split, mint a separate **Read-only** key pair for the volume.
- Optional: `aws` CLI (`brew install awscli`) for verifying uploads out-of-band.

## Configure the coordinator

Create `coordinator.toml` in the directory you'll run from:

```toml
data_dir = "elide_data"

[store]
bucket   = "elide-test"
endpoint = "https://t3.storage.dev"
region   = "auto"
```

Export credentials in the shell that will run the coordinator:

```sh
export AWS_ACCESS_KEY_ID=tid_...
export AWS_SECRET_ACCESS_KEY=tsec_...
```

`object_store`'s `AmazonS3Builder::from_env()` picks these up. No other env vars are required — `endpoint` and `region` come from `coordinator.toml`.

## Volume fetcher — automatic from `coordinator.toml`

Volume processes are spawned by the coordinator and inherit:

- the access keys (from the coordinator's own `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env)
- the store settings (`bucket`, `endpoint`, `region`), which the coordinator exports to each subprocess as `ELIDE_S3_BUCKET`, `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION` based on `coordinator.toml`.

So no per-volume `fetch.toml` is required for a uniform store. If you do want to override per-volume (e.g. pointing one volume at a different bucket), drop a `fetch.toml` into that volume directory — it takes precedence over the inherited env:

```toml
bucket   = "elide-test-other"
endpoint = "https://t3.storage.dev"
region   = "auto"
```

## Start the coordinator

```sh
./target/debug/elide-coordinator serve
```

The startup log should show the configured bucket and endpoint. If it instead shows `local_store elide_store/`, the `[store]` section didn't parse — double-check the `coordinator.toml` path and syntax.

## Import, branch a replica, write, verify

In a second terminal (same repo root):

```sh
./target/debug/elide volume import start ubuntu-22.04 ubuntu:22.04
./target/debug/elide volume import attach <import-ulid>
./target/debug/elide volume create vm1 --from ubuntu-22.04
```

Within a few seconds of each import segment landing in `pending/`, the coordinator's drain loop should upload it to Tigris. Verify:

```sh
aws --endpoint-url https://t3.storage.dev s3 ls \
    s3://elide-test/by_id/ --recursive | head
```

You should see `by_id/<volume-ulid>/segments/<YYYYMMDD>/<segment-ulid>` objects, plus `volume.pub`, `volume.provenance`, and `names/<name>` entries.

## Test demand-fetch

Force the volume to go back to Tigris for a read:

```sh
./target/debug/elide volume evict vm1
./target/debug/elide volume ls vm1 /etc
```

`evict` removes `cache/*.body` for S3-confirmed segments. The next `ls` triggers demand-fetch via the `S3RangeFetcher` — range-GET requests against Tigris re-hydrate just the extents needed, not whole segments.

Watch the volume log: lines tagged `demand-fetch` confirm the round-trip to Tigris. If you see `NotFound`, the upload step above didn't complete — re-run `aws s3 ls` to check.

## Troubleshooting

**`InvalidAccessKeyId` or `SignatureDoesNotMatch` on PUT** — The coordinator's env-var credentials are wrong or not exported into its shell. Tigris keys always start with `tid_` / `tsec_`; a leading `AKIA` is an AWS key in the wrong env.

**PUT succeeds but fetch 403s** — Likely region mismatch. Tigris uses `auto`; if the volume's `AWS_DEFAULT_REGION` falls back to `us-east-1`, signing fails on fetch. Set `AWS_DEFAULT_REGION=auto` in the coordinator's shell *before* starting it so the volume subprocess inherits it.

**`endpoint did not resolve`** — The endpoint URL needs the `https://` scheme and no trailing slash. `https://t3.storage.dev` is the canonical Tigris endpoint.

**Bucket not found on first upload** — Create the bucket through the Tigris console before the first drain tick. Elide does not create buckets automatically.
