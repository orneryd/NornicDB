# Trusted GitHub Actions runner

This image runs one repository-scoped Linux Actions runner in Docker. It is
registered with the `nornic-trusted` label and no default labels, so workflows
must explicitly opt in. The CI and release-verification workflows send only
their Cypher test shards here; other shards continue on GitHub-hosted runners.

The runner is intended for trusted, same-repository code. CI dispatch still
requires the `admin-approved-actions` environment, and automatic release
verification accepts only same-repository `release/` heads. Do not expose this
runner to untrusted pull requests or mount the Docker socket into it.

Build from the repository root:

```sh
docker build -t nornic-gh-runner:2.338.2 docker/gha-runner
```

Generate a short-lived registration token through the GitHub repository's
Actions runner settings, then start a runner with a dedicated data volume:

```sh
docker volume create nornic-gh-runner-1-data
docker run -d --name nornic-gh-runner-1 --restart unless-stopped \
  --memory 10g --cpus 10 \
  -e GITHUB_URL=https://github.com/orneryd/NornicDB \
  -e RUNNER_NAME=buttocks-ci-1 \
  -e RUNNER_TOKEN="$REGISTRATION_TOKEN" \
  -v nornic-gh-runner-1-data:/runner \
  nornic-gh-runner:2.338.2
```

The registration token is used only on first registration. Remove the
`RUNNER_TOKEN` environment variable from the long-running container after
registration by recreating it with the same volume and other options. Keep
runner volumes separate if adding another runner. Measure memory and job
duration under a real CI shard before increasing concurrency.

The runner release tarball is pinned by SHA-256 in the Dockerfile. Update both
the version and hash when rebuilding for a newer runner release.
