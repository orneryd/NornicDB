FROM ubuntu:24.04

ARG RUNNER_VERSION=2.338.2
ARG RUNNER_SHA256=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git jq unzip zip tar gzip xz-utils \
    make gcc g++ pkg-config cmake ninja-build \
    python3 python3-venv python3-pip \
    libicu-dev libssl-dev zlib1g-dev gosu sudo \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 1001 --create-home --shell /bin/bash runner \
    && mkdir -p /opt/actions-runner /runner \
    && curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz" -o /tmp/runner.tar.gz \
    && echo "${RUNNER_SHA256}  /tmp/runner.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/runner.tar.gz -C /opt/actions-runner \
    && rm /tmp/runner.tar.gz

COPY entrypoint.sh /usr/local/bin/nornic-runner-entrypoint
RUN chmod +x /usr/local/bin/nornic-runner-entrypoint
VOLUME ["/runner"]
WORKDIR /runner
ENTRYPOINT ["/usr/local/bin/nornic-runner-entrypoint"]
