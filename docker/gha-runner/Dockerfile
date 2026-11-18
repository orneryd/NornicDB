FROM timothyswt/llama-cpu-libs:v0.7.0@sha256:ec78fcac54c5f6d8f4cb7db4d41cae2eb856d348579001a41328bde839d2f028 AS llama-libs

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

COPY --from=llama-libs /output/lib/libllama_combined.a /opt/nornic-llama/libllama_linux_amd64.a
COPY --from=llama-libs /output/include/ /opt/nornic-llama/include/
RUN printf '%s\n' 'sha256:ec78fcac54c5f6d8f4cb7db4d41cae2eb856d348579001a41328bde839d2f028' > /opt/nornic-llama/CPU_IMAGE_DIGEST

COPY entrypoint.sh /usr/local/bin/nornic-runner-entrypoint
RUN chmod +x /usr/local/bin/nornic-runner-entrypoint
VOLUME ["/runner"]
WORKDIR /runner
ENTRYPOINT ["/usr/local/bin/nornic-runner-entrypoint"]
