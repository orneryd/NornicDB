#!/bin/bash
# NornicDB Build & Deploy Script
# Usage: ./build.sh [build|deploy] [arm64-metal|arm64-metal-bge|amd64-cuda|amd64-cuda-bge|all]

set -e

REGISTRY="${REGISTRY:-timothyswt}"
VERSION="${VERSION:-latest}"
DOCKER_DIR="docker"

# Detect architecture
UNAME_M=$(uname -m)
if [[ "$UNAME_M" == "arm64" ]] || [[ "$UNAME_M" == "aarch64" ]]; then
    HOST_ARCH="arm64"
else
    HOST_ARCH="amd64"
fi

# Images
declare -A IMAGES=(
    ["arm64-metal"]="${REGISTRY}/nornicdb-arm64-metal:${VERSION}"
    ["arm64-metal-bge"]="${REGISTRY}/nornicdb-arm64-metal-bge:${VERSION}"
    ["amd64-cuda"]="${REGISTRY}/nornicdb-amd64-cuda:${VERSION}"
    ["amd64-cuda-bge"]="${REGISTRY}/nornicdb-amd64-cuda-bge:${VERSION}"
)

declare -A DOCKERFILES=(
    ["arm64-metal"]="${DOCKER_DIR}/Dockerfile.arm64-metal"
    ["arm64-metal-bge"]="${DOCKER_DIR}/Dockerfile.arm64-metal"
    ["amd64-cuda"]="${DOCKER_DIR}/Dockerfile.amd64-cuda"
    ["amd64-cuda-bge"]="${DOCKER_DIR}/Dockerfile.amd64-cuda"
)

declare -A PLATFORMS=(
    ["arm64-metal"]="linux/arm64"
    ["arm64-metal-bge"]="linux/arm64"
    ["amd64-cuda"]="linux/amd64"
    ["amd64-cuda-bge"]="linux/amd64"
)

declare -A BUILD_ARGS=(
    ["arm64-metal-bge"]="--build-arg EMBED_MODEL=true"
    ["amd64-cuda-bge"]="--build-arg EMBED_MODEL=true"
)

build_image() {
    local arch=$1
    local image="${IMAGES[$arch]}"
    [[ -z "$image" ]] && { echo "Unknown: $arch"; exit 1; }
    
    local variant="BYOM"
    [[ "$arch" == *"-bge" ]] && variant="BGE"
    
    echo "Building: $image [$variant]"
    docker build --platform "${PLATFORMS[$arch]}" ${BUILD_ARGS[$arch]:-} \
        -t "$image" -f "${DOCKERFILES[$arch]}" .
    echo "✓ Built $image"
}

push_image() {
    local arch=$1
    local image="${IMAGES[$arch]}"
    [[ -z "$image" ]] && { echo "Unknown: $arch"; exit 1; }
    
    echo "→ Pushing $image"
    docker push "$image"
    echo "✓ Pushed $image"
}

deploy_image() {
    build_image "$1"
    push_image "$1"
}

case "${1:-help}" in
    build)
        case "${2:-}" in
            all)
                if [[ "$HOST_ARCH" == "arm64" ]]; then
                    build_image "arm64-metal"
                    build_image "arm64-metal-bge"
                else
                    build_image "amd64-cuda"
                    build_image "amd64-cuda-bge"
                fi
                ;;
            *)
                [[ -n "$2" ]] && build_image "$2" || echo "Usage: $0 build <arch|all>"
                ;;
        esac
        ;;
    deploy)
        case "${2:-}" in
            all)
                echo "Detected architecture: $HOST_ARCH"
                if [[ "$HOST_ARCH" == "arm64" ]]; then
                    deploy_image "arm64-metal"
                    deploy_image "arm64-metal-bge"
                else
                    deploy_image "amd64-cuda"
                    deploy_image "amd64-cuda-bge"
                fi
                echo "✓ All $HOST_ARCH images deployed"
                ;;
            *)
                [[ -n "$2" ]] && deploy_image "$2" || echo "Usage: $0 deploy <arch|all>"
                ;;
        esac
        ;;
    images)
        echo "Host: $HOST_ARCH"
        echo ""
        echo "ARM64 Metal:"
        echo "  ${IMAGES[arm64-metal]} [BYOM]"
        echo "  ${IMAGES[arm64-metal-bge]} [BGE]"
        echo ""
        echo "AMD64 CUDA:"
        echo "  ${IMAGES[amd64-cuda]} [BYOM]"
        echo "  ${IMAGES[amd64-cuda-bge]} [BGE]"
        ;;
    *)
        echo "NornicDB Build Script (detected: $HOST_ARCH)"
        echo ""
        echo "Usage: $0 [build|deploy] <arch|all>"
        echo ""
        echo "Architectures:"
        echo "  arm64-metal      ARM64 base (BYOM)"
        echo "  arm64-metal-bge  ARM64 with BGE model"
        echo "  amd64-cuda       AMD64 base (BYOM)"
        echo "  amd64-cuda-bge   AMD64 with BGE model"
        echo "  all              Both variants for $HOST_ARCH"
        echo ""
        echo "Examples:"
        echo "  $0 build arm64-metal       # Build base only"
        echo "  $0 deploy arm64-metal-bge  # Build + push with BGE"
        echo "  $0 deploy all              # Deploy both variants for $HOST_ARCH"
        echo ""
        echo "Config: REGISTRY=name VERSION=tag $0 ..."
        ;;
esac
