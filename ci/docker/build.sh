#!/bin/bash
set -e

# Parameters:
# $1: context root
#     - The path to a parent of a directory containing a Dockerfile
# $2: image name
#     - The base image name matching a directory name
# $3: account
#     - The dockerhub account to upload to
# $4: image repository uri 
#     - repository URI, such as "docker.io"
# $5: build args (optional)
#     - extra build args
SCRIPT="$0"
CONTEXT_ROOT="$1"
IMAGE_NAME="$2"
ACCOUNT="$3"
REPO="$4"
BUILD_ARGS="$5"

CONTEXT_PATH="$CONTEXT_ROOT/$IMAGE_NAME"
ACCOUNT_DEFAULT=ankona
VERSION_DEFAULT=$(git rev-parse --short HEAD)
REPO_DEFAULT="docker.io"
LOG_FORMAT="[%s] %s %s\n"

log_info() {
    printf "$LOG_FORMAT" "INFO" "$SCRIPT" "$1"
}

log_error() {
    printf "$LOG_FORMAT" "ERROR" "$SCRIPT" "$1" >&2
}

show_usage() {
    printf "Usage: %s <image_name> <context-path> [<account>] [<version>] [<repo-uri>]\n" $SCRIPT
    printf " - default account: %s\n" $ACCOUNT_DEFAULT
    printf " - default version: %s\n" $VERSION_DEFAULT
    printf " - default repo: %s\n" $REPO_DEFAULT
}

if [ $# -lt 2 ]; then
    show_usage
    exit 1
fi

if [ ! -e "$CONTEXT_PATH" ]; then
    log_error "Directory was not found at the supplied context path: $CONTEXT_PATH"
    show_usage
    exit 1
fi

if [ -z "$ACCOUNT" ]; then
    ACCOUNT=$ACCOUNT_DEFAULT
    log_info "Using default account: $ACCOUNT" 
fi

if [ -z "$TAG_VERSION" ]; then
    TAG_VERSION=$VERSION_DEFAULT
    log_info "Using default tag version: $TAG_VERSION"
fi

if [ -z "$REPO" ]; then
    REPO=$REPO_DEFAULT
    log_info "Using default repo: $REPO" 
fi

ENGINES=("podman-hpc" "podman" "docker")
RUNTIME_ENGINE=""

for item in "${ENGINES[@]}"; do
    log_info "Checking for $item runtime engine"
    RESULT=$(command -v "$item" || true)

    if [ -n "$RESULT" ]; then
        log_info "Using $item runtime engine."
        RUNTIME_ENGINE="$RESULT"
        break
    else
        log_info "Runtime engine $item was not found."
    fi
done

if [ -z "$RUNTIME_ENGINE" ]; then
    log_error "No runtime engine was found."
    exit 1
fi

LATEST="latest"
VTAG="cstar-$IMAGE_NAME:$TAG_VERSION"
TAG_V="$ACCOUNT/cstar-$IMAGE_NAME:$TAG_VERSION"
TAG_L="$ACCOUNT/cstar-$IMAGE_NAME:$LATEST"
CONTEXT_CONTENT=$(ls -l $CONTEXT_PATH)

if [[ "$CONTEXT_CONTENT" != *Dockerfile* ]]; then
    log_error "Dockerfile not found in $CONTEXT_PATH"
    exit 1
fi

cp "$CONTEXT_PATH/../entrypoint.sh" $CONTEXT_PATH/entrypoint.sh
chmod a+r $CONTEXT_PATH/entrypoint.sh

if [ -z "$BUILD_ARGS" ]; then
    log_info "Building $VTAG in $CONTEXT_PATH from source \n $CONTEXT_DIR"
    $RUNTIME_ENGINE build -t "$VTAG" "$CONTEXT_PATH"
else
    log_info "Building parameterized $VTAG in $CONTEXT_PATH from source \n$CONTEXT_DIR\n$BUILD_ARGS"
    $RUNTIME_ENGINE build -t "$VTAG" --build-arg "$BUILD_ARGS" "$CONTEXT_PATH" 
fi

if [[ "$RUNTIME_ENGINE" =~ "hpc" ]]; then
    log_info "Migrating $VTAG"
    $RUNTIME_ENGINE migrate "$VTAG"
fi

TAGS=("$TAG_V" "$TAG_L")

for item in "${TAGS[@]}"; do
    log_info "Tagging $VTAG in $CONTEXT_PATH as [$item]"
    $RUNTIME_ENGINE tag "$VTAG" "$item"

    if [[ "$RUNTIME_ENGINE" =~ "hpc" ]]; then
        log_info "Migrating $item"
        $RUNTIME_ENGINE migrate "$item"
    fi

    log_info "Publishing $REPO/$item"
    $RUNTIME_ENGINE push "$item"
done

log_info "Run the new image with: $RUNTIME_ENGINE run -i -t --rm $TAG_V bash"
rm $CONTEXT_PATH/entrypoint.sh
