#!/bin/bash
set -e

# Parameters:
# - see action handling blocks
#
# Required Environment variables:
# $CSTAR_CI_TAG_VERSION
# - The version information to use when tagging & pushing images (e.g. <repo>/<image>:<$CSTAR_CI_TAG_VERSION>)
# $CSTAR_CI_CONTAINER_REGISTRY_ACCOUNT
# - The acount to use when tagging images (e.g. ubuntu)
# $CSTAR_CI_CONTAINER_REGISTRY
# - The image repository to use when tagging & pushing images (e.g. "docker.io")

SCRIPT="$0"
ACTION="$1"

ACCOUNT="$CSTAR_CI_CONTAINER_REGISTRY_ACCOUNT"
REPO_URI="$CSTAR_CI_CONTAINER_REGISTRY"

LOG_FORMAT="[%s] %s %b\n"
RUNTIME_ENGINE=""

log_info() {
    # print a log entry to stdout using LOG_FORMAT
    printf "$LOG_FORMAT" "INFO" "$SCRIPT" "$1"
}

log_error() {
    # print a log entry to stderr using LOG_FORMAT
    printf "$LOG_FORMAT" "ERROR" "$SCRIPT" "$1" >&2
}

show_usage() {
    # Print a user-friendly usage string on stdout
    printf "Usage: %s <action> [<parameters>]\n" $SCRIPT
    printf " > %s [login|build|pull|run|clean] <parameters>\n" $SCRIPT
    printf "\n"
    printf "Using the build action:\n"
    printf "%s build <context-path> <image-name> [<build-args>]\n\n" $SCRIPT
    printf " > %s build ~/code/cstar/ci/docker/buildbase buildbase\n" $SCRIPT
    printf " > %s build ~/code/cstar/ci/docker/runner runner\n" $SCRIPT
    printf "\n"
    printf "Using the login action:\n"
    printf "%s login\n\n" $SCRIPT
    printf "> %s login\n" $SCRIPT
    printf "\n"
    printf "Using the pull action:\n"
    printf "%s pull <image-name>\n" $SCRIPT
    printf " > %s pull buildbase\n" $SCRIPT
    printf " > %s pull runner\n" $SCRIPT
    printf "\n"
    printf "Required Environment Variables:\n"
    printf " CSTAR_CI_CONTAINER_REGISTRY_ACCOUNT\n"
    printf " CSTAR_CI_CONTAINER_REGISTRY\n"
    printf " CSTAR_CI_TAG_VERSION\n"
}

check_num_args() {
    # Verify the minimum number of arguments is passed.
    if [ $1 -lt $2 ]; then
        log_error "Not enough arguments supplied. $0 $1 $2 $3"
        exit 1
    fi
}

check_context_path() {
    # Verify the context path exists and has a Containerfile to build
    if [ ! -e "$1" ]; then
        log_error "Directory was not found at the supplied context path: $1"
        exit 1
    fi

    CONTEXT_CONTENT=$(ls -l $1)

    if [[ "$CONTEXT_CONTENT" != *Dockerfile* && "$CONTEXT_CONTENT" != *Containerfile* ]]; then
        log_error "Containerfile not found in: $1"
        exit 1
    fi
}

get_runtime_engine() {
    # Determine the appropriate runtime engine to use
    ENGINES=("podman-hpc" "podman" "docker")
    RUNTIME_ENGINE=""

    for item in "${ENGINES[@]}"; do
        RESULT=$(command -v "$item" || true)

        if [ -n "$RESULT" ]; then
            RUNTIME_ENGINE="$RESULT"
            break
        fi
    done

    if [ -z "$RUNTIME_ENGINE" ]; then
        log_error "No runtime engine was found."
        exit 1
    fi

    echo "$RUNTIME_ENGINE"
}

check_registry_info() {
    # Check for registry information in environment variables
    if [ -z "$CSTAR_CI_CONTAINER_REGISTRY_ACCOUNT" ]; then
        log_error "Container registry account not set."
        log_info "Configure environment with: export CSTAR_CI_CONTAINER_REGISTRY_ACCOUNT=<account-id>"
        exit 1
    fi

    log_info "Using container registry account: $CSTAR_CI_CONTAINER_REGISTRY_ACCOUNT"

    if [ -z "$CSTAR_CI_CONTAINER_REGISTRY" ]; then
        log_error "Container registry URI not set."
        log_info "Configure environment with: export CSTAR_CI_CONTAINER_REGISTRY=<registry-uri>"
        exit 1
    fi

    log_info "Using container registry URI: $CSTAR_CI_CONTAINER_REGISTRY"
}

perform_build() {
    # Build an image
    # Parameters:
    # $1 - context path (directory containing a Containerfile)
    # $2 - core image name (e.g. python for cstar-python:latest)
    # $3 - build args to customize the image
    CONTEXT_PATH=$1
    IMAGE_NAME=$2
    BUILD_ARGS=$3

    check_context_path "$CONTEXT_PATH"

    LOCAL_TAG="cstar-$IMAGE_NAME:$CSTAR_CI_TAG_VERSION"
    TAG_V="$ACCOUNT/cstar-$IMAGE_NAME:$CSTAR_CI_TAG_VERSION"
    TAG_L="$ACCOUNT/cstar-$IMAGE_NAME:latest"

    cp "$CONTEXT_PATH/../entrypoint.sh" $CONTEXT_PATH/entrypoint.sh
    chmod a+r $CONTEXT_PATH/entrypoint.sh

    if [ -z "$BUILD_ARGS" ]; then
        log_info "Building '$LOCAL_TAG' in '$CONTEXT_PATH'"
        $RUNTIME_ENGINE build -t "$LOCAL_TAG" --build-arg ACCT=$ACCOUNT --build-arg FROM_TAG=$CSTAR_CI_TAG_VERSION  "$CONTEXT_PATH"
    else
        log_info "Building parameterized '$LOCAL_TAG' in '$CONTEXT_PATH' with build-args '$BUILD_ARGS'"
        $RUNTIME_ENGINE build -t "$LOCAL_TAG" --build-arg "$BUILD_ARGS" --build-arg ACCT=$ACCOUNT --build-arg FROM_TAG=$CSTAR_CI_TAG_VERSION "$CONTEXT_PATH"
    fi

    if [[ "$RUNTIME_ENGINE" =~ "hpc" ]]; then
        log_info "Migrating: $LOCAL_TAG"
        $RUNTIME_ENGINE migrate "$LOCAL_TAG"
    fi

    TAGS=("$TAG_V" "$TAG_L")

    for item in "${TAGS[@]}"; do
        log_info "Tagging '$LOCAL_TAG' in '$CONTEXT_PATH' as [$item]"
        $RUNTIME_ENGINE tag "$LOCAL_TAG" "$item"

        if [[ "$RUNTIME_ENGINE" =~ "hpc" ]]; then
            log_info "Migrating: $item"
            $RUNTIME_ENGINE migrate "$item"
        fi

        log_info "Publishing: $REPO_URI/$item"
        $RUNTIME_ENGINE push "$item"
    done

    log_info "Build complete. Run with: $RUNTIME_ENGINE run -i -t --rm $TAG_V bash"
}

perform_login() {
    # Authenticate with an image repository
    $RUNTIME_ENGINE login $REPO_URI -u "$ACCOUNT"
}

perform_pull() {
    # Fetch an image from the image repository
    # Parameters:
    # $1 - image name
    $RUNTIME_ENGINE pull $REPO_URI/$ACCOUNT/cstar-$1:latest
}

perform_run() {
    # Run an interactive shell in the latest version of an image
    # Parameters:
    # $1 - image name
    $RUNTIME_ENGINE run --rm -i -t $ACCOUNT/cstar-$1:latest bash
}

perform_clean() {
    # Clean up local image resources
    # Parameters:
    # $1 - image name
    if [[ "$RUNTIME_ENGINE" =~ "hpc" ]]; then
        log_info "Cleaning..."
        $RUNTIME_ENGINE images --format "{{.Repository}}:{{.Tag}}" | sed 's/^localhost\///' | grep "$1" | xargs -r $RUNTIME_ENGINE rmi --force --ignore
    else
        $RUNTIME_ENGINE image rm $REPO_URI/$ACCOUNT/cstar-$1:latest
    fi
}

handle_exit() {
    exit_code=$?

    if [[ -n "$CONTEXT_PATH" && -f "$CONTEXT_PATH/entrypoint.sh" ]]; then
        rm -f "$CONTEXT_PATH/entrypoint.sh"
    fi

    if [ $exit_code -ne 0 ]; then
        show_usage
    fi
}

trap handle_exit EXIT

RUNTIME_ENGINE=$(get_runtime_engine)

check_registry_info

if [[ $ACTION == "build" ]]; then
    check_num_args $# 3
    CONTEXT_PATH="$2"
    IMAGE_NAME="$3"
    BUILD_ARGS="$4"
    
    perform_build "$CONTEXT_PATH" "$IMAGE_NAME" "$BUILD_ARGS"
elif [[ $ACTION == "login" ]]; then
    perform_login
elif [[ $ACTION == "run" ]]; then
    check_num_args $# 2
    IMAGE_NAME="$2"
    
    perform_run $IMAGE_NAME
elif [[ $ACTION == "pull" ]]; then
    check_num_args $# 2
    IMAGE_NAME="$2"
    
    perform_pull $IMAGE_NAME
elif [[ $ACTION == "clean" ]]; then
    check_num_args $# 2
    IMAGE_NAME="$2"
    
    perform_clean $IMAGE_NAME
else
    show_usage
fi
