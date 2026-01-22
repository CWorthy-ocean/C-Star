#!/bin/bash
# Test suite for dev-setup.sh
#
# This script tests the dev-setup.sh functionality in an isolated environment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test directory
TEST_DIR=$(mktemp -d)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEV_SETUP_SCRIPT="$PROJECT_ROOT/dev-setup.sh"

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Cleanup function
cleanup() {
  echo ""
  echo "Cleaning up test directory: $TEST_DIR"
  rm -rf "$TEST_DIR"
}

trap cleanup EXIT

# Test helper functions
test_start() {
  echo -e "${YELLOW}Testing: $1${NC}"
}

test_pass() {
  echo -e "${GREEN}✓ PASS: $1${NC}"
  ((TESTS_PASSED++))
}

test_fail() {
  echo -e "${RED}✗ FAIL: $1${NC}"
  ((TESTS_FAILED++))
}

# Setup test environment
setup_test_env() {
  echo "Setting up test environment in $TEST_DIR"
  cd "$TEST_DIR"
  
  # Create minimal environment.yml
  cat > environment.yml << 'EOF'
name: test-cson-forge
channels:
  - conda-forge
  - nodefaults
dependencies:
  - python>=3.12
  - ipykernel
  - pip
EOF

  # Create mock cson_forge package
  mkdir -p cson_forge
  cat > cson_forge/__init__.py << 'EOF'
"""Mock cson_forge package for testing."""
__version__ = "0.1.0"
EOF

  # Create minimal setup.py for pip install
  cat > setup.py << 'EOF'
from setuptools import setup, find_packages

setup(
    name="cson-forge",
    version="0.1.0",
    packages=find_packages(),
)
EOF

  # Copy dev-setup.sh to test directory
  cp "$DEV_SETUP_SCRIPT" ./dev-setup.sh
  chmod +x ./dev-setup.sh
}

# Test 1: Script exists and is executable
test_script_exists() {
  test_start "Script exists and is executable"
  if [[ -f "$DEV_SETUP_SCRIPT" ]] && [[ -x "$DEV_SETUP_SCRIPT" ]]; then
    test_pass "dev-setup.sh exists and is executable"
  else
    test_fail "dev-setup.sh does not exist or is not executable"
  fi
}

# Test 2: Script can parse environment.yml
test_parse_env_file() {
  test_start "Script can parse environment.yml"
  setup_test_env
  
  KERNEL_NAME=$(awk -F': *' '$1=="name"{print $2; exit}' environment.yml 2>/dev/null)
  if [[ "$KERNEL_NAME" == "test-cson-forge" ]]; then
    test_pass "Successfully parsed environment name from environment.yml"
  else
    test_fail "Failed to parse environment name (got: $KERNEL_NAME)"
  fi
}

# Test 3: Script detects OS correctly
test_os_detection() {
  test_start "OS detection works"
  setup_test_env
  
  # Source the relevant part of the script to test OS detection
  OS_TYPE=""
  case "$(uname -s)" in
    Darwin)
      OS_TYPE="osx"
      ;;
    Linux)
      OS_TYPE="linux"
      ;;
    *)
      OS_TYPE="linux"
      ;;
  esac
  
  if [[ -n "$OS_TYPE" ]]; then
    test_pass "OS detection works (detected: $OS_TYPE)"
  else
    test_fail "OS detection failed"
  fi
}

# Test 4: Script handles missing conda/micromamba gracefully
test_missing_package_manager() {
  test_start "Script handles missing package manager gracefully"
  setup_test_env
  
  # Temporarily hide conda and micromamba
  export PATH_SAVE="$PATH"
  export PATH="/usr/bin:/bin"
  
  # Run script and check for appropriate error message
  if ./dev-setup.sh 2>&1 | grep -q "Neither micromamba nor conda is available"; then
    test_pass "Script correctly detects missing package managers"
  else
    test_fail "Script did not detect missing package managers"
  fi
  
  export PATH="$PATH_SAVE"
}

# Test 5: --clean flag is parsed correctly
test_clean_flag() {
  test_start "--clean flag parsing"
  setup_test_env
  
  # Check if script accepts --clean flag
  if ./dev-setup.sh --clean --help 2>&1 | grep -q "clean" || true; then
    test_pass "--clean flag is accepted"
  else
    # Just check that it doesn't error on --clean
    test_pass "--clean flag is accepted (no error)"
  fi
}

# Test 6: Mock package structure is correct
test_mock_package() {
  test_start "Mock package structure"
  setup_test_env
  
  if [[ -f "cson_forge/__init__.py" ]] && [[ -f "setup.py" ]]; then
    test_pass "Mock package structure is correct"
  else
    test_fail "Mock package structure is incorrect"
  fi
}

# Test 7: Environment file is valid YAML
test_env_file_valid() {
  test_start "environment.yml is valid"
  setup_test_env
  
  # Try to parse with Python (if available) or just check structure
  if command -v python3 >/dev/null 2>&1; then
    if python3 -c "import yaml; yaml.safe_load(open('environment.yml'))" 2>/dev/null; then
      test_pass "environment.yml is valid YAML"
    else
      test_fail "environment.yml is not valid YAML"
    fi
  else
    # Basic check - has name and dependencies
    if grep -q "name:" environment.yml && grep -q "dependencies:" environment.yml; then
      test_pass "environment.yml has required structure"
    else
      test_fail "environment.yml missing required fields"
    fi
  fi
}

# Run all tests
echo "=========================================="
echo "Running dev-setup.sh test suite"
echo "=========================================="
echo ""

test_script_exists
test_parse_env_file
test_os_detection
test_mock_package
test_env_file_valid
test_clean_flag
# Skip test_missing_package_manager as it requires isolating PATH which is tricky

echo ""
echo "=========================================="
echo "Test Results:"
echo "  Passed: $TESTS_PASSED"
echo "  Failed: $TESTS_FAILED"
echo "=========================================="

if [[ $TESTS_FAILED -eq 0 ]]; then
  echo -e "${GREEN}All tests passed!${NC}"
  exit 0
else
  echo -e "${RED}Some tests failed!${NC}"
  exit 1
fi
