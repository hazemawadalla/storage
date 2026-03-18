#!/bin/bash

# ============================================================================
# KV Cache Benchmark - Complete Functional Test Runner with Multinode Clients
# ============================================================================
# Runs all functional tests and outputs results line by line
# Usage: ./run_kv_cache_tests_all_mc.sh --cache-target /path/to/cache [options]
# ============================================================================

set -o pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Test configuration
BENCHMARK_CMD="python3 kv-cache.py"
CONFIG_FILE="config.yaml"
FAILED_TESTS=0
PASSED_TESTS=0
TOTAL_TESTS=0

# Create logs directory
LOG_DIR="$SCRIPT_DIR/testkvcache"
mkdir -p "$LOG_DIR"

# Parse command line arguments
CACHE_TARGET=""
CLIENT_LIST=""
LOADENV_PATH=""
SKIP_MC_TESTS=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --cache-target|-c)
            CACHE_TARGET="$2"
            shift 2
            ;;
        --client-list|-l)
            CLIENT_LIST="$2"
            shift 2
            ;;
        --loadenv|-e)
            LOADENV_PATH="$2"
            shift 2
            ;;
        --skip-multinode)
            SKIP_MC_TESTS=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 --cache-target /path/to/cache [options]"
            echo ""
            echo "Options:"
            echo "  --cache-target, -c      Cache directory for all tests (required)"
            echo "  --client-list, -l       Comma-separated list of clients (e.g., c0,c1,c2) for multinode tests"
            echo "  --loadenv, -e           Path to environment script to source before running commands"
            echo "  --skip-multinode        Skip multinode client tests entirely"
            echo "  --help, -h              Show this help message"
            echo ""
            echo "Example with multinode clients:"
            echo "  $0 --cache-target /tmp/kv_cache_tests --client-list c0,c1,c2 --loadenv /path/to/env.sh"
            echo ""
            echo "Example without multinode:"
            echo "  $0 --cache-target /tmp/kv_cache_tests"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate cache target
if [[ -z "$CACHE_TARGET" ]]; then
    echo "Error: --cache-target is required"
    echo "Usage: $0 --cache-target /path/to/cache"
    exit 1
fi

# Create cache directory
mkdir -p "$CACHE_TARGET"
CACHE_BASE="$CACHE_TARGET"

# Cleanup function
cleanup() {
    echo ""
    echo "Cache directory: $CACHE_BASE"
    ls -la "$CACHE_BASE" 2>/dev/null || echo "Directory does not exist"
}

trap cleanup EXIT

# Check if clush is available for multinode tests
check_clush() {
    if ! command -v clush &> /dev/null; then
        echo -e "${RED}Error: clush is not installed.${NC}"
        echo "Install Cluster Shell with:"
        echo "  Ubuntu/Debian: sudo apt-get install clustershell"
        echo "  RHEL/CentOS: sudo yum install ClusterShell"
        echo ""
        return 1
    fi
    return 0
}

# Helper function to run a test with logging
run_test() {
    local test_name="$1"
    shift
    local cmd="$@"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # Create file paths
    log_file="$LOG_DIR/test_${TOTAL_TESTS}.log"
    cmd_file="$LOG_DIR/cmd_${TOTAL_TESTS}.log"
    success_file="$LOG_DIR/success_${TOTAL_TESTS}.log"
    
    echo -n "Test $TOTAL_TESTS ($test_name): "
    
    # Check if this test already succeeded (skip if success file exists)
    if [[ -f "$success_file" ]]; then
        echo -e "${YELLOW}[SKIPPED]${NC}"
        return 0
    fi
    
    # Save the command to cmd file
    echo "# Command executed at $(date)" > "$cmd_file"
    echo "# Test: $test_name" >> "$cmd_file"
    echo "$cmd" >> "$cmd_file"
    
    if eval "$cmd" > "$log_file" 2>&1; then
        # Command succeeded - create success marker file
        echo "Test $TOTAL_TESTS ($test_name) completed successfully at $(date)" > "$success_file"
        echo -e "${GREEN}[OK]${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}[FAIL]${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# Helper function to run multinode client test
# Uses clush to distribute command across multiple clients
run_test_mc() {
    local test_name="$1"
    local clients="$2"
    shift 2
    
    # Build command sequence
    # 1. Change to script directory on each client
    # 2. Source loadenv if provided
    # 3. Run the benchmark command
    
    local cd_cmd="cd '$SCRIPT_DIR'"
    
    if [[ -n "$LOADENV_PATH" ]]; then
        final_cmd="$cd_cmd && source $LOADENV_PATH && $@"
    else
        final_cmd="$cd_cmd && $@"
    fi
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # Create file paths
    log_file="$LOG_DIR/test_${TOTAL_TESTS}_mc.log"
    cmd_file="$LOG_DIR/cmd_${TOTAL_TESTS}_mc.log"
    success_file="$LOG_DIR/success_${TOTAL_TESTS}_mc.log"
    
    echo -n "Test $TOTAL_TESTS ($test_name, clients=$clients): "
    
    # Check if this test already succeeded (skip if success file exists)
    if [[ -f "$success_file" ]]; then
        echo -e "${YELLOW}[SKIPPED]${NC}"
        return 0
    fi
    
    # Save the command to cmd file
    echo "# Multinode Command executed at $(date)" > "$cmd_file"
    echo "# Test: $test_name" >> "$cmd_file"
    echo "# Clients: $clients" >> "$cmd_file"
    echo "clush -w $clients -S -b '$final_cmd'" >> "$cmd_file"
    
    # Run clush command
    # clush -w <clients> -S -b <command>
    # -w: target clients
    # -S: SSH to each client (returns if any fails)
    # -b: bare command execution
    
    local clush_cmd="clush -w $clients -S -b '$final_cmd'"
    
    if eval "$clush_cmd" > "$log_file" 2>&1; then
        echo -e "${GREEN}[OK]${NC}"
        echo "Test $TOTAL_TESTS ($test_name, clients=$clients) completed successfully at $(date)" > "$success_file"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}[FAIL]${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# ============================================================================
# SECTION 1: Basic Storage Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 1: Basic Storage Tests (Single Node)"
echo "============================================================================"
echo ""

# Test 1.1: Basic Storage Test (Small)
run_test "Basic Storage Test (50 users)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_basic"

# Test 1.2: NVMe Only Test
run_test "NVMe Only (No GPU, No CPU)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 100 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 0 --max-concurrent-allocs 16 --generation-mode none --cache-dir $CACHE_BASE/kv_cache_nvme_only"

# Test 1.3: CPU + NVMe Test
run_test "CPU + NVMe (GPU disabled)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --max-concurrent-allocs 16 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_cpu_nvme"

# Test 1.4: Full Three-Tier Test
run_test "Full Three-Tier (GPU + CPU + NVMe)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 30 --duration 60 --gpu-mem-gb 16 --cpu-mem-gb 32 --max-concurrent-allocs 0 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_full_stack"

# ============================================================================
# SECTION 2: Model-Specific Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 2: Model-Specific Tests"
echo "============================================================================"
echo ""

# Test 2.1: Tiny Model
run_test "Tiny Model (1B parameters)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model tiny-1b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --cache-dir $CACHE_BASE/kv_cache_tiny"

# Test 2.2: Mistral 7B
run_test "Mistral 7B" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model mistral-7b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --cache-dir $CACHE_BASE/kv_cache_mistral"

# Test 2.3: Llama 3.1 70B
run_test "Llama 3.1 70B (Large model)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-70b-instruct --num-users 20 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 8 --max-concurrent-allocs 4 --cache-dir $CACHE_BASE/kv_cache_70b"

# Test 2.4: DeepSeek-v3
run_test "DeepSeek-v3 (MLA architecture)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model deepseek-v3 --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 8 --cache-dir $CACHE_BASE/kv_cache_deepseek"

# ============================================================================
# SECTION 3: Advanced Feature Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 3: Advanced Feature Tests"
echo "============================================================================"
echo ""

# Test 3.1: QoS Autoscaling
run_test "QoS Autoscaling" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 20 --duration 60 --gpu-mem-gb 16 --cpu-mem-gb 32 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_qos --enable-autoscaling --autoscaler-mode qos"

# Test 3.2: Capacity Mode Autoscaling
run_test "Capacity Mode Autoscaling" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-70b-instruct --num-users 10 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 32 --max-concurrent-allocs 0 --generation-mode none --cache-dir $CACHE_BASE/kv_cache_capacity --enable-autoscaling --autoscaler-mode capacity"

# Test 3.3: Prefix Caching Disabled
run_test "Prefix Caching Disabled" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --cache-dir $CACHE_BASE/kv_cache_no_prefix --disable-prefix-caching"

# Test 3.4: Multi-Turn Disabled
run_test "Multi-Turn Disabled" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --cache-dir $CACHE_BASE/kv_cache_no_multiturn --disable-multi-turn"

# Test 3.5: RAG Enabled
run_test "RAG Enabled" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --cache-dir $CACHE_BASE/kv_cache_rag --enable-rag"

# ============================================================================
# SECTION 4: Performance Profile Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 4: Performance Profile Tests"
echo "============================================================================"
echo ""

# Test 4.1: Latency Profile
run_test "Latency Performance Profile" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 16 --cpu-mem-gb 32 --cache-dir $CACHE_BASE/kv_cache_latency --performance-profile latency"

# Test 4.2: Throughput Profile
run_test "Throughput Performance Profile" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 100 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 0 --max-concurrent-allocs 0 --generation-mode none --cache-dir $CACHE_BASE/kv_cache_throughput --performance-profile throughput"

# ============================================================================
# SECTION 5: Multi-Client Tests (Multinode)
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 5: Multi-Client Tests (Multinode)"
echo "============================================================================"
echo ""

# Check if client list is provided and multinode not skipped
if [[ -n "$CLIENT_LIST" && "$SKIP_MC_TESTS" != "true" ]]; then
    echo -e "${BLUE}Running multi-client tests with clients: $CLIENT_LIST${NC}"
    echo ""
    
    # Test 5.1: Multi-Client Test
    run_test_mc "Multi-Client Test (N clients)" "$CLIENT_LIST" \
        "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 25 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --cache-dir $CACHE_BASE/kv_cache_parallel_1"
    
    # Test 5.2: Multi-Client Scalability
    run_test_mc "Multi-Client Scalability Test (N clients)" "$CLIENT_LIST" \
        "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 20 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --cache-dir $CACHE_BASE/kv_cache_parallel_2"
    
    # Test 5.3: High Scale Multi-Client
    run_test_mc "High Scale Multi-Client Test (N clients)" "$CLIENT_LIST" \
        "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 15 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --cache-dir $CACHE_BASE/kv_cache_parallel_3"
else
    if [[ -z "$CLIENT_LIST" ]]; then
        echo -e "${YELLOW}No client list provided. Multi-client tests skipped.${NC}"
        echo "To enable these tests, run with: --client-list c0,c1,c2 --loadenv /path/to/env.sh"
        echo ""
    elif [[ "$SKIP_MC_TESTS" == "true" ]]; then
        echo -e "${YELLOW}Multi-client tests explicitly skipped (--skip-multinode)${NC}"
        echo ""
    fi
fi

# ============================================================================
# SECTION 6: Storage-Specific Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 6: Storage-Specific Tests"
echo "============================================================================"
echo ""

# Test 6.1: Explicit Storage Capacity
run_test "Explicit Storage Capacity (50GB)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 0 --storage-capacity-gb 50 --cache-dir $CACHE_BASE/kv_cache_explicit_capacity"

# ============================================================================
# SECTION 7: Mode-Specific Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 7: Mode-Specific Tests"
echo "============================================================================"
echo ""

# Test 7.1: Decode-Only Mode
run_test "Decode-Only Mode" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_decode_only --decode-only"

# Test 7.2: Prefill-Only Mode
run_test "Prefill-Only Mode" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 8 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_prefill_only --prefill-only"

# ============================================================================
# SECTION 8: Reproducibility Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 8: Reproducibility Tests"
echo "============================================================================"
echo ""

# Test 8.1: Seed Reproducibility
run_test "Seed Reproducibility (seed=42)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 30 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --seed 42 --cache-dir $CACHE_BASE/kv_cache_seed_42"

# Test 8.2: Seed Reproducibility (seed=123)
run_test "Seed Reproducibility (seed=123)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 30 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --seed 123 --cache-dir $CACHE_BASE/kv_cache_seed_123"

# Test 8.3: Seed Reproducibility (seed=456)
run_test "Seed Reproducibility (seed=456)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 30 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --seed 456 --cache-dir $CACHE_BASE/kv_cache_seed_456"

# ============================================================================
# SECTION 9: Short Duration Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 9: Short Duration Tests"
echo "============================================================================"
echo ""

# Test 9.1: Quick Test
run_test "Quick Test (60 seconds)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 20 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 2 --cache-dir $CACHE_BASE/kv_cache_quick"

# ============================================================================
# SECTION 10: Storage Performance Assessment
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 10: Storage Performance Assessment"
echo "============================================================================"
echo ""

# Test 10.1: Full Storage Assessment
run_test "Storage Performance Assessment" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 100 --duration 120 --gpu-mem-gb 0 --cpu-mem-gb 0 --max-concurrent-allocs 16 --generation-mode none --cache-dir $CACHE_BASE/kv_cache_assessment"

# ============================================================================
# SECTION 11: GPU Support Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 11: GPU Support Tests"
echo "============================================================================"
echo ""

# Test 11.1: GPU Test (if GPU available)
run_test "GPU Support Test (16GB)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 16 --cpu-mem-gb 8 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_gpu_test"

# ============================================================================
# SECTION 12: Export Format Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 12: Export Format Tests"
echo "============================================================================"
echo ""

# Test 12.1: CSV Export
run_test "JSON Export" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 30 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --seed 42 --output $CACHE_BASE/results_export_test.json"

# ============================================================================
# SECTION 13: Duration Scaling Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 13: Duration Scaling Tests"
echo "============================================================================"
echo ""

# Test 13.1: Long Duration Stress Test
run_test "Long Duration Stress (300 seconds)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 200 --duration 300 --gpu-mem-gb 0 --cpu-mem-gb 0 --max-concurrent-allocs 16 --generation-mode none --cache-dir $CACHE_BASE/kv_cache_long_stress"

# ============================================================================
# SECTION 14: Scale Scaling Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 14: Scale Scaling Tests"
echo "============================================================================"
echo ""

# Test 14.1: High Scale Test
run_test "High Scale Test (300 users)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 300 --duration 120 --gpu-mem-gb 0 --cpu-mem-gb 0 --max-concurrent-allocs 32 --generation-mode none --cache-dir $CACHE_BASE/kv_cache_high_scale"

# ============================================================================
# SECTION 15: Preconditioning Tests
# ============================================================================
echo ""
echo "============================================================================"
echo "SECTION 15: Preconditioning Tests"
echo "============================================================================"
echo ""

# Test 15.1: Preconditioning Mode (prefill cache population without decode)
run_test "Preconditioning Mode" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 100 --duration 120 --gpu-mem-gb 0 --cpu-mem-gb 0 --max-concurrent-allocs 16 --generation-mode none --cache-dir $CACHE_BASE/kv_cache_preconditioning --precondition"

# Test 15.2: Prefill-Only Mode (decode disabled, different from preconditioning)
run_test "Prefill-Only Mode (Decode Disabled)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 8 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_prefill_only_2 --prefill-only"

# Test 15.3: Decode-Only Mode (prefill disabled, different from preconditioning)
run_test "Decode-Only Mode (Prefill Disabled)" \
    "$BENCHMARK_CMD --config $CONFIG_FILE --model llama3.1-8b --num-users 50 --duration 60 --gpu-mem-gb 0 --cpu-mem-gb 4 --generation-mode realistic --cache-dir $CACHE_BASE/kv_cache_decode_only_2 --decode-only --prefill-threads 0"

# ============================================================================
# SUMMARY
# ============================================================================
echo ""
echo "============================================================================"
echo "TEST SUMMARY"
echo "============================================================================"
echo ""
echo -e "Total Tests:  $TOTAL_TESTS"
echo -e "Passed:       ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed:       ${RED}$FAILED_TESTS${NC}"
echo ""

if [[ $FAILED_TESTS -eq 0 ]]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${YELLOW}Some tests failed. Check outputs for details.${NC}"
    exit 1
fi
