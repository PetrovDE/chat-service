#!/bin/bash

# Monitoring script for Llama Chat Service
# Provides comprehensive system and service monitoring

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Configuration
APP_DIR="/opt/llama-chat"
LOG_DIR="/var/log/llama-chat"
DATA_DIR="/var/lib/llama-chat"

# Functions
print_header() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════${NC}"
}

print_status() {
    echo -e "${BLUE}►${NC} $1"
}

check_service() {
    local service=$1
    local status=$(systemctl is-active $service 2>/dev/null)

    if [ "$status" = "active" ]; then
        echo -e "  $service: ${GREEN}● Running${NC}"
    elif [ "$status" = "inactive" ]; then
        echo -e "  $service: ${YELLOW}○ Stopped${NC}"
    else
        echo -e "  $service: ${RED}✗ Failed${NC}"
    fi

    # Get uptime if running
    if [ "$status" = "active" ]; then
        local uptime=$(systemctl show $service --property=ActiveEnterTimestamp --value)
        if [ ! -z "$uptime" ]; then
            local duration=$(systemd-analyze timespan "$(date +%s)s - $(date -d "$uptime" +%s)s" 2>/dev/null | grep "Human" | cut -d: -f2)
            echo -e "    Uptime:$duration"
        fi
    fi
}

# System Information
system_info() {
    print_header "System Information"

    echo -e "Hostname: ${BLUE}$(hostname -f)${NC}"
    echo -e "OS: ${BLUE}$(cat /etc/os-release | grep PRETTY_NAME | cut -d'"' -f2)${NC}"
    echo -e "Kernel: ${BLUE}$(uname -r)${NC}"
    echo -e "Uptime: ${BLUE}$(uptime -p)${NC}"
    echo -e "Load Average: ${BLUE}$(uptime | awk -F'load average:' '{print $2}')${NC}"
}

# Service Status
service_status() {
    print_header "Service Status"

    check_service "llama-chat"
    check_service "ollama"
    check_service "nginx"
}

# Resource Usage
resource_usage() {
    print_header "Resource Usage"

    # CPU Usage
    echo -e "${YELLOW}CPU Usage:${NC}"
    top -bn1 | head -5 | tail -4
    echo ""

    # Memory Usage
    echo -e "${YELLOW}Memory Usage:${NC}"
    free -h | grep -E "^Mem|^Swap"
    echo ""

    # Disk Usage
    echo -e "${YELLOW}Disk Usage:${NC}"
    df -h | grep -E "^/dev|Filesystem" | head -5
    echo ""

    # Specific directories
    echo -e "${YELLOW}Application Directories:${NC}"
    du -sh $APP_DIR 2>/dev/null | awk '{print "  App Dir: " $1}'
    du -sh $LOG_DIR 2>/dev/null | awk '{print "  Log Dir: " $1}'
    du -sh $DATA_DIR 2>/dev/null | awk '{print "  Data Dir: " $1}'
}

# Process Information
process_info() {
    print_header "Process Information"

    # Llama Chat processes
    echo -e "${YELLOW}Llama Chat Processes:${NC}"
    ps aux | grep -E "uvicorn|llama-chat" | grep -v grep || echo "  No processes found"
    echo ""

    # Ollama processes
    echo -e "${YELLOW}Ollama Processes:${NC}"
    ps aux | grep ollama | grep -v grep || echo "  No processes found"
}

# Network Connections
network_info() {
    print_header "Network Connections"

    echo -e "${YELLOW}Listening Ports:${NC}"
    ss -tlnp 2>/dev/null | grep -E ":8000|:11434|:80|:443" | while read line; do
        echo "  $line"
    done
    echo ""

    # Active connections
    local conn_count=$(ss -tn state established '( dport = :8000 or dport = :443 )' | wc -l)
    echo -e "Active Connections: ${BLUE}$conn_count${NC}"
}

# API Health Check
api_health() {
    print_header "API Health Check"

    # Check local API
    print_status "Local API (http://localhost:8000/health):"
    if curl -s -f -m 5 http://localhost:8000/health > /tmp/health.json 2>/dev/null; then
        echo -e "  Status: ${GREEN}● Healthy${NC}"

        # Parse JSON response
        if command -v jq &> /dev/null; then
            cat /tmp/health.json | jq '.'
        else
            python3 -m json.tool < /tmp/health.json 2>/dev/null || cat /tmp/health.json
        fi
    else
        echo -e "  Status: ${RED}✗ Unreachable${NC}"
    fi
    rm -f /tmp/health.json

    echo ""

    # Check Ollama API
    print_status "Ollama API (http://localhost:11434):"
    if curl -s -f -m 5 http://localhost:11434/api/tags > /tmp/ollama.json 2>/dev/null; then
        echo -e "  Status: ${GREEN}● Healthy${NC}"

        # Count models
        if command -v jq &> /dev/null; then
            local model_count=$(cat /tmp/ollama.json | jq '.models | length')
            echo -e "  Available Models: ${BLUE}$model_count${NC}"
            cat /tmp/ollama.json | jq '.models[].name' 2>/dev/null | while read model; do
                echo "    - $model"
            done
        fi
    else
        echo -e "  Status: ${RED}✗ Unreachable${NC}"
    fi
    rm -f /tmp/ollama.json
}

# Log Analysis
log_analysis() {
    print_header "Recent Logs"

    # Llama Chat logs
    echo -e "${YELLOW}Llama Chat Service (last 10 lines):${NC}"
    if [ -f "$LOG_DIR/service.log" ]; then
        tail -n 10 $LOG_DIR/service.log
    else
        journalctl -u llama-chat -n 10 --no-pager 2>/dev/null || echo "  No logs available"
    fi
    echo ""

    # Error logs
    echo -e "${YELLOW}Recent Errors:${NC}"
    if [ -f "$LOG_DIR/error.log" ]; then
        tail -n 5 $LOG_DIR/error.log 2>/dev/null | grep -E "ERROR|CRITICAL" || echo "  No recent errors"
    else
        journalctl -u llama-chat -p err -n 5 --no-pager 2>/dev/null || echo "  No errors found"
    fi
}

# Performance Metrics
performance_metrics() {
    print_header "Performance Metrics"

    # Response time test
    echo -e "${YELLOW}API Response Time:${NC}"
    if command -v curl &> /dev/null; then
        local response_time=$(curl -o /dev/null -s -w '%{time_total}' http://localhost:8000/health 2>/dev/null)
        if [ ! -z "$response_time" ]; then
            echo -e "  Health endpoint: ${BLUE}${response_time}s${NC}"
        fi
    fi

    # Model performance (if available)
    echo ""
    echo -e "${YELLOW}Model Performance:${NC}"
    # This would need actual metrics from your application
    echo "  [Metrics collection not implemented]"
}

# Recommendations
recommendations() {
    print_header "Recommendations"

    local has_issues=false

    # Check services
    if ! systemctl is-active --quiet llama-chat; then
        echo -e "${RED}⚠${NC} Llama Chat service is not running"
        echo "  Run: sudo systemctl start llama-chat"
        has_issues=true
    fi

    if ! systemctl is-active --quiet ollama; then
        echo -e "${RED}⚠${NC} Ollama service is not running"
        echo "  Run: sudo systemctl start ollama"
        has_issues=true
    fi

    # Check disk space
    local disk_usage=$(df / | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ "$disk_usage" -gt 80 ]; then
        echo -e "${YELLOW}⚠${NC} Disk usage is above 80%"
        echo "  Consider cleaning up old logs and files"
        has_issues=true
    fi

    # Check memory
    local mem_usage=$(free | awk '/^Mem:/ {printf "%.0f", $3/$2 * 100}')
    if [ "$mem_usage" -gt 90 ]; then
        echo -e "${YELLOW}⚠${NC} Memory usage is above 90%"
        echo "  Consider adding more RAM or reducing worker processes"
        has_issues=true
    fi

    if [ "$has_issues" = false ]; then
        echo -e "${GREEN}✓${NC} All systems operating normally"
    fi
}

# Interactive mode
interactive_mode() {
    while true; do
        echo ""
        echo -e "${CYAN}═══════════════════════════════════════${NC}"
        echo -e "${CYAN}  Llama Chat Monitor - Interactive${NC}"
        echo -e "${CYAN}═══════════════════════════════════════${NC}"
        echo ""
        echo "1) System Information"
        echo "2) Service Status"
        echo "3) Resource Usage"
        echo "4) Process Information"
        echo "5) Network Connections"
        echo "6) API Health Check"
        echo "7) Log Analysis"
        echo "8) Performance Metrics"
        echo "9) Full Report"
        echo "0) Exit"
        echo ""
        read -p "Select option: " option

        case $option in
            1) system_info ;;
            2) service_status ;;
            3) resource_usage ;;
            4) process_info ;;
            5) network_info ;;
            6) api_health ;;
            7) log_analysis ;;
            8) performance_metrics ;;
            9) main ;;
            0) exit 0 ;;
            *) echo "Invalid option" ;;
        esac

        echo ""
        read -p "Press Enter to continue..."
    done
}

# Main function
main() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}  Llama Chat Service Monitor${NC}"
    echo -e "${BLUE}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"

    system_info
    service_status
    resource_usage
    network_info
    api_health
    log_analysis
    recommendations
}

# Parse arguments
case "${1:-}" in
    -i|--interactive)
        interactive_mode
        ;;
    -h|--help)
        echo "Usage: $0 [OPTIONS]"
        echo "Options:"
        echo "  -i, --interactive  Interactive mode"
        echo "  -w, --watch       Continuous monitoring (updates every 5s)"
        echo "  -h, --help        Show this help message"
        exit 0
        ;;
    -w|--watch)
        while true; do
            clear
            main
            sleep 5
        done
        ;;
    *)
        main
        ;;
esac