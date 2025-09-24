#!/bin/bash

# Update script for Llama Chat Service

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
APP_DIR="/opt/llama-chat"
APP_USER="llama-chat"
BACKUP_DIR="/var/backups/llama-chat"

# Functions
print_status() {
    echo -e "${BLUE}[*]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

# Check if running with correct privileges
check_permissions() {
    if [[ $EUID -eq 0 ]]; then
        print_error "This script should not be run as root"
        print_status "Run as: ./scripts/update.sh"
        exit 1
    fi

    # Check if user can sudo
    if ! sudo -n true 2>/dev/null; then
        print_warning "This script requires sudo privileges"
        sudo true
    fi
}

# Create backup
create_backup() {
    print_status "Creating backup..."

    # Create backup directory
    sudo mkdir -p $BACKUP_DIR

    # Backup timestamp
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    BACKUP_FILE="$BACKUP_DIR/backup_$TIMESTAMP.tar.gz"

    # Create backup
    sudo tar czf $BACKUP_FILE \
        --exclude='venv' \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        --exclude='.git' \
        $APP_DIR

    # Keep only last 5 backups
    ls -t $BACKUP_DIR/backup_*.tar.gz | tail -n +6 | xargs -r sudo rm

    print_success "Backup created: $BACKUP_FILE"
}

# Check for updates
check_updates() {
    print_status "Checking for updates..."

    cd $APP_DIR

    # Fetch latest changes
    git fetch origin

    # Check if updates are available
    LOCAL=$(git rev-parse HEAD)
    REMOTE=$(git rev-parse origin/main)

    if [ "$LOCAL" = "$REMOTE" ]; then
        print_success "Already up to date"
        exit 0
    else
        print_warning "Updates available"

        # Show changes
        echo ""
        echo "Changes to be applied:"
        git log --oneline HEAD..origin/main
        echo ""

        # Confirm update
        read -p "Do you want to continue with the update? (y/n) " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_warning "Update cancelled"
            exit 0
        fi
    fi
}

# Stop services
stop_services() {
    print_status "Stopping services..."

    sudo systemctl stop llama-chat

    print_success "Services stopped"
}

# Update code
update_code() {
    print_status "Updating code..."

    cd $APP_DIR

    # Pull latest changes
    git pull origin main

    # Check for merge conflicts
    if git ls-files -u | grep -q .; then
        print_error "Merge conflicts detected!"
        print_warning "Please resolve conflicts manually and re-run update"
        exit 1
    fi

    print_success "Code updated"
}

# Update dependencies
update_dependencies() {
    print_status "Updating Python dependencies..."

    cd $APP_DIR

    # Activate virtual environment
    source venv/bin/activate

    # Upgrade pip
    pip install --upgrade pip setuptools wheel

    # Update dependencies
    pip install -r requirements.txt --upgrade

    deactivate

    print_success "Dependencies updated"
}

# Update configuration
update_config() {
    print_status "Checking configuration..."

    # Check for new environment variables
    if [ -f "$APP_DIR/config/.env.example" ]; then
        # Get all variables from example
        EXAMPLE_VARS=$(grep -oP '^[A-Z_]+(?=\=)' $APP_DIR/config/.env.example | sort)

        # Get all variables from current .env
        if [ -f "$APP_DIR/.env" ]; then
            CURRENT_VARS=$(grep -oP '^[A-Z_]+(?=\=)' $APP_DIR/.env | sort)

            # Find missing variables
            MISSING_VARS=$(comm -13 <(echo "$CURRENT_VARS") <(echo "$EXAMPLE_VARS"))

            if [ ! -z "$MISSING_VARS" ]; then
                print_warning "New configuration variables detected:"
                echo "$MISSING_VARS"
                print_warning "Please update $APP_DIR/.env with new variables from config/.env.example"
            else
                print_success "Configuration is up to date"
            fi
        fi
    fi
}

# Update database (if applicable)
update_database() {
    print_status "Checking for database updates..."

    # Placeholder for database migrations
    # If you add a database later, add migration logic here

    print_success "No database updates needed"
}

# Update systemd service
update_systemd() {
    print_status "Updating systemd service..."

    # Check if service file has changed
    if [ -f "$APP_DIR/config/systemd.service.example" ]; then
        if ! diff -q /etc/systemd/system/llama-chat.service $APP_DIR/config/systemd.service.example > /dev/null; then
            print_warning "Systemd service file has changes"
            print_status "Updating service file..."

            sudo cp $APP_DIR/config/systemd.service.example /etc/systemd/system/llama-chat.service
            sudo systemctl daemon-reload

            print_success "Service file updated"
        else
            print_success "Service file is up to date"
        fi
    fi
}

# Update Nginx configuration
update_nginx() {
    print_status "Checking Nginx configuration..."

    if [ -f "$APP_DIR/config/nginx.conf.example" ]; then
        print_warning "Please manually review Nginx configuration for any changes"
        print_status "Compare: diff /etc/nginx/conf.d/llama-chat.conf $APP_DIR/config/nginx.conf.example"
    fi
}

# Start services
start_services() {
    print_status "Starting services..."

    sudo systemctl start llama-chat

    # Wait for service to be ready
    sleep 5

    # Check service status
    if systemctl is-active --quiet llama-chat; then
        print_success "Services started successfully"
    else
        print_error "Failed to start services"
        print_status "Check logs: journalctl -u llama-chat -n 50"
        exit 1
    fi
}

# Run health check
health_check() {
    print_status "Running health check..."

    # Wait a bit for service to fully start
    sleep 3

    # Check health endpoint
    if curl -s -f http://localhost:8000/health > /dev/null; then
        print_success "Health check passed"

        # Show health status
        curl -s http://localhost:8000/health | python3 -m json.tool
    else
        print_error "Health check failed"
        exit 1
    fi
}

# Clean up
cleanup() {
    print_status "Cleaning up..."

    # Remove Python cache files
    find $APP_DIR -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
    find $APP_DIR -type f -name "*.pyc" -delete 2>/dev/null || true

    # Clean old logs (older than 30 days)
    find /var/log/llama-chat -name "*.log" -mtime +30 -delete 2>/dev/null || true

    print_success "Cleanup complete"
}

# Print update summary
print_summary() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Update Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""

    # Get current version
    cd $APP_DIR
    CURRENT_VERSION=$(git describe --tags --always)

    echo -e "${BLUE}Current Version:${NC} $CURRENT_VERSION"
    echo ""
    echo -e "${BLUE}Service Status:${NC}"
    echo -e "  Llama Chat: $(systemctl is-active llama-chat)"
    echo -e "  Ollama: $(systemctl is-active ollama)"
    echo ""
    echo -e "${GREEN}Update completed successfully!${NC}"
}

# Main update flow
main() {
    echo ""
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${BLUE}  Llama Chat Service Updater${NC}"
    echo -e "${BLUE}=====================================${NC}"
    echo ""

    check_permissions
    create_backup
    check_updates
    stop_services
    update_code
    update_dependencies
    update_config
    update_database
    update_systemd
    update_nginx
    start_services
    health_check
    cleanup
    print_summary
}

# Error handler
trap 'error_handler $? $LINENO' ERR

error_handler() {
    print_error "Error occurred at line $2 with exit code $1"
    print_warning "Rolling back..."

    # Attempt to restart services
    sudo systemctl start llama-chat || true

    print_status "Check logs for more information:"
    echo "  journalctl -u llama-chat -n 50"
    exit 1
}

# Run main function
main "$@"