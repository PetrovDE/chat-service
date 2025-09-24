#!/bin/bash

# Backup script for Llama Chat Service
# Creates compressed backups of configuration, data, and logs

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
APP_DIR="/opt/llama-chat"
DATA_DIR="/var/lib/llama-chat"
LOG_DIR="/var/log/llama-chat"
BACKUP_DIR="/var/backups/llama-chat"
OLLAMA_DIR="/var/lib/ollama"
RETENTION_DAYS=30

# Backup name with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="backup_${TIMESTAMP}"

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
    if [ ! -w "$BACKUP_DIR" ] && [ $EUID -ne 0 ]; then
        print_error "Insufficient permissions. Run with sudo or as root"
        exit 1
    fi
}

# Create backup directory
create_backup_dir() {
    if [ ! -d "$BACKUP_DIR" ]; then
        print_status "Creating backup directory..."
        mkdir -p "$BACKUP_DIR"
        print_success "Backup directory created"
    fi

    # Create subdirectory for this backup
    BACKUP_PATH="$BACKUP_DIR/$BACKUP_NAME"
    mkdir -p "$BACKUP_PATH"
}

# Backup configuration files
backup_config() {
    print_status "Backing up configuration files..."

    # Application config
    if [ -f "$APP_DIR/.env" ]; then
        cp "$APP_DIR/.env" "$BACKUP_PATH/env.backup"
    fi

    # Nginx config
    if [ -f "/etc/nginx/conf.d/llama-chat.conf" ]; then
        cp "/etc/nginx/conf.d/llama-chat.conf" "$BACKUP_PATH/nginx.conf.backup"
    fi

    # Systemd service
    if [ -f "/etc/systemd/system/llama-chat.service" ]; then
        cp "/etc/systemd/system/llama-chat.service" "$BACKUP_PATH/systemd.service.backup"
    fi

    print_success "Configuration backed up"
}

# Backup application data
backup_data() {
    print_status "Backing up application data..."

    # Uploads directory
    if [ -d "$DATA_DIR/uploads" ]; then
        tar czf "$BACKUP_PATH/uploads.tar.gz" -C "$DATA_DIR" uploads 2>/dev/null || true
    fi

    # Database (if exists)
    if [ -f "$DATA_DIR/chat.db" ]; then
        cp "$DATA_DIR/chat.db" "$BACKUP_PATH/chat.db.backup"
    fi

    print_success "Application data backed up"
}

# Backup logs
backup_logs() {
    print_status "Backing up logs..."

    if [ -d "$LOG_DIR" ]; then
        tar czf "$BACKUP_PATH/logs.tar.gz" -C "$LOG_DIR" . 2>/dev/null || true
    fi

    print_success "Logs backed up"
}

# Backup Ollama models list
backup_ollama_info() {
    print_status "Backing up Ollama model information..."

    # Get list of models
    if command -v ollama &> /dev/null; then
        ollama list > "$BACKUP_PATH/ollama_models.txt" 2>/dev/null || true
    fi

    print_success "Ollama information backed up"
}

# Get application version
backup_version_info() {
    print_status "Saving version information..."

    cd "$APP_DIR"

    # Git information
    if [ -d ".git" ]; then
        git log -1 --format="%H %s" > "$BACKUP_PATH/version.txt"
        git status >> "$BACKUP_PATH/version.txt"
    fi

    # Python packages
    if [ -f "venv/bin/pip" ]; then
        venv/bin/pip freeze > "$BACKUP_PATH/requirements_frozen.txt"
    fi

    print_success "Version information saved"
}

# Create compressed archive
create_archive() {
    print_status "Creating compressed archive..."

    cd "$BACKUP_DIR"
    tar czf "${BACKUP_NAME}.tar.gz" "$BACKUP_NAME"

    # Remove uncompressed backup
    rm -rf "$BACKUP_NAME"

    # Calculate size
    BACKUP_SIZE=$(du -h "${BACKUP_NAME}.tar.gz" | cut -f1)

    print_success "Archive created: ${BACKUP_NAME}.tar.gz (${BACKUP_SIZE})"
}

# Clean old backups
cleanup_old_backups() {
    print_status "Cleaning old backups..."

    # Find and remove backups older than retention period
    find "$BACKUP_DIR" -name "backup_*.tar.gz" -mtime +$RETENTION_DAYS -delete

    # Count remaining backups
    BACKUP_COUNT=$(ls -1 "$BACKUP_DIR"/backup_*.tar.gz 2>/dev/null | wc -l)

    print_success "Cleanup complete. ${BACKUP_COUNT} backups retained"
}

# Upload to remote storage (optional)
upload_to_remote() {
    if [ ! -z "$REMOTE_BACKUP_PATH" ]; then
        print_status "Uploading to remote storage..."

        # Example: rsync to remote server
        # rsync -av "${BACKUP_DIR}/${BACKUP_NAME}.tar.gz" "$REMOTE_BACKUP_PATH/"

        # Example: AWS S3
        # aws s3 cp "${BACKUP_DIR}/${BACKUP_NAME}.tar.gz" "s3://your-bucket/backups/"

        print_warning "Remote backup not configured"
    fi
}

# Backup database with proper dump
backup_database() {
    print_status "Backing up database..."

    # PostgreSQL
    if [ ! -z "$DATABASE_URL" ] && [[ "$DATABASE_URL" == postgres* ]]; then
        pg_dump "$DATABASE_URL" > "$BACKUP_PATH/database.sql" 2>/dev/null || true
    fi

    # SQLite
    if [ -f "$DATA_DIR/chat.db" ]; then
        sqlite3 "$DATA_DIR/chat.db" ".backup '$BACKUP_PATH/chat.db.backup'" 2>/dev/null || true
    fi

    print_success "Database backed up"
}

# Verify backup
verify_backup() {
    print_status "Verifying backup..."

    if [ -f "${BACKUP_DIR}/${BACKUP_NAME}.tar.gz" ]; then
        tar tzf "${BACKUP_DIR}/${BACKUP_NAME}.tar.gz" > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            print_success "Backup verified successfully"
        else
            print_error "Backup verification failed"
            exit 1
        fi
    else
        print_error "Backup file not found"
        exit 1
    fi
}

# Send notification
send_notification() {
    local status=$1
    local message=$2

    # Email notification (if configured)
    if [ ! -z "$EMAIL_TO" ]; then
        echo "$message" | mail -s "Llama Chat Backup - $status" "$EMAIL_TO" 2>/dev/null || true
    fi

    # Slack notification (if configured)
    if [ ! -z "$SLACK_WEBHOOK_URL" ]; then
        curl -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"Llama Chat Backup - $status: $message\"}" \
            "$SLACK_WEBHOOK_URL" 2>/dev/null || true
    fi
}

# Main backup process
main() {
    echo ""
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${BLUE}  Llama Chat Service Backup${NC}"
    echo -e "${BLUE}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${BLUE}=====================================${NC}"
    echo ""

    # Start timer
    START_TIME=$(date +%s)

    # Run backup steps
    check_permissions
    create_backup_dir
    backup_config
    backup_data
    backup_database
    backup_logs
    backup_ollama_info
    backup_version_info
    create_archive
    verify_backup
    cleanup_old_backups
    upload_to_remote

    # Calculate duration
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    # Print summary
    echo ""
    echo -e "${GREEN}=====================================${NC}"
    echo -e "${GREEN}  Backup Completed Successfully${NC}"
    echo -e "${GREEN}=====================================${NC}"
    echo -e "Backup file: ${BLUE}${BACKUP_DIR}/${BACKUP_NAME}.tar.gz${NC}"
    echo -e "Duration: ${BLUE}${DURATION} seconds${NC}"
    echo ""

    # Send notification
    send_notification "SUCCESS" "Backup completed: ${BACKUP_NAME}.tar.gz in ${DURATION}s"
}

# Restore function (separate script or option)
restore_backup() {
    local RESTORE_FILE=$1

    if [ -z "$RESTORE_FILE" ]; then
        print_error "Usage: $0 --restore <backup_file>"
        exit 1
    fi

    if [ ! -f "$RESTORE_FILE" ]; then
        print_error "Backup file not found: $RESTORE_FILE"
        exit 1
    fi

    print_warning "This will restore from backup: $RESTORE_FILE"
    read -p "Are you sure? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        print_warning "Restore cancelled"
        exit 0
    fi

    # Stop services
    print_status "Stopping services..."
    systemctl stop llama-chat || true

    # Extract backup
    print_status "Extracting backup..."
    TEMP_DIR="/tmp/llama-restore-$$"
    mkdir -p "$TEMP_DIR"
    tar xzf "$RESTORE_FILE" -C "$TEMP_DIR"

    # Find backup directory
    BACKUP_CONTENT=$(ls "$TEMP_DIR")

    # Restore files
    print_status "Restoring configuration..."
    if [ -f "$TEMP_DIR/$BACKUP_CONTENT/env.backup" ]; then
        cp "$TEMP_DIR/$BACKUP_CONTENT/env.backup" "$APP_DIR/.env"
    fi

    print_status "Restoring data..."
    if [ -f "$TEMP_DIR/$BACKUP_CONTENT/uploads.tar.gz" ]; then
        tar xzf "$TEMP_DIR/$BACKUP_CONTENT/uploads.tar.gz" -C "$DATA_DIR"
    fi

    # Cleanup
    rm -rf "$TEMP_DIR"

    # Start services
    print_status "Starting services..."
    systemctl start llama-chat

    print_success "Restore completed successfully"
}

# Parse command line arguments
case "${1:-}" in
    --restore)
        restore_backup "$2"
        ;;
    --help)
        echo "Usage: $0 [OPTIONS]"
        echo "Options:"
        echo "  --restore <file>  Restore from backup file"
        echo "  --help           Show this help message"
        exit 0
        ;;
    *)
        main
        ;;
esac