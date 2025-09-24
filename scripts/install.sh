#!/bin/bash

# Llama Chat Service Installation Script
# Compatible with RedOS8, RHEL8, CentOS8

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_DIR="/opt/llama-chat"
APP_USER="llama-chat"
LOG_DIR="/var/log/llama-chat"
DATA_DIR="/var/lib/llama-chat"
PYTHON_VERSION="3.11"

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

# Check if running with sudo
check_sudo() {
    if [[ $EUID -ne 0 ]]; then
        print_error "This script must be run with sudo privileges"
        exit 1
    fi
}

# Detect OS
detect_os() {
    if [ -f /etc/redos-release ]; then
        OS="RedOS"
        OS_VERSION=$(cat /etc/redos-release | grep -oP '\d+' | head -1)
    elif [ -f /etc/redhat-release ]; then
        if grep -q "Red Hat" /etc/redhat-release; then
            OS="RHEL"
        else
            OS="CentOS"
        fi
        OS_VERSION=$(rpm -E %{rhel})
    elif [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        OS_VERSION=$VERSION_ID
    else
        print_error "Unsupported operating system"
        exit 1
    fi

    print_status "Detected OS: $OS $OS_VERSION"
}

# Install system dependencies
install_dependencies() {
    print_status "Installing system dependencies..."

    # Enable EPEL repository if needed
    if [[ "$OS" == "RedOS" || "$OS" == "RHEL" || "$OS" == "CentOS" ]]; then
        dnf install -y epel-release || true
        dnf config-manager --set-enabled powertools || \
        dnf config-manager --set-enabled crb || \
        dnf config-manager --set-enabled PowerTools || true
    fi

    # Install packages
    dnf install -y \
        python${PYTHON_VERSION} \
        python${PYTHON_VERSION}-pip \
        python${PYTHON_VERSION}-devel \
        git \
        nginx \
        gcc \
        gcc-c++ \
        make \
        openssl-devel \
        libffi-devel \
        curl \
        wget \
        tar \
        systemd-devel

    print_success "System dependencies installed"
}

# Create application user
create_user() {
    print_status "Creating application user..."

    if id "$APP_USER" &>/dev/null; then
        print_warning "User $APP_USER already exists"
    else
        useradd -r -s /bin/false -d $APP_DIR $APP_USER
        print_success "User $APP_USER created"
    fi
}

# Setup directory structure
setup_directories() {
    print_status "Setting up directory structure..."

    # Create directories
    mkdir -p $APP_DIR
    mkdir -p $LOG_DIR
    mkdir -p $DATA_DIR/uploads
    mkdir -p /etc/llama-chat

    # Set permissions
    chown -R $APP_USER:$APP_USER $APP_DIR
    chown -R $APP_USER:$APP_USER $LOG_DIR
    chown -R $APP_USER:$APP_USER $DATA_DIR

    # Set proper permissions
    chmod 755 $APP_DIR
    chmod 755 $LOG_DIR
    chmod 755 $DATA_DIR

    print_success "Directories created and permissions set"
}

# Clone or update repository
setup_repository() {
    print_status "Setting up repository..."

    if [ -d "$APP_DIR/.git" ]; then
        print_status "Repository exists, pulling latest changes..."
        cd $APP_DIR
        sudo -u $APP_USER git pull origin main
    else
        print_status "Cloning repository..."
        cd /opt
        sudo -u $APP_USER git clone https://github.com/YOUR_USERNAME/llama-chat-service.git llama-chat
    fi

    print_success "Repository setup complete"
}

# Setup Python virtual environment
setup_python_env() {
    print_status "Setting up Python virtual environment..."

    cd $APP_DIR

    # Create virtual environment
    sudo -u $APP_USER python${PYTHON_VERSION} -m venv venv

    # Upgrade pip
    sudo -u $APP_USER bash -c "source venv/bin/activate && pip install --upgrade pip setuptools wheel"

    # Install requirements
    sudo -u $APP_USER bash -c "source venv/bin/activate && pip install -r requirements.txt"

    print_success "Python environment setup complete"
}

# Install Ollama
install_ollama() {
    print_status "Installing Ollama..."

    if command -v ollama &> /dev/null; then
        print_warning "Ollama is already installed"
    else
        curl -fsSL https://ollama.ai/install.sh | sh
        print_success "Ollama installed"
    fi

    # Create Ollama user if not exists
    if ! id "ollama" &>/dev/null; then
        useradd -r -s /bin/false ollama
    fi

    # Setup Ollama systemd service
    cat > /etc/systemd/system/ollama.service <<EOF
[Unit]
Description=Ollama Service
After=network.target

[Service]
Type=simple
User=ollama
Group=ollama
ExecStart=/usr/local/bin/ollama serve
Restart=always
RestartSec=10
Environment="OLLAMA_HOST=0.0.0.0:11434"
Environment="OLLAMA_MODELS=/var/lib/ollama/models"

[Install]
WantedBy=multi-user.target
EOF

    # Create Ollama directories
    mkdir -p /var/lib/ollama/models
    chown -R ollama:ollama /var/lib/ollama

    # Start Ollama service
    systemctl daemon-reload
    systemctl enable ollama
    systemctl start ollama

    print_success "Ollama service configured and started"

    # Wait for Ollama to be ready
    print_status "Waiting for Ollama to be ready..."
    sleep 5

    # Pull default model
    print_status "Pulling Llama 3.1 8B model (this may take a while)..."
    sudo -u ollama ollama pull llama3.1:8b

    print_success "Default model downloaded"
}

# Setup configuration files
setup_config() {
    print_status "Setting up configuration files..."

    # Copy environment file
    if [ ! -f "$APP_DIR/.env" ]; then
        cp $APP_DIR/config/.env.example $APP_DIR/.env
        chown $APP_USER:$APP_USER $APP_DIR/.env
        chmod 600 $APP_DIR/.env

        # Generate secret key
        SECRET_KEY=$(openssl rand -hex 32)
        sed -i "s/your-secret-key-here/$SECRET_KEY/" $APP_DIR/.env

        print_warning "Please edit $APP_DIR/.env with your configuration"
    else
        print_warning ".env file already exists, skipping..."
    fi

    print_success "Configuration files setup complete"
}

# Setup systemd service
setup_systemd() {
    print_status "Setting up systemd service..."

    cat > /etc/systemd/system/llama-chat.service <<EOF
[Unit]
Description=Llama Chat Service
After=network.target ollama.service
Requires=ollama.service

[Service]
Type=simple
User=$APP_USER
Group=$APP_USER
WorkingDirectory=$APP_DIR
Environment="PATH=$APP_DIR/venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=$APP_DIR/venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
Restart=always
RestartSec=10

# Security
PrivateTmp=true
NoNewPrivileges=true

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Logging
StandardOutput=append:$LOG_DIR/service.log
StandardError=append:$LOG_DIR/error.log

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable llama-chat

    print_success "Systemd service configured"
}

# Setup Nginx
setup_nginx() {
    print_status "Setting up Nginx configuration..."

    # Backup default config
    if [ -f /etc/nginx/nginx.conf ]; then
        cp /etc/nginx/nginx.conf /etc/nginx/nginx.conf.backup
    fi

    # Copy Nginx config
    if [ ! -f /etc/nginx/conf.d/llama-chat.conf ]; then
        cp $APP_DIR/config/nginx.conf.example /etc/nginx/conf.d/llama-chat.conf

        # Get hostname
        HOSTNAME=$(hostname -f)
        sed -i "s/YOUR_DOMAIN/$HOSTNAME/g" /etc/nginx/conf.d/llama-chat.conf

        print_warning "Please edit /etc/nginx/conf.d/llama-chat.conf with your domain"
    fi

    # Create SSL directory
    mkdir -p /etc/nginx/ssl

    # Generate self-signed certificate for testing
    if [ ! -f /etc/nginx/ssl/cert.pem ]; then
        print_status "Generating self-signed SSL certificate..."
        openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
            -keyout /etc/nginx/ssl/key.pem \
            -out /etc/nginx/ssl/cert.pem \
            -subj "/C=RU/ST=Moscow/L=Moscow/O=Company/CN=$HOSTNAME"
    fi

    # Test Nginx configuration
    nginx -t

    # Enable and start Nginx
    systemctl enable nginx
    systemctl restart nginx

    print_success "Nginx configured"
}

# Setup firewall
setup_firewall() {
    print_status "Configuring firewall..."

    # Check if firewalld is installed and running
    if systemctl is-active --quiet firewalld; then
        firewall-cmd --permanent --add-service=http
        firewall-cmd --permanent --add-service=https
        firewall-cmd --reload
        print_success "Firewall rules added"
    else
        print_warning "Firewalld is not running, skipping firewall configuration"
    fi
}

# Setup log rotation
setup_logrotate() {
    print_status "Setting up log rotation..."

    cat > /etc/logrotate.d/llama-chat <<EOF
$LOG_DIR/*.log {
    daily
    missingok
    rotate 14
    compress
    delaycompress
    notifempty
    create 640 $APP_USER $APP_USER
    sharedscripts
    postrotate
        systemctl reload llama-chat > /dev/null 2>&1 || true
    endscript
}
EOF

    print_success "Log rotation configured"
}

# Final steps
final_steps() {
    print_status "Running final configuration steps..."

    # Set SELinux context if enabled
    if command -v getenforce &> /dev/null && [ "$(getenforce)" != "Disabled" ]; then
        print_status "Setting SELinux contexts..."
        setsebool -P httpd_can_network_connect 1
        semanage fcontext -a -t httpd_sys_content_t "$APP_DIR/app/static(/.*)?"
        restorecon -Rv $APP_DIR/app/static
    fi

    # Start services
    print_status "Starting services..."
    systemctl start ollama
    sleep 5
    systemctl start llama-chat
    systemctl restart nginx

    print_success "Services started"
}

# Print installation summary
print_summary() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Llama Chat Service Installation Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "${BLUE}Service Information:${NC}"
    echo -e "  Web Interface: ${GREEN}http://$(hostname -I | awk '{print $1}')${NC}"
    echo -e "  API Docs: ${GREEN}http://$(hostname -I | awk '{print $1}')/docs${NC}"
    echo ""
    echo -e "${BLUE}Service Status:${NC}"
    echo -e "  Ollama: $(systemctl is-active ollama)"
    echo -e "  Llama Chat: $(systemctl is-active llama-chat)"
    echo -e "  Nginx: $(systemctl is-active nginx)"
    echo ""
    echo -e "${YELLOW}Next Steps:${NC}"
    echo -e "  1. Edit configuration: ${GREEN}nano $APP_DIR/.env${NC}"
    echo -e "  2. Configure domain in Nginx: ${GREEN}nano /etc/nginx/conf.d/llama-chat.conf${NC}"
    echo -e "  3. Setup SSL certificate for production"
    echo -e "  4. Check service logs: ${GREEN}journalctl -u llama-chat -f${NC}"
    echo ""
    echo -e "${BLUE}Useful Commands:${NC}"
    echo -e "  Start service: ${GREEN}systemctl start llama-chat${NC}"
    echo -e "  Stop service: ${GREEN}systemctl stop llama-chat${NC}"
    echo -e "  View logs: ${GREEN}journalctl -u llama-chat -f${NC}"
    echo -e "  Check status: ${GREEN}systemctl status llama-chat${NC}"
    echo ""
}

# Main installation flow
main() {
    echo ""
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${BLUE}  Llama Chat Service Installer${NC}"
    echo -e "${BLUE}=====================================${NC}"
    echo ""

    check_sudo
    detect_os
    install_dependencies
    create_user
    setup_directories
    setup_repository
    setup_python_env
    install_ollama
    setup_config
    setup_systemd
    setup_nginx
    setup_firewall
    setup_logrotate
    final_steps
    print_summary
}

# Run main function
main "$@"