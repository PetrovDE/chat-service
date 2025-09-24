#!/bin/bash

# Setup systemd services for Llama Chat Service

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}This script must be run as root${NC}"
   exit 1
fi

echo -e "${BLUE}Setting up systemd services...${NC}"

# Variables
APP_DIR="/opt/llama-chat"
SERVICE_FILE="/etc/systemd/system/llama-chat.service"

# Copy service file
if [ -f "$APP_DIR/config/systemd.service.example" ]; then
    cp "$APP_DIR/config/systemd.service.example" "$SERVICE_FILE"
    echo -e "${GREEN}✓ Service file copied${NC}"
else
    echo -e "${RED}✗ Service file not found${NC}"
    exit 1
fi

# Reload systemd
systemctl daemon-reload
echo -e "${GREEN}✓ Systemd reloaded${NC}"

# Enable service
systemctl enable llama-chat.service
echo -e "${GREEN}✓ Service enabled for autostart${NC}"

# Start service
systemctl start llama-chat.service
echo -e "${GREEN}✓ Service started${NC}"

# Check status
if systemctl is-active --quiet llama-chat; then
    echo -e "${GREEN}✓ Service is running${NC}"
else
    echo -e "${RED}✗ Service failed to start${NC}"
    journalctl -u llama-chat -n 20
    exit 1
fi

echo -e "${GREEN}Setup complete!${NC}"