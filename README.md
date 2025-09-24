# Llama Chat Service

<div align="center">

![Version](https://img.shields.io/badge/version-2.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.11+-green.svg)
![License](https://img.shields.io/badge/license-MIT-purple.svg)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20RedOS8-lightgrey.svg)

**Корпоративный чат-сервис с поддержкой локальных и облачных LLM моделей**

[Features](#features) • [Installation](#installation) • [Documentation](#documentation) • [API](#api) • [Contributing](#contributing)

</div>

## 🌟 Features

- 🦙 **Локальные модели через Ollama** - Полная приватность данных
- ☁️ **Корпоративные API** - Поддержка OpenAI, Claude, Custom endpoints
- 📊 **Анализ файлов** - Excel, CSV, JSON, TXT с интеллектуальным разбором
- 🔄 **Горячее переключение** - Между источниками моделей без перезапуска
- 🚀 **Streaming ответы** - Реальное время генерации через SSE
- 🔒 **Безопасность** - Изоляция данных, systemd hardening, HTTPS
- 📈 **Мониторинг** - Встроенные health checks и метрики

## 🛠 Tech Stack

- **Backend**: FastAPI, LangChain, Ollama
- **Frontend**: Vanilla JS, Modern CSS
- **Deployment**: systemd, Nginx, RedOS8
- **Models**: Llama 3.1 8B (default), GPT-4, Claude (optional)

## 📋 Requirements

### System Requirements
- **OS**: RedOS8, RHEL 8+, CentOS 8+, Ubuntu 20.04+
- **Python**: 3.11+
- **RAM**: 16GB minimum (8GB for model + 8GB system)
- **Storage**: 20GB+ free space
- **CPU**: 4+ cores recommended

### Software Dependencies
- Ollama (for local models)
- Nginx (reverse proxy)
- systemd (service management)

## 🚀 Quick Start

### Local Development

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/llama-chat-service.git
cd llama-chat-service

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Setup Ollama
curl -fsSL https://ollama.ai/install.sh | sh
ollama pull llama3.1:8b

# Configure environment
cp config/.env.example .env
# Edit .env with your settings

# Run development server
python app/main.py
```

Open http://localhost:8000 in your browser.

## 📦 Production Deployment

### 1. Automated Installation

```bash
# On your RedOS8/RHEL server
git clone https://github.com/YOUR_USERNAME/llama-chat-service.git
cd llama-chat-service
chmod +x scripts/*.sh

# Run installation script
./scripts/install.sh

# Setup systemd service
sudo ./scripts/setup_systemd.sh

# Configure Nginx (edit domain in config)
sudo cp config/nginx.conf.example /etc/nginx/conf.d/llama-chat.conf
sudo nano /etc/nginx/conf.d/llama-chat.conf
sudo systemctl restart nginx
```

### 2. Manual Installation

See [Deployment Guide](docs/DEPLOYMENT.md) for detailed instructions.

## 🔧 Configuration

### Environment Variables

```bash
# Ollama Configuration
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=llama3.1:8b

# Server Configuration  
HOST=0.0.0.0
PORT=8000
WORKERS=4

# API Configuration (optional)
CORPORATE_API_URL=https://api.openai.com/v1/chat/completions
CORPORATE_API_KEY=sk-...
CORPORATE_API_TYPE=openai
CORPORATE_MODEL_NAME=gpt-4

# Security
SECRET_KEY=your-secret-key-here
ALLOWED_ORIGINS=http://localhost:8000,https://your-domain.com
```

### Model Sources

#### Local Models (Ollama)
```bash
# List available models
ollama list

# Pull new model
ollama pull mistral:7b
ollama pull codellama:13b
```

#### Corporate API
Configure through UI or API:
```json
{
  "source": "api",
  "api_config": {
    "api_url": "https://api.openai.com/v1/chat/completions",
    "api_key": "sk-...",
    "model_name": "gpt-4",
    "api_type": "openai"
  }
}
```

## 📖 API Documentation

### Chat Endpoints

#### Send Message
```bash
POST /chat
Content-Type: application/json

{
  "message": "Hello, how are you?",
  "temperature": 0.7,
  "max_tokens": 1000
}
```

#### Stream Response
```bash
POST /chat/stream
Content-Type: application/json

{
  "message": "Tell me a story",
  "temperature": 0.8
}
```

### File Analysis

#### Upload File
```bash
POST /upload
Content-Type: multipart/form-data

file: <binary>
```

#### Analyze File
```bash
POST /analyze-file
Content-Type: application/json

{
  "content": "file content...",
  "filename": "data.xlsx",
  "analysis_type": "summary",
  "custom_prompt": null
}
```

### Model Management

#### Switch Model Source
```bash
POST /api/source
Content-Type: application/json

{
  "source": "local" | "api",
  "api_config": {...}  # if source="api"
}
```

#### List Local Models
```bash
GET /api/models/local
```

#### Health Check
```bash
GET /health
```

Full API documentation available at `/docs` when service is running.

## 🗂 Project Structure

```
llama-chat-service/
├── app/                    # Application code
│   ├── main.py            # FastAPI application
│   ├── models.py          # Pydantic models
│   ├── llm_service.py     # Ollama integration
│   ├── llm_manager.py     # Model source manager
│   ├── api_llm_service.py # Corporate API integration
│   └── static/            # Frontend files
│       ├── index.html
│       ├── styles.css
│       └── js/
├── config/                 # Configuration examples
│   ├── nginx.conf.example
│   ├── systemd.service.example
│   └── .env.example
├── scripts/               # Automation scripts
│   ├── install.sh
│   ├── setup_systemd.sh
│   └── update.sh
├── docs/                  # Documentation
├── tests/                 # Test suite
├── requirements.txt       # Python dependencies
└── README.md
```

## 🔒 Security Features

- **Data Privacy**: All data processed locally when using Ollama
- **HTTPS**: Enforced in production with SSL/TLS
- **systemd Hardening**: PrivateTmp, NoNewPrivileges
- **Input Validation**: Pydantic models for all endpoints
- **CORS**: Configurable allowed origins
- **File Upload**: Type and size restrictions
- **API Keys**: Secure storage, never logged

## 🐛 Troubleshooting

### Common Issues

#### Ollama Connection Error
```bash
# Check Ollama service
systemctl status ollama
ollama list

# Restart Ollama
sudo systemctl restart ollama
```

#### 502 Bad Gateway
```bash
# Check app service
systemctl status llama-chat
journalctl -u llama-chat -n 50

# Check port binding
ss -tlnp | grep :8000
```

#### Model Not Found
```bash
# Pull required model
ollama pull llama3.1:8b

# Verify installation
ollama list
```

See [Troubleshooting Guide](docs/TROUBLESHOOTING.md) for more solutions.

## 📊 Monitoring

### Service Health
```bash
# Check all services
./scripts/monitor.sh

# View logs
journalctl -u llama-chat -f
tail -f /var/log/llama-chat/service.log
```

### Performance Metrics
```bash
# CPU and Memory usage
htop -p $(pgrep -f "uvicorn")

# API response times
curl -w "@curl-format.txt" -o /dev/null -s http://localhost:8000/health
```

## 🔄 Updates

### Automatic Update
```bash
cd /opt/llama-chat
./scripts/update.sh
```

### Manual Update
```bash
# Stop service
sudo systemctl stop llama-chat

# Pull changes
git pull origin main

# Update dependencies
source venv/bin/activate
pip install -r requirements.txt --upgrade

# Restart service
sudo systemctl start llama-chat
```

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](docs/CONTRIBUTING.md) for details.

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

## 📝 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Ollama](https://ollama.ai) - Local model runtime
- [FastAPI](https://fastapi.tiangolo.com) - Modern web framework
- [LangChain](https://langchain.com) - LLM orchestration
- [Meta AI](https://ai.meta.com) - Llama models

## 📮 Support

- **Issues**: [GitHub Issues](https://github.com/YOUR_USERNAME/llama-chat-service/issues)
- **Discussions**: [GitHub Discussions](https://github.com/YOUR_USERNAME/llama-chat-service/discussions)
- **Wiki**: [Documentation Wiki](https://github.com/YOUR_USERNAME/llama-chat-service/wiki)

## 🚦 Status

- ✅ Production Ready
- ✅ Active Development
- ✅ Security Updates

---

<div align="center">
Made with ❤️ for enterprise AI deployment
</div>