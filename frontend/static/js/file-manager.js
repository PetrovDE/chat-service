import settingsManager from './settings-manager.js';

class FileManager {
  constructor() {
    this.userId = null;
    this.currentMode = settingsManager.mode;
    this.currentModel = settingsManager.model;
    settingsManager.onChange((mode, model) => {
      this.currentMode = mode;
      this.currentModel = model;
    });
  }

  async uploadAndProcess(file) {
    // Передаем в backend mode/model для корректной цепочки RAG
    const formData = new FormData();
    formData.append('file', file);
    formData.append('user_id', this.userId);
    formData.append('mode', this.currentMode);
    formData.append('model', this.currentModel);

    const resp = await fetch('/api/files/upload', {
      method: 'POST',
      body: formData,
    });

    if (!resp.ok) {
      throw new Error('Failed to upload file');
    }

    const result = await resp.json();

    // Асинхронно запускаем обработку файла (mode/model учитывается сервером)
    await fetch('/api/files/process', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        filepath: result.filepath,
        user_id: this.userId,
        mode: this.currentMode,
        model: this.currentModel,
      }),
    });

    return result;
  }
}

const fileManager = new FileManager();
export default fileManager;
