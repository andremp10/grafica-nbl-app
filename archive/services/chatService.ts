/**
 * Serviço de Chat IA - Integração com Webhook N8N
 * Configurado via VITE_WEBHOOK_URL
 */

const WEBHOOK_URL = import.meta.env.VITE_WEBHOOK_URL || '';

export const chatWithIA = async (
  message: string,
  history: { role: 'user' | 'model', parts: { text: string }[] }[]
) => {
  if (!WEBHOOK_URL) {
    throw new Error('VITE_WEBHOOK_URL não configurada. Configure no arquivo .env.local');
  }

  try {
    const response = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        message,
        history,
        timestamp: new Date().toISOString()
      }),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.error || `Erro ${response.status}: Falha na comunicação com Agente`);
    }

    const data = await response.json();

    // Suporta diferentes formatos de resposta
    return data.response || data.message || data.output || data.text || JSON.stringify(data);

  } catch (error) {
    console.error("Erro ao chamar Agente N8N:", error);
    throw error;
  }
};
