/**
 * Serviço de Chat IA - Suporta Netlify Function OU Webhook Externo
 * Configure VITE_WEBHOOK_URL para usar um agente de IA externo
 */

const WEBHOOK_URL = import.meta.env.VITE_WEBHOOK_URL || '';
const NETLIFY_CHAT_URL = '/.netlify/functions/chat';

export const chatWithIA = async (
  message: string,
  history: { role: 'user' | 'model', parts: { text: string }[] }[]
) => {
  // Se tiver webhook externo configurado, usa ele
  const endpoint = WEBHOOK_URL || NETLIFY_CHAT_URL;

  try {
    const response = await fetch(endpoint, {
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
      throw new Error(errorData.error || `Erro ${response.status}: Falha na comunicação`);
    }

    const data = await response.json();

    // Suporta diferentes formatos de resposta
    return data.response || data.message || data.text || JSON.stringify(data);

  } catch (error) {
    console.error("Erro ao chamar serviço de IA:", error);
    throw error;
  }
};
