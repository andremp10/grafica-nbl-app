export const chatWithIA = async (message: string, history: { role: 'user' | 'model', parts: { text: string }[] }[]) => {
  try {
    const response = await fetch('/.netlify/functions/chat', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ message, history }),
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || 'Erro ao comunicar com o assistente.');
    }

    const data = await response.json();
    return data.response;
  } catch (error) {
    console.error("Erro ao chamar serviço de IA:", error);
    throw error;
  }
};
