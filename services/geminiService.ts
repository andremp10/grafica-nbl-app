
import { GoogleGenAI } from "@google/genai";
import { MOCK_ORDERS } from "../mockData";

export const chatWithIA = async (message: string, history: { role: 'user' | 'model', parts: { text: string }[] }[]) => {
  const apiKey = process.env.API_KEY;
  if (!apiKey) throw new Error("API Key not found");

  const ai = new GoogleGenAI({ apiKey });
  
  // Preparing context from mock database
  const context = `Você é um assistente administrativo de uma gráfica. 
  Abaixo está o banco de dados atual de pedidos em formato JSON:
  ${JSON.stringify(MOCK_ORDERS)}

  Instruções:
  - Responda de forma curta e profissional.
  - Utilize as informações acima para responder perguntas sobre prazos, clientes e produtos.
  - Se perguntarem sobre valores, some-os se necessário.
  - O tom deve ser amigável mas focado em dados.`;

  const response = await ai.models.generateContent({
    model: 'gemini-3-flash-preview',
    contents: [
      { role: 'user', parts: [{ text: context }] },
      ...history,
      { role: 'user', parts: [{ text: message }] }
    ],
    config: {
      temperature: 0.7,
      maxOutputTokens: 500,
    }
  });

  return response.text;
};
