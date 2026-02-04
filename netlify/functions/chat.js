import { GoogleGenAI } from "@google/genai";

const MOCK_DB = {
  orders: [
    { id: '101', client: 'Padaria Silva', product: 'Panfletos 5000un', quantity: 5000, status: 'production', dueDate: '2023-10-27', price: 450.00, priority: 'Normal' },
    { id: '102', client: 'Tech Solutions', product: 'Cartões de Visita Verniz Localizado', quantity: 1000, status: 'production', dueDate: '2023-10-27', price: 180.00, priority: 'Alta' },
    { id: '103', client: 'Dra. Ana Paula', product: 'Receituários 10 blocos', quantity: 500, status: 'tomorrow', dueDate: '2023-10-28', price: 120.00, priority: 'Normal' },
    { id: '104', client: 'Restaurante Gourmet', product: 'Cardápios PVC', quantity: 20, status: 'tomorrow', dueDate: '2023-10-28', price: 850.00, priority: 'Alta' },
    { id: '105', client: 'Evento Rock In Rio', product: 'Banners Lona 2x1m', quantity: 5, status: 'next_7_days', dueDate: '2023-11-02', price: 1200.00, priority: 'Urgente' },
    { id: '106', client: 'Loja de Roupas Chic', product: 'Sacolas Personalizadas', quantity: 200, status: 'next_7_days', dueDate: '2023-11-04', price: 600.00, priority: 'Normal' },
    { id: '107', client: 'Escola ABC', product: 'Apostilas encadernadas', quantity: 50, status: 'production', dueDate: '2023-10-27', price: 320.00, priority: 'Baixa' },
    { id: '108', client: 'Construtora Forte', product: 'Placas de Sinalização', quantity: 30, status: 'production', dueDate: '2023-10-29', price: 1500.00, priority: 'Normal' },
    { id: '109', client: 'Buffet Alegria', product: 'Convites de Casamento Luxo', quantity: 150, status: 'next_7_days', dueDate: '2023-11-05', price: 890.00, priority: 'Alta' },
  ],
  inventory: [
    { item: 'Papel Couché 150g', quantity: '5000 fls', status: 'OK' },
    { item: 'Papel Supremo 300g', quantity: '200 fls', status: 'Baixo' },
    { item: 'Tinta Ciano (Offset)', quantity: '2 Latas', status: 'Crítico' },
    { item: 'Lona Vinílica', quantity: '3 Rolos', status: 'OK' }
  ],
  financial_summary: {
    daily_revenue: 3450.00,
    monthly_revenue: 89200.00,
    pending_payments: 4500.00
  },
  productionFlow: [
    { stage: 'Pré-Impressão', count: 12, percent: 85, detail: 'Aprovação de arte pendente em 3 jobs' },
    { stage: 'Produção (Fila)', count: 9, percent: 60, detail: 'Offset rodando job 101' },
    { stage: 'Acabamento', count: 5, percent: 40, detail: 'Corte e vinco em manutenção programada' },
    { stage: 'Expedição', count: 3, percent: 20, detail: 'Atraso na transportadora LogFast' },
  ],
  sectorLoad: [
    { name: 'Setor Offset', load: 88, status: 'Crítico', obs: 'Máquina Heidelberg com ruído' },
    { name: 'Impressão Digital', load: 45, status: 'Estável', obs: '' },
    { name: 'Comunicação Visual', load: 72, status: 'Alerta', obs: 'Alta demanda de fim de ano' },
    { name: 'Corte e Vinco', load: 15, status: 'Ocioso', obs: 'Aguardando jobs da impressão' },
  ]
};

export async function handler(event) {
  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Method not allowed" }),
    };
  }

  try {
    const body = JSON.parse(event.body || "{}");
    const { message, history } = body;

    if (!message) {
      return {
        statusCode: 400,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Message is required" }),
      };
    }

    const apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
    if (!apiKey) {
      return {
        statusCode: 500,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Server configuration error: Missing API Key" }),
      };
    }

    const ai = new GoogleGenAI({ apiKey });

    // SYSTEM PROMPT AVANÇADO - BUSINESS INTELLIGENCE
    const systemInstruction = `ATUE COMO UM GESTOR DE OPERAÇÕES E INTELIGÊNCIA DE DADOS DA GRÁFICA NBL.
    
    VOCÊ TEM ACESSO TOTAL AO BANCO DE DADOS EM TEMPO REAL (JSON ABAIXO):
    ${JSON.stringify(MOCK_DB, null, 2)}

    SUAS DIRETRIZES DE RESPOSTA (RACIOCÍNIO ANALÍTICO):
    1. **Cruze Informações**: Nunca olhe um dado isolado. Ex: Se perguntarem de um pedido, verifique se o setor responsável está 'Crítico' ou se falta material no 'inventory'.
    2. **Seja Proativo**: Se vir um problema (ex: Tinta acabando, Setor Crítico), ALERTE o usuário mesmo que ele não tenha perguntado explicitamente.
    3. **Financeiro**: Você tem acesso ao faturamento. Use isso para priorizar respostas (ex: "O pedido X é de alto valor").
    4. **Tom de Voz**: Profissional, Executivo, Orientado a Solução. Não diga "eu acho", diga "os dados indicam".
    5. **Cálculos**: Sinta-se livre para somar valores, calcular prazos e médias.

    EXEMPLOS DE RACIOCÍNIO:
    - Usuário: "Como está o pedido da Padaria Silva?"
    - Você: "O pedido 101 (Padaria Silva) está em Produção. PORÉM, notei que o Setor Offset está com carga Crítica (88%) e a Tinta Ciano está Crítica. Isso pode gerar atraso. Recomendo verificar o estoque de tinta imediatamente."

    Responda em português claro e formatado (use bullets se necessário).`;

    let contents = [];
    if (history && Array.isArray(history)) {
      contents = history.map(msg => ({
        role: msg.role === 'model' ? 'model' : 'user',
        parts: msg.parts
      }));
    }

    contents.push({ role: 'user', parts: [{ text: message }] });

    const response = await ai.models.generateContent({
      model: 'gemini-2.0-flash',
      config: {
        systemInstruction: { parts: [{ text: systemInstruction }] },
      },
      contents: contents
    });

    const responseText = response.text();

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ response: responseText }),
    };

  } catch (error) {
    console.error("Error calling Gemini:", error);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Internal Server Error calling AI provider" }),
    };
  }
}
