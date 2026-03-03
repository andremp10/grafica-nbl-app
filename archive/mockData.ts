// Banco de Dados Fictício Centralizado
import { Order } from './types';

// Tabela de Pedidos
export const MOCK_ORDERS: Order[] = [
  { id: '101', client: 'Padaria Silva', product: 'Panfletos 5000un', quantity: 5000, status: 'production', dueDate: '2023-10-27', price: 450.00, priority: 'Normal' },
  { id: '102', client: 'Tech Solutions', product: 'Cartões de Visita Verniz Localizado', quantity: 1000, status: 'production', dueDate: '2023-10-27', price: 180.00, priority: 'Alta' },
  { id: '103', client: 'Dra. Ana Paula', product: 'Receituários 10 blocos', quantity: 500, status: 'tomorrow', dueDate: '2023-10-28', price: 120.00, priority: 'Normal' },
  { id: '104', client: 'Restaurante Gourmet', product: 'Cardápios PVC', quantity: 20, status: 'tomorrow', dueDate: '2023-10-28', price: 850.00, priority: 'Alta' },
  { id: '105', client: 'Evento Rock In Rio', product: 'Banners Lona 2x1m', quantity: 5, status: 'next_7_days', dueDate: '2023-11-02', price: 1200.00, priority: 'Urgente' },
  { id: '106', client: 'Loja de Roupas Chic', product: 'Sacolas Personalizadas', quantity: 200, status: 'next_7_days', dueDate: '2023-11-04', price: 600.00, priority: 'Normal' },
  { id: '107', client: 'Escola ABC', product: 'Apostilas encadernadas', quantity: 50, status: 'production', dueDate: '2023-10-27', price: 320.00, priority: 'Baixa' },
  { id: '108', client: 'Construtora Forte', product: 'Placas de Sinalização', quantity: 30, status: 'production', dueDate: '2023-10-29', price: 1500.00, priority: 'Normal' },
  { id: '109', client: 'Buffet Alegria', product: 'Convites de Casamento Luxo', quantity: 150, status: 'next_7_days', dueDate: '2023-11-05', price: 890.00, priority: 'Alta' },
];

export const MOCK_INVENTORY = [
  { item: 'Papel Couché 150g', quantity: '5000 fls', status: 'OK' },
  { item: 'Papel Supremo 300g', quantity: '200 fls', status: 'Baixo' },
  { item: 'Tinta Ciano (Offset)', quantity: '2 Latas', status: 'Crítico' },
  { item: 'Lona Vinílica', quantity: '3 Rolos', status: 'OK' }
];

export const MOCK_FINANCIAL = {
  daily_revenue: 3450.00,
  monthly_revenue: 89200.00,
  pending_payments: 4500.00
};

// Tabelas de Insights e Métricas (Dashboard)
export const MOCK_PRODUCTION_FLOW = [
  { stage: 'Pré-Impressão', count: 12, color: 'bg-slate-400', percent: 85, detail: 'Aprovação de arte pendente em 3 jobs' },
  { stage: 'Produção (Fila)', count: MOCK_ORDERS.filter(o => o.status === 'production').length + 5, color: 'bg-orange-500', percent: 60, detail: 'Offset rodando job 101' },
  { stage: 'Acabamento', count: 5, color: 'bg-orange-400', percent: 40, detail: 'Corte e vinco em manutenção programada' },
  { stage: 'Expedição', count: 3, color: 'bg-emerald-500', percent: 20, detail: 'Atraso na transportadora LogFast' },
];

export const MOCK_SECTOR_LOAD = [
  { name: 'Setor Offset', load: 88, status: 'Crítico', obs: 'Máquina Heidelberg com ruído' },
  { name: 'Impressão Digital', load: 45, status: 'Estável', obs: '' },
  { name: 'Comunicação Visual', load: 72, status: 'Alerta', obs: 'Alta demanda de fim de ano' },
  { name: 'Corte e Vinco', load: 15, status: 'Ocioso', obs: 'Aguardando jobs da impressão' },
];

export const MOCK_KPIS = [
  { label: 'Fila Total', value: String(MOCK_ORDERS.length + 15), sub: 'Pedidos', color: 'text-orange-600' },
  { label: 'Eficiência', value: '94%', sub: 'No Prazo', color: 'text-slate-900' },
  { label: 'Materiais', value: '62%', sub: 'Estoque', color: 'text-slate-900' }, // Poderíamos calcular real, mas manterei hardcoded por consistência visual
  { label: 'Ticket', value: 'R$ ' + (MOCK_ORDERS.reduce((acc, o) => acc + o.price, 0) / MOCK_ORDERS.length).toFixed(0), sub: 'Média', color: 'text-orange-600' },
];
