
import { Order } from './types';

export const MOCK_ORDERS: Order[] = [
  { id: '101', client: 'Padaria Silva', product: 'Panfletos 5000un', quantity: 5000, status: 'production', dueDate: '2023-10-27', price: 450.00 },
  { id: '102', client: 'Tech Solutions', product: 'Cartões de Visita Verniz Localizado', quantity: 1000, status: 'production', dueDate: '2023-10-27', price: 180.00 },
  { id: '103', client: 'Dra. Ana Paula', product: 'Receituários 10 blocos', quantity: 500, status: 'tomorrow', dueDate: '2023-10-28', price: 120.00 },
  { id: '104', client: 'Restaurante Gourmet', product: 'Cardápios PVC', quantity: 20, status: 'tomorrow', dueDate: '2023-10-28', price: 850.00 },
  { id: '105', client: 'Evento Rock In Rio', product: 'Banners Lona 2x1m', quantity: 5, status: 'next_7_days', dueDate: '2023-11-02', price: 1200.00 },
  { id: '106', client: 'Loja de Roupas Chic', product: 'Sacolas Personalizadas', quantity: 200, status: 'next_7_days', dueDate: '2023-11-04', price: 600.00 },
  { id: '107', client: 'Escola ABC', product: 'Apostilas encadernadas', quantity: 50, status: 'production', dueDate: '2023-10-27', price: 320.00 },
];
