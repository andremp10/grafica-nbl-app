
import React from 'react';
import { Order } from '../types';

interface OrderCardProps {
  order: Order;
  onClick: (order: Order) => void;
}

const OrderCard: React.FC<OrderCardProps> = ({ order, onClick }) => {
  const getStatusStyles = () => {
    switch (order.status) {
      case 'tomorrow': return 'bg-red-50 text-red-600 border-red-100';
      case 'next_7_days': return 'bg-orange-50 text-orange-600 border-orange-100';
      default: return 'bg-slate-50 text-slate-600 border-slate-100';
    }
  };

  return (
    <div 
      onClick={() => onClick(order)}
      className="bg-white p-4 rounded-2xl border border-orange-50 shadow-sm hover:shadow-lg hover:border-orange-200 transition-all cursor-pointer group active:scale-[0.98]"
    >
      <div className="flex justify-between items-start mb-2">
        <div className="overflow-hidden">
          <h4 className="font-black text-slate-800 text-sm group-hover:text-orange-600 transition-colors truncate">{order.client}</h4>
          <p className="text-[10px] text-slate-400 font-bold uppercase truncate">{order.product}</p>
        </div>
        <span className={`px-2 py-0.5 rounded-md text-[8px] font-black uppercase tracking-wider border shrink-0 ${getStatusStyles()}`}>
          {order.status === 'tomorrow' ? 'Amanhã' : 'Semana'}
        </span>
      </div>
      <div className="flex items-center justify-between mt-3">
        <div className="flex items-center gap-1.5 text-[10px] text-slate-400 font-bold">
          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2.5" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          {new Date(order.dueDate).toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit' })}
        </div>
        <span className="font-black text-slate-900 text-sm">
          R$ {order.price.toFixed(2)}
        </span>
      </div>
    </div>
  );
};

export default OrderCard;
