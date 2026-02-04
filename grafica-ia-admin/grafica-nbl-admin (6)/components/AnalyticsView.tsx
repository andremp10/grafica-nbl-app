
import React from 'react';
import { MOCK_ORDERS } from '../mockData';
import { Order } from '../types';
import OrderCard from './OrderCard';

interface AnalyticsViewProps {
  onOrderClick: (order: Order) => void;
}

const AnalyticsView: React.FC<AnalyticsViewProps> = ({ onOrderClick }) => {
  const forTomorrow = MOCK_ORDERS.filter(o => o.status === 'tomorrow');
  const next7Days = MOCK_ORDERS.filter(o => o.status === 'next_7_days');

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-left-4 duration-700">
      <header>
        <h1 className="text-3xl font-black text-slate-900 tracking-tighter">Fila de Pedidos</h1>
        <p className="text-xs text-slate-500 font-bold uppercase tracking-widest">Gestão de Cronograma NBL</p>
      </header>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Amanhã */}
        <section className="bg-red-50/20 p-6 rounded-[2.5rem] border border-red-100/50">
          <div className="flex items-center justify-between mb-6 px-2">
            <h2 className="text-[11px] font-black text-red-800 uppercase tracking-widest flex items-center gap-2">
              <span className="w-2 h-2 bg-red-500 rounded-full animate-pulse"></span>
              Urgência (Amanhã)
            </h2>
            <span className="bg-red-500 text-white px-2.5 py-1 rounded-xl text-[10px] font-black shadow-lg shadow-red-200">
              {forTomorrow.length}
            </span>
          </div>
          <div className="space-y-4">
            {forTomorrow.map(order => <OrderCard key={order.id} order={order} onClick={onOrderClick} />)}
            {forTomorrow.length === 0 && (
              <div className="text-center py-12 border-2 border-dashed border-red-100 rounded-3xl text-red-300 text-[10px] font-black uppercase italic">
                Sem pendências urgentes
              </div>
            )}
          </div>
        </section>

        {/* Semana */}
        <section className="bg-slate-50 p-6 rounded-[2.5rem] border border-slate-200/50">
          <div className="flex items-center justify-between mb-6 px-2">
            <h2 className="text-[11px] font-black text-slate-800 uppercase tracking-widest flex items-center gap-2">
              <span className="w-2 h-2 bg-slate-400 rounded-full"></span>
              Próximos 7 Dias
            </h2>
            <span className="bg-slate-800 text-white px-2.5 py-1 rounded-xl text-[10px] font-black shadow-lg shadow-slate-200">
              {next7Days.length}
            </span>
          </div>
          <div className="space-y-4">
            {next7Days.map(order => <OrderCard key={order.id} order={order} onClick={onOrderClick} />)}
          </div>
        </section>
      </div>
    </div>
  );
};

export default AnalyticsView;
