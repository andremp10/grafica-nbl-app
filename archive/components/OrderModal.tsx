
import React from 'react';
import { Order } from '../types';

interface OrderModalProps {
  order: Order | null;
  onClose: () => void;
}

const OrderModal: React.FC<OrderModalProps> = ({ order, onClose }) => {
  if (!order) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-md animate-in fade-in duration-300">
      <div className="bg-white w-full max-w-xl rounded-[3rem] shadow-[0_32px_64px_-12px_rgba(0,0,0,0.3)] overflow-hidden animate-in zoom-in-95 duration-500">
        <div className="bg-slate-900 p-10 text-white flex justify-between items-start">
          <div>
            <div className="flex items-center gap-3 mb-4">
              <span className="bg-white/10 px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-[0.2em] border border-white/20">REGISTRO #{order.id}</span>
              {order.status === 'tomorrow' && <span className="bg-red-500 px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-[0.2em] shadow-lg shadow-red-500/20">PRIORIDADE ALTA</span>}
            </div>
            <h3 className="text-4xl font-black tracking-tighter">{order.client}</h3>
          </div>
          <button onClick={onClose} className="p-3 hover:bg-white/10 rounded-2xl transition-all active:scale-90">
            <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2.5" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        
        <div className="p-10 space-y-10">
          <div className="grid grid-cols-2 gap-10">
            <div className="space-y-2">
              <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Especificação do Produto</p>
              <p className="font-black text-slate-800 text-xl tracking-tight">{order.product}</p>
            </div>
            <div className="space-y-2 text-right">
              <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Tiragem Total</p>
              <p className="font-black text-slate-800 text-xl tracking-tight">{order.quantity.toLocaleString()} un</p>
            </div>
            <div className="space-y-2">
              <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Expedição Programada</p>
              <p className="font-bold text-slate-800 text-lg">{new Date(order.dueDate).toLocaleDateString('pt-BR')}</p>
            </div>
            <div className="space-y-2 text-right">
              <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Faturamento Estimado</p>
              <p className="text-3xl font-black text-slate-900 tracking-tighter">R$ {order.price.toFixed(2)}</p>
            </div>
          </div>

          <div className="bg-slate-50 p-8 rounded-[2rem] space-y-4 border border-slate-100">
            <div className="flex items-center gap-3">
              <div className="w-6 h-6 bg-slate-900 rounded-md flex items-center justify-center">
                <svg className="w-3.5 h-3.5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="3" d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <h4 className="text-xs font-black text-slate-800 uppercase tracking-widest">Checklist de Produção</h4>
            </div>
            <ul className="text-xs text-slate-500 font-bold space-y-3 list-none">
              <li className="flex items-center gap-3"><span className="w-1.5 h-1.5 bg-blue-500 rounded-full"></span> Matéria-prima em estoque (Couchê 300g)</li>
              <li className="flex items-center gap-3"><span className="w-1.5 h-1.5 bg-blue-500 rounded-full"></span> Arte final vetorizada e aprovada</li>
              <li className="flex items-center gap-3"><span className="w-1.5 h-1.5 bg-blue-500 rounded-full"></span> Acabamento especial Pro-Gloss solicitado</li>
            </ul>
          </div>

          <div className="flex gap-5">
            <button className="flex-[2] bg-slate-900 text-white py-5 rounded-[1.5rem] font-black hover:bg-black transition-all shadow-2xl shadow-slate-200 active:scale-95 uppercase tracking-widest text-xs">
              Iniciar Ordem de Produção
            </button>
            <button onClick={onClose} className="flex-1 bg-slate-100 text-slate-500 py-5 rounded-[1.5rem] font-black hover:bg-slate-200 transition-colors uppercase tracking-widest text-xs">
              Fechar
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default OrderModal;
