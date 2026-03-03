import React from 'react';
import { MOCK_PRODUCTION_FLOW, MOCK_SECTOR_LOAD, MOCK_KPIS, MOCK_INVENTORY } from '../mockData';

const Dashboard: React.FC = () => {
  // Dados agora vêm do "Banco de Dados Central"
  const productionFlow = MOCK_PRODUCTION_FLOW;
  const sectorLoad = MOCK_SECTOR_LOAD;

  return (
    <div className="space-y-8 animate-in fade-in slide-in-from-right-4 duration-700">
      <header className="flex justify-between items-end">
        <div>
          <h1 className="text-3xl font-black text-slate-900 tracking-tighter">Status de Produção</h1>
          <p className="text-xs text-slate-500 font-bold uppercase tracking-widest">Monitoramento NBL em Tempo Real</p>
        </div>
      </header>

      {/* KPI Row */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {MOCK_KPIS.map((kpi, i) => (
          <div key={i} className="bg-orange-50/30 p-5 rounded-3xl border border-orange-100 shadow-sm">
            <p className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1">{kpi.label}</p>
            <p className={`text-xl font-black tracking-tighter ${kpi.color}`}>{kpi.value}</p>
            <p className="text-[8px] font-bold text-slate-400 uppercase">{kpi.sub}</p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">

        {/* Coluna 1: Operações (Fluxo + Estoque) */}
        <div className="space-y-8">
          {/* Fluxo de Produção */}
          <div className="bg-white p-8 rounded-[2.5rem] border border-orange-100 shadow-xl shadow-orange-900/5">
            <div className="mb-8">
              <h2 className="text-lg font-black text-slate-800">Funil Produtivo</h2>
              <p className="text-[10px] text-slate-400 font-bold uppercase tracking-widest">Distribuição por estágio</p>
            </div>

            <div className="space-y-6">
              {productionFlow.map((item, i) => (
                <div key={i}>
                  <div className="flex justify-between items-end mb-2">
                    <span className="text-[10px] font-black text-slate-600 uppercase tracking-tight">{item.stage}</span>
                    <span className="text-[10px] font-black text-orange-600">{item.count} un</span>
                  </div>
                  <div className="w-full h-2 bg-slate-100 rounded-full overflow-hidden">
                    <div
                      className={`h-full ${item.color} transition-all duration-1000`}
                      style={{ width: `${item.percent}%` }}
                    ></div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Alerta de Estoque (Novo Widget) */}
          <div className="bg-slate-900 p-8 rounded-[2.5rem] text-white shadow-xl shadow-slate-900/10">
            <div className="flex items-center gap-4 mb-6">
              <div className="w-10 h-10 rounded-full bg-orange-500 flex items-center justify-center font-bold animate-pulse">!</div>
              <div>
                <h2 className="text-lg font-bold">Insumos Críticos</h2>
                <p className="text-[10px] text-slate-400 uppercase tracking-widest">Monitoramento de Estoque</p>
              </div>
            </div>
            <div className="space-y-3">
              {MOCK_INVENTORY?.filter(i => i.status !== 'OK').map((item, idx) => (
                <div key={idx} className="flex justify-between items-center text-xs border-b border-white/10 pb-3 last:border-0 last:pb-0">
                  <span className="text-slate-300 font-medium">{item.item}</span>
                  <span className={`px-2 py-1 rounded text-[9px] font-black uppercase tracking-wider ${item.status === 'Crítico' ? 'bg-red-500/20 text-red-400' : 'bg-orange-500/20 text-orange-300'}`}>
                    {item.quantity} ({item.status})
                  </span>
                </div>
              ))}
              {(!MOCK_INVENTORY || MOCK_INVENTORY.every(i => i.status === 'OK')) && (
                <p className="text-xs text-slate-500 italic">Nenhum alerta de estoque.</p>
              )}
            </div>
          </div>
        </div>

        {/* Coluna 2: Máquinas + Financeiro (Novo) */}
        <div className="space-y-8">
          {/* Carga por Setor */}
          <div className="bg-white p-8 rounded-[2.5rem] border border-orange-100 shadow-xl shadow-orange-900/5">
            <div className="mb-8">
              <h2 className="text-lg font-black text-slate-800">Uso de Máquinas</h2>
              <p className="text-[10px] text-slate-400 font-bold uppercase tracking-widest">Carga técnica atual</p>
            </div>

            <div className="space-y-6">
              {sectorLoad.map((s, i) => (
                <div key={i} className="flex items-center gap-4">
                  <div className="flex-1">
                    <div className="flex justify-between items-center mb-1">
                      <span className="text-[9px] font-black text-slate-500 uppercase">{s.name}</span>
                      <span className="text-[9px] font-mono font-black">{s.load}%</span>
                    </div>
                    <div className="w-full h-1.5 bg-slate-50 rounded-full overflow-hidden">
                      <div
                        className={`h-full ${s.load > 80 ? 'bg-red-500' : 'bg-orange-500'}`}
                        style={{ width: `${s.load}%` }}
                      ></div>
                    </div>
                  </div>
                  <span className={`text-[8px] font-black px-2 py-0.5 rounded ${s.status === 'Crítico' ? 'bg-red-50 text-red-600' : 'bg-emerald-50 text-emerald-600'
                    }`}>
                    {s.status}
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* Resumo Financeiro Rápido (Exemplo Visual) */}
          <div className="bg-gradient-to-br from-orange-400 to-orange-600 p-8 rounded-[2.5rem] text-white shadow-xl shadow-orange-500/20">
            <h2 className="text-lg font-black mb-1">Visão Financeira</h2>
            <p className="text-[10px] text-orange-100 font-bold uppercase tracking-widest mb-6">Receita Projetada (Mês)</p>

            <div className="flex items-baseline gap-1 mb-4">
              <span className="text-3xl font-black tracking-tighter">R$ 89.2k</span>
              <span className="text-xs font-bold text-orange-200">+12%</span>
            </div>

            <div className="flex gap-2">
              <div className="flex-1 bg-white/20 p-3 rounded-2xl">
                <p className="text-[8px] font-black uppercase text-orange-100 mb-1">Dia</p>
                <p className="text-sm font-bold">R$ 3.4k</p>
              </div>
              <div className="flex-1 bg-white/20 p-3 rounded-2xl">
                <p className="text-[8px] font-black uppercase text-orange-100 mb-1">Pendente</p>
                <p className="text-sm font-bold">R$ 4.5k</p>
              </div>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
};

export default Dashboard;
