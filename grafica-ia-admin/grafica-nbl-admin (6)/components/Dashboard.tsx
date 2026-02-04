
import React from 'react';

const Dashboard: React.FC = () => {
  // Dados simulados de operação (Dashboard agora é a inteligência)
  const productionFlow = [
    { stage: 'Pré-Impressão', count: 12, color: 'bg-slate-400', percent: 85 },
    { stage: 'Produção (Fila)', count: 8, color: 'bg-orange-500', percent: 60 },
    { stage: 'Acabamento', count: 5, color: 'bg-orange-400', percent: 40 },
    { stage: 'Expedição', count: 3, color: 'bg-emerald-500', percent: 20 },
  ];

  const sectorLoad = [
    { name: 'Setor Offset', load: 88, status: 'Crítico' },
    { name: 'Impressão Digital', load: 45, status: 'Estável' },
    { name: 'Comunicação Visual', load: 72, status: 'Alerta' },
    { name: 'Corte e Vinco', load: 15, status: 'Ocioso' },
  ];

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
        {[
          { label: 'Fila Total', value: '28', sub: 'Pedidos', color: 'text-orange-600' },
          { label: 'Eficiência', value: '94%', sub: 'No Prazo', color: 'text-slate-900' },
          { label: 'Materiais', value: '62%', sub: 'Estoque', color: 'text-slate-900' },
          { label: 'Ticket', value: 'R$ 480', sub: 'Média', color: 'text-orange-600' },
        ].map((kpi, i) => (
          <div key={i} className="bg-orange-50/30 p-5 rounded-3xl border border-orange-100 shadow-sm">
            <p className="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-1">{kpi.label}</p>
            <p className={`text-xl font-black tracking-tighter ${kpi.color}`}>{kpi.value}</p>
            <p className="text-[8px] font-bold text-slate-400 uppercase">{kpi.sub}</p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
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
                <span className={`text-[8px] font-black px-2 py-0.5 rounded ${
                  s.status === 'Crítico' ? 'bg-red-50 text-red-600' : 'bg-emerald-50 text-emerald-600'
                }`}>
                  {s.status}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
