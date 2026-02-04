
import React from 'react';
import { AppView } from '../types';

interface SidebarProps {
  currentView: AppView;
  setView: (view: AppView) => void;
}

const Sidebar: React.FC<SidebarProps> = ({ currentView, setView }) => {
  return (
    <aside className="w-20 md:w-64 bg-slate-900 border-r border-slate-800 flex flex-col h-screen transition-all duration-300 z-30 shrink-0">
      <div className="p-8 flex items-center gap-3">
        <div className="w-10 h-10 bg-orange-500 rounded-xl flex items-center justify-center text-white font-bold text-xl shadow-lg shadow-orange-500/20">N</div>
        <span className="hidden md:block font-black text-white text-lg tracking-tighter uppercase">grafica NBL</span>
      </div>

      <nav className="flex-1 px-4 py-4 space-y-2 overflow-y-auto scrollbar-hide">
        <div>
          <p className="hidden md:block text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] mb-4 px-4">Menu Principal</p>

          <button
            onClick={() => setView('home')}
            className={`w-full flex items-center gap-4 p-4 rounded-2xl transition-all ${currentView === 'home'
                ? 'bg-orange-500 text-white shadow-xl shadow-orange-900/20'
                : 'text-slate-400 hover:bg-white/5 hover:text-white'
              }`}
          >
            {/* Ícone de insights/gráficos para a nova aba Insights (Home) */}
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
            </svg>
            <span className="hidden md:block font-bold text-sm">Insights</span>
          </button>

          <button
            onClick={() => setView('analytics')}
            className={`w-full flex items-center gap-4 p-4 rounded-2xl transition-all ${currentView === 'analytics'
                ? 'bg-orange-500 text-white shadow-xl shadow-orange-900/20'
                : 'text-slate-400 hover:bg-white/5 hover:text-white'
              }`}
          >
            {/* Ícone de home/dashboard para a nova aba Dashboard (Analytics) */}
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
            </svg>
            <span className="hidden md:block font-bold text-sm">Dashboard</span>
          </button>

          <button
            onClick={() => setView('fullscreen-chat')}
            className={`w-full flex items-center gap-4 p-4 rounded-2xl transition-all ${currentView === 'fullscreen-chat'
                ? 'bg-orange-500 text-white shadow-xl shadow-orange-900/20'
                : 'text-slate-400 hover:bg-white/5 hover:text-white'
              }`}
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
            </svg>
            <span className="hidden md:block font-bold text-sm">Consultar IA</span>
          </button>
        </div>

        {/* Quick View Stats - Bottom Sidebar */}
        <div className="hidden md:block pt-8 border-t border-slate-800 mt-4">
          <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] mb-4 px-4">Status de Recurso</p>

          <div className="mx-2 p-3 rounded-xl bg-white/5 space-y-3">
            <div className="space-y-2">
              <div className="flex justify-between items-center text-[10px] font-bold">
                <span className="text-slate-400 uppercase">Capacidade de Produção</span>
                <span className="text-orange-400">78%</span>
              </div>
              <div className="w-full h-1 bg-slate-800 rounded-full">
                <div className="h-full bg-orange-500 rounded-full shadow-[0_0_8px_rgba(249,115,22,0.5)]" style={{ width: '78%' }}></div>
              </div>
            </div>
          </div>
        </div>
      </nav>


    </aside>
  );
};

export default Sidebar;
