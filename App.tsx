
import React, { useState, useCallback, useRef } from 'react';
import Sidebar from './components/Sidebar';
import Dashboard from './components/Dashboard';
import ChatInterface from './components/ChatInterface';
import OrderModal from './components/OrderModal';
import AnalyticsView from './components/AnalyticsView';
import { AppView, Message, Order } from './types';
import { chatWithIA } from './services/chatService';

const App: React.FC = () => {
  const [view, setView] = useState<AppView>('home');
  const [selectedOrder, setSelectedOrder] = useState<Order | null>(null);
  const [messages, setMessages] = useState<Message[]>([
    { role: 'model', text: 'Bem-vindo ao centro de inteligência gráfica NBL. Estou monitorando o fluxo lateral para te auxiliar. O que deseja consultar?', timestamp: new Date() }
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  // Resizing Logic (Horizontal)
  const [chatWidth, setChatWidth] = useState(380); // Largura inicial em pixels
  const isResizing = useRef(false);

  const stopResizing = useCallback(() => {
    isResizing.current = false;
    document.removeEventListener('mousemove', handleMouseMove);
    document.removeEventListener('mouseup', stopResizing);
    document.body.style.cursor = 'default';
  }, []);

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isResizing.current) return;

    // Calcula a nova largura baseada na posição do mouse em relação ao final da janela (direita)
    const newWidth = window.innerWidth - e.clientX;
    const thresholdFullscreen = window.innerWidth * 0.70;

    // Gatilho automático: se expandir mais que 70% da tela, muda para fullscreen
    if (newWidth >= thresholdFullscreen) {
      setView('fullscreen-chat');
      stopResizing();
      return;
    }

    // Limites de largura (mínimo 280px)
    if (newWidth > 280) {
      setChatWidth(newWidth);
    }
  }, [stopResizing]);

  const startResizing = useCallback((e: React.MouseEvent) => {
    isResizing.current = true;
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', stopResizing);
    document.body.style.cursor = 'col-resize';
  }, [handleMouseMove, stopResizing]);

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;
    const userMsg: Message = { role: 'user', text: input, timestamp: new Date() };
    setMessages(prev => [...prev, userMsg]);
    setInput('');
    setIsLoading(true);

    try {
      const history = messages.map(m => ({
        role: m.role,
        parts: [{ text: m.text }]
      }));
      const responseText = await chatWithIA(input, history);
      setMessages(prev => [...prev, { role: 'model', text: responseText || 'Erro no processamento.', timestamp: new Date() }]);
    } catch (error) {
      setMessages(prev => [...prev, { role: 'model', text: 'Sem conexão com NBL Cloud.', timestamp: new Date() }]);
    } finally {
      setIsLoading(false);
    }
  };

  const renderActiveView = () => {
    switch (view) {
      case 'home':
        // Agora a Home (Insights) mostra o Dashboard de Inteligência
        return <Dashboard />;
      case 'analytics':
        // Agora o Dashboard (Aba de Pedidos) mostra a Fila de Pedidos
        return <AnalyticsView onOrderClick={setSelectedOrder} />;
      default:
        return null;
    }
  };

  const handleSetView = (newView: AppView) => {
    if (view === 'fullscreen-chat' && newView !== 'fullscreen-chat') {
      setChatWidth(380); // Restaura para tamanho padrão ao sair do fullscreen
    }
    setView(newView);
  };

  return (
    <div className="flex h-screen w-full bg-slate-50 overflow-hidden text-slate-800 selection:bg-orange-100 selection:text-orange-900 font-['Inter']">
      <Sidebar currentView={view} setView={handleSetView} />

      <div className="flex-1 flex overflow-hidden">
        {/* Main Workspace Area */}
        <main className={`flex flex-col relative overflow-hidden h-full transition-all duration-300 ${view === 'fullscreen-chat' ? 'w-0' : 'flex-1'}`}>
          <header className="h-16 border-b border-orange-100 bg-white flex items-center justify-between px-8 shrink-0 z-20 shadow-sm">
            <div className="flex items-center gap-3">
              <span className="text-[10px] font-black text-slate-900 uppercase tracking-[0.25em] bg-orange-50 border border-orange-100 px-4 py-1.5 rounded-full">Sistema NBL</span>
              <span className="text-slate-200">|</span>
              <span className="text-xs font-bold text-slate-400 tracking-widest uppercase">
                {view === 'home' ? 'Inteligência & Métricas' : 'Dashboard de Pedidos'}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
              <span className="text-[9px] font-black text-slate-400 uppercase tracking-widest">Servidor Online</span>
            </div>
          </header>

          <div className="flex-1 overflow-y-auto bg-white p-6 scrollbar-hide">
            {renderActiveView()}
          </div>
        </main>

        {/* Vertical Resizer Handle */}
        {view !== 'fullscreen-chat' && (
          <div
            onMouseDown={startResizing}
            className="w-1.5 h-full bg-transparent hover:bg-orange-400/20 cursor-col-resize flex items-center justify-center group transition-colors relative z-30"
          >
            <div className="h-12 w-1 bg-orange-200 rounded-full group-hover:bg-orange-500 transition-colors"></div>
          </div>
        )}

        {/* Right Side Chat Panel */}
        <aside
          style={{ width: view === 'fullscreen-chat' ? '100%' : `${chatWidth}px` }}
          className="bg-white border-l border-orange-100 shadow-[-20px_0_40px_rgba(0,0,0,0.02)] z-20 overflow-hidden flex flex-col transition-[width] duration-75"
        >
          <ChatInterface
            fullscreen={view === 'fullscreen-chat'}
            messages={messages}
            setMessages={setMessages}
            input={input}
            setInput={setInput}
            onSend={handleSend}
            isLoading={isLoading}
          />
        </aside>
      </div>

      <OrderModal order={selectedOrder} onClose={() => setSelectedOrder(null)} />
    </div>
  );
};

export default App;
