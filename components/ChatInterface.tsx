
import React, { useRef, useEffect } from 'react';
import { Message } from '../types';

interface ChatInterfaceProps {
  fullscreen?: boolean;
  messages: Message[];
  setMessages: React.Dispatch<React.SetStateAction<Message[]>>;
  input: string;
  setInput: (val: string) => void;
  onSend: () => void;
  isLoading: boolean;
}

const ChatInterface: React.FC<ChatInterfaceProps> = ({ 
  fullscreen, messages, input, setInput, onSend, isLoading 
}) => {
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isLoading]);

  return (
    <div className="flex flex-col h-full bg-white relative">
      {/* Header do Chat */}
      <div className="h-16 px-6 bg-slate-900 flex justify-between items-center shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-2 h-2 bg-orange-500 rounded-full animate-pulse shadow-[0_0_8px_#f97316]"></div>
          <span className="text-[10px] font-black text-white uppercase tracking-[0.3em]">IA NBL Assist</span>
        </div>
        {fullscreen && (
          <span className="text-[8px] font-black text-orange-400 uppercase tracking-widest border border-orange-400/30 px-2 py-0.5 rounded">Foco Total</span>
        )}
      </div>

      {/* Chat Messages */}
      <div className="flex-1 overflow-y-auto px-6 py-8 space-y-8 scrollbar-hide" ref={scrollRef}>
        {messages.map((msg, i) => (
          <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div className={`max-w-[85%] p-4 rounded-2xl ${
              msg.role === 'user' 
              ? 'bg-orange-600 text-white rounded-tr-none shadow-xl shadow-orange-100' 
              : 'bg-slate-50 text-slate-800 rounded-tl-none border border-slate-100'
            }`}>
              <p className="text-sm leading-relaxed font-medium">{msg.text}</p>
              <span className={`text-[8px] font-bold uppercase mt-2 block opacity-40 ${msg.role === 'user' ? 'text-right' : 'text-left'}`}>
                {msg.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              </span>
            </div>
          </div>
        ))}
        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-slate-50 p-4 rounded-2xl rounded-tl-none border border-slate-100 animate-pulse">
              <div className="flex gap-1.5">
                <div className="w-1.5 h-1.5 bg-orange-400 rounded-full animate-bounce"></div>
                <div className="w-1.5 h-1.5 bg-orange-400 rounded-full animate-bounce [animation-delay:0.2s]"></div>
                <div className="w-1.5 h-1.5 bg-orange-400 rounded-full animate-bounce [animation-delay:0.4s]"></div>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Input Area */}
      <div className="p-6 bg-white border-t border-slate-50">
        <div className="relative group">
          <textarea 
            rows={2}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                onSend();
              }
            }}
            placeholder="O que deseja consultar agora?"
            className="w-full bg-slate-50 rounded-2xl border border-slate-100 p-4 pr-16 text-xs font-bold placeholder:text-slate-300 focus:ring-4 focus:ring-orange-50 focus:border-orange-200 transition-all outline-none resize-none"
          />
          <button 
            onClick={onSend}
            disabled={isLoading || !input.trim()}
            className="absolute right-3 bottom-3 p-2 bg-orange-600 text-white rounded-xl shadow-lg shadow-orange-100 hover:bg-orange-700 active:scale-90 transition-all disabled:bg-slate-200 disabled:shadow-none"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="3" d="M14 5l7 7m0 0l-7 7m7-7H3" />
            </svg>
          </button>
        </div>
      </div>
    </div>
  );
};

export default ChatInterface;
