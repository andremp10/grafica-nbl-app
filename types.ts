
export interface Order {
  id: string;
  client: string;
  product: string;
  quantity: number;
  status: 'production' | 'tomorrow' | 'next_7_days';
  dueDate: string;
  price: number;
}

export interface Message {
  role: 'user' | 'model';
  text: string;
  timestamp: Date;
}

export type AppView = 'home' | 'fullscreen-chat' | 'analytics';
