-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.etl_rejections (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  run_id uuid NOT NULL,
  table_name text NOT NULL,
  legacy_pk text,
  reason text NOT NULL,
  payload jsonb,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT etl_rejections_pkey PRIMARY KEY (id),
  CONSTRAINT fk_etl_rejections_run_id FOREIGN KEY (run_id) REFERENCES public.etl_runs(run_id)
);
CREATE TABLE public.etl_runs (
  run_id uuid NOT NULL DEFAULT gen_random_uuid(),
  run_date date,
  source text,
  status text NOT NULL CHECK (status = ANY (ARRAY['running'::text, 'success'::text, 'failed'::text])),
  started_at timestamp without time zone NOT NULL DEFAULT now(),
  finished_at timestamp without time zone,
  counts jsonb,
  error text,
  CONSTRAINT etl_runs_pkey PRIMARY KEY (run_id)
);
CREATE TABLE public.is_apps_whatsapp_msgs (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo character varying NOT NULL UNIQUE,
  mensagem text NOT NULL,
  CONSTRAINT is_apps_whatsapp_msgs_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_arquivos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  idr character varying,
  idf character varying,
  tipo character varying,
  url_base character varying,
  bucket character varying,
  caminho character varying,
  nome character varying,
  status integer,
  json text,
  tamanho double precision,
  extensao character varying,
  data timestamp without time zone,
  data_modificado timestamp without time zone,
  identificacao character varying,
  CONSTRAINT is_arquivos_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_bancos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  banco character varying NOT NULL,
  operador character varying NOT NULL,
  agencia character varying NOT NULL,
  cpf_cnpj character varying NOT NULL,
  cc character varying NOT NULL,
  cp character varying NOT NULL,
  titular character varying NOT NULL,
  CONSTRAINT is_bancos_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_clientes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  saldo numeric NOT NULL DEFAULT 0,
  tipo character varying NOT NULL CHECK (upper(tipo::text) = ANY (ARRAY['PF'::text, 'PJ'::text])),
  telefone character varying,
  celular character varying,
  email_log character varying NOT NULL UNIQUE,
  senha_log character varying,
  ultimo_acesso timestamp without time zone,
  ip character varying,
  status integer NOT NULL DEFAULT 1,
  retirada smallint,
  retirada_limite numeric DEFAULT 0,
  revendedor integer,
  pdv integer,
  wpp_verificado character varying,
  logotipo character varying,
  pagarme_id character varying,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_clientes_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_clientes_enderecos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  cliente_id uuid,
  titulo character varying,
  cep character varying,
  logradouro character varying,
  numero character varying,
  bairro character varying,
  complemento character varying,
  cidade character varying,
  estado character varying,
  is_principal boolean NOT NULL DEFAULT false,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_clientes_enderecos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_clientes_enderecos_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id)
);
CREATE TABLE public.is_clientes_extratos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  cliente_id uuid,
  pedido_id uuid,
  pagamento_id uuid,
  saldo_antes numeric NOT NULL,
  saldo_depois numeric NOT NULL,
  descricao character varying,
  obs character varying,
  valor numeric NOT NULL,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_clientes_extratos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_clientes_extratos_pagamento_id FOREIGN KEY (pagamento_id) REFERENCES public.is_pedidos_pagamentos(id),
  CONSTRAINT fk_is_clientes_extratos_pedido_id FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id),
  CONSTRAINT fk_is_clientes_extratos_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id)
);
CREATE TABLE public.is_clientes_pf (
  cliente_id uuid NOT NULL,
  nome character varying CHECK (nome IS NOT NULL),
  sobrenome character varying,
  nascimento timestamp without time zone,
  cpf character varying,
  sexo character varying,
  CONSTRAINT is_clientes_pf_pkey PRIMARY KEY (cliente_id),
  CONSTRAINT fk_is_clientes_pf_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id)
);
CREATE TABLE public.is_clientes_pj (
  cliente_id uuid NOT NULL,
  razao_social character varying CHECK (razao_social IS NOT NULL),
  fantasia character varying,
  ie character varying,
  cnpj character varying,
  CONSTRAINT is_clientes_pj_pkey PRIMARY KEY (cliente_id),
  CONSTRAINT fk_is_clientes_pj_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id)
);
CREATE TABLE public.is_config (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome character varying NOT NULL UNIQUE,
  valor text,
  CONSTRAINT is_config_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_config_logs_curl (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  metodo character varying NOT NULL,
  destino text NOT NULL,
  requisicao_cabecalho text,
  requisicao_corpo text,
  retorno_cabecalho text,
  retorno_corpo text,
  retorno_status character varying,
  duracao character varying,
  servidor_detalhes text,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_config_logs_curl_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_entregas_balcoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  titulo character varying NOT NULL,
  telefone character varying,
  logradouro character varying,
  cep character varying,
  complemento character varying,
  bairro character varying,
  cidade character varying,
  estado character varying,
  custo numeric NOT NULL CHECK (custo >= 0::numeric),
  prazo character varying,
  created_at timestamp without time zone DEFAULT now(),
  arquivado boolean DEFAULT false,
  CONSTRAINT is_entregas_balcoes_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_entregas_fretes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  titulo character varying NOT NULL,
  descricao character varying,
  prazo integer,
  min_km character varying,
  max_km character varying,
  taxa numeric NOT NULL,
  min_compra numeric NOT NULL DEFAULT 0,
  tipo integer NOT NULL,
  minimo_peso character varying,
  limite_peso character varying,
  minimo_c integer,
  limite_c integer,
  CONSTRAINT is_entregas_fretes_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_entregas_fretes_locais (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  frete_id uuid NOT NULL,
  estado character varying,
  cidade character varying,
  bairro character varying,
  cep_inicio character varying,
  cep_fim character varying,
  CONSTRAINT is_entregas_fretes_locais_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_entregas_fretes_locais_frete_id FOREIGN KEY (frete_id) REFERENCES public.is_entregas_fretes(id)
);
CREATE TABLE public.is_entregas_fretes_produtos (
  frete_id uuid NOT NULL,
  produto_id uuid NOT NULL,
  CONSTRAINT is_entregas_fretes_produtos_pkey PRIMARY KEY (frete_id, produto_id),
  CONSTRAINT fk_is_entregas_fretes_produtos_frete_id FOREIGN KEY (frete_id) REFERENCES public.is_entregas_fretes(id),
  CONSTRAINT fk_is_entregas_fretes_produtos_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_extras_status (
  id integer NOT NULL,
  nome character varying NOT NULL,
  num integer,
  visivel integer,
  CONSTRAINT is_extras_status_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_financeiro_caixas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  operador_id uuid,
  pdv_id uuid,
  abertura_descricao character varying,
  abertura_valor numeric NOT NULL,
  abertura_data timestamp without time zone,
  fechamento_descricao character varying,
  fechamento_valor numeric,
  fechamento_data timestamp without time zone,
  fechamento_json text,
  status integer NOT NULL,
  CONSTRAINT is_financeiro_caixas_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_financeiro_caixas_operador_id FOREIGN KEY (operador_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_financeiro_caixas_pdv_id FOREIGN KEY (pdv_id) REFERENCES public.is_financeiro_pdvs(id)
);
CREATE TABLE public.is_financeiro_caixas_movimentacoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  caixa_id uuid NOT NULL,
  tipo integer NOT NULL,
  descricao text,
  forma character varying,
  valor numeric NOT NULL CHECK (valor >= 0::numeric),
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_financeiro_caixas_movimentacoes_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_financeiro_caixas_movimentacoes_caixa_id FOREIGN KEY (caixa_id) REFERENCES public.is_financeiro_caixas(id)
);
CREATE TABLE public.is_financeiro_carteiras (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  titulo character varying NOT NULL,
  tipo integer NOT NULL,
  saldo_inicial numeric NOT NULL DEFAULT 0 CHECK (saldo_inicial >= 0::numeric),
  CONSTRAINT is_financeiro_carteiras_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_financeiro_categorias (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  titulo character varying NOT NULL,
  cor character varying,
  arquivado boolean DEFAULT false,
  CONSTRAINT is_financeiro_categorias_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_financeiro_centros_custo (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  titulo character varying NOT NULL UNIQUE,
  CONSTRAINT is_financeiro_centros_custo_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_financeiro_conciliacoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  conta character varying,
  arquivo character varying,
  arquivo_id uuid,
  data character varying,
  CONSTRAINT is_financeiro_conciliacoes_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_financeiro_conciliacoes_arquivo_id FOREIGN KEY (arquivo_id) REFERENCES public.is_arquivos(id)
);
CREATE TABLE public.is_financeiro_fornecedores (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo character varying,
  razao_social character varying,
  ie character varying,
  cnpj character varying,
  nome character varying,
  sobrenome character varying,
  cpf character varying,
  telefone character varying,
  celular character varying,
  cep character varying,
  logradouro character varying,
  numero character varying,
  bairro character varying,
  complemento character varying,
  cidade character varying,
  estado character varying,
  email_log character varying,
  created_at timestamp without time zone DEFAULT now(),
  arquivado boolean DEFAULT false,
  obs text,
  CONSTRAINT is_financeiro_fornecedores_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_financeiro_funcionarios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome character varying NOT NULL,
  sobrenome character varying,
  nascimento timestamp without time zone,
  cpf character varying,
  rg character varying,
  sexo character varying,
  telefone character varying,
  celular character varying,
  cep character varying,
  logradouro character varying,
  numero character varying,
  bairro character varying,
  complemento character varying,
  cidade character varying,
  estado character varying,
  admissao date,
  demissao date,
  salario numeric NOT NULL,
  salario_vencimento integer,
  vale numeric DEFAULT 0,
  vale_vencimento integer,
  cargo character varying,
  obs text,
  CONSTRAINT is_financeiro_funcionarios_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_financeiro_lancamentos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  descricao character varying,
  valor numeric NOT NULL CHECK (valor >= 0::numeric),
  data timestamp without time zone,
  data_pagto timestamp without time zone,
  data_emissao timestamp without time zone,
  categoria_id uuid,
  obs text,
  anexo character varying,
  anexo_arquivo_id uuid,
  carteira_id uuid,
  tipo integer NOT NULL CHECK (tipo = ANY (ARRAY[1, 2])),
  status integer NOT NULL,
  fornecedor_id uuid,
  pdv_id uuid,
  funcionario_id uuid,
  vendedor_id uuid,
  caixa_id uuid,
  centro_custo_id uuid,
  origem integer,
  uid character varying,
  agrupar integer,
  conciliacao character varying,
  conciliacao_movimentacao integer,
  conciliacao_pagto integer,
  neutro integer,
  repetir integer,
  CONSTRAINT is_financeiro_lancamentos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_financeiro_lancamentos_categoria_id FOREIGN KEY (categoria_id) REFERENCES public.is_financeiro_categorias(id),
  CONSTRAINT fk_is_financeiro_lancamentos_carteira_id FOREIGN KEY (carteira_id) REFERENCES public.is_financeiro_carteiras(id),
  CONSTRAINT fk_is_financeiro_lancamentos_fornecedor_id FOREIGN KEY (fornecedor_id) REFERENCES public.is_financeiro_fornecedores(id),
  CONSTRAINT fk_is_financeiro_lancamentos_pdv_id FOREIGN KEY (pdv_id) REFERENCES public.is_financeiro_pdvs(id),
  CONSTRAINT fk_is_financeiro_lancamentos_funcionario_id FOREIGN KEY (funcionario_id) REFERENCES public.is_financeiro_funcionarios(id),
  CONSTRAINT fk_is_financeiro_lancamentos_vendedor_id FOREIGN KEY (vendedor_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_financeiro_lancamentos_caixa_id FOREIGN KEY (caixa_id) REFERENCES public.is_financeiro_caixas(id),
  CONSTRAINT fk_is_financeiro_lancamentos_centro_custo_id FOREIGN KEY (centro_custo_id) REFERENCES public.is_financeiro_centros_custo(id),
  CONSTRAINT fk_is_financeiro_lancamentos_anexo_arquivo_id FOREIGN KEY (anexo_arquivo_id) REFERENCES public.is_arquivos(id)
);
CREATE TABLE public.is_financeiro_notasfiscais (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nf character varying NOT NULL,
  chave character varying,
  serie character varying,
  fornecedor_id uuid NOT NULL,
  transportadora_id uuid,
  emissao timestamp without time zone,
  saida timestamp without time zone,
  valor numeric NOT NULL CHECK (valor >= 0::numeric),
  arquivo character varying,
  arquivo_id uuid,
  created_at timestamp without time zone DEFAULT now(),
  json text,
  CONSTRAINT is_financeiro_notasfiscais_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_financeiro_notasfiscais_fornecedor_id FOREIGN KEY (fornecedor_id) REFERENCES public.is_financeiro_fornecedores(id),
  CONSTRAINT fk_is_financeiro_notasfiscais_transportadora_id FOREIGN KEY (transportadora_id) REFERENCES public.is_financeiro_transportadoras(id),
  CONSTRAINT fk_is_financeiro_notasfiscais_arquivo_id FOREIGN KEY (arquivo_id) REFERENCES public.is_arquivos(id)
);
CREATE TABLE public.is_financeiro_pdvs (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  titulo character varying NOT NULL,
  telefone character varying,
  logradouro character varying,
  cep character varying,
  complemento character varying,
  bairro character varying,
  cidade character varying,
  estado character varying,
  bling_id character varying,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_financeiro_pdvs_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_financeiro_repeticoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  descricao character varying,
  valor numeric NOT NULL CHECK (valor >= 0::numeric),
  categoria_id uuid NOT NULL,
  obs text,
  tipo integer NOT NULL,
  repeticao integer NOT NULL,
  created_at timestamp without time zone DEFAULT now(),
  carteira_id uuid NOT NULL,
  fornecedor_id uuid,
  pdv_id uuid,
  funcionario_id uuid,
  vendedor_id uuid,
  centro_custo_id uuid,
  CONSTRAINT is_financeiro_repeticoes_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_financeiro_repeticoes_categoria_id FOREIGN KEY (categoria_id) REFERENCES public.is_financeiro_categorias(id),
  CONSTRAINT fk_is_financeiro_repeticoes_carteira_id FOREIGN KEY (carteira_id) REFERENCES public.is_financeiro_carteiras(id),
  CONSTRAINT fk_is_financeiro_repeticoes_fornecedor_id FOREIGN KEY (fornecedor_id) REFERENCES public.is_financeiro_fornecedores(id),
  CONSTRAINT fk_is_financeiro_repeticoes_pdv_id FOREIGN KEY (pdv_id) REFERENCES public.is_financeiro_pdvs(id),
  CONSTRAINT fk_is_financeiro_repeticoes_funcionario_id FOREIGN KEY (funcionario_id) REFERENCES public.is_financeiro_funcionarios(id),
  CONSTRAINT fk_is_financeiro_repeticoes_vendedor_id FOREIGN KEY (vendedor_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_financeiro_repeticoes_centro_custo_id FOREIGN KEY (centro_custo_id) REFERENCES public.is_financeiro_centros_custo(id)
);
CREATE TABLE public.is_financeiro_repeticoes_criadas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  repeticao_id uuid,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_financeiro_repeticoes_criadas_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_financeiro_repeticoes_criadas_repeticao_id FOREIGN KEY (repeticao_id) REFERENCES public.is_financeiro_repeticoes(id)
);
CREATE TABLE public.is_financeiro_transportadoras (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  tipo character varying,
  razao_social character varying,
  ie character varying,
  cnpj character varying,
  nome character varying,
  sobrenome character varying,
  cpf character varying,
  telefone character varying,
  celular character varying,
  cep character varying,
  logradouro character varying,
  numero character varying,
  bairro character varying,
  complemento character varying,
  cidade character varying,
  estado character varying,
  email_log character varying,
  created_at timestamp without time zone DEFAULT now(),
  arquivado boolean DEFAULT false,
  CONSTRAINT is_financeiro_transportadoras_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_mensagens (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  prefixo character varying NOT NULL,
  assunto character varying NOT NULL,
  mensagem text NOT NULL,
  placeholders character varying,
  CONSTRAINT is_mensagens_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_mkt_banners (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  imagem character varying NOT NULL,
  link character varying,
  local character varying NOT NULL,
  target integer NOT NULL,
  imagem_mobile character varying,
  CONSTRAINT is_mkt_banners_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_mkt_cupons (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  cliente_id uuid,
  codigo character varying NOT NULL UNIQUE,
  tipo character varying NOT NULL CHECK (lower(tipo::text) = ANY (ARRAY['percent'::text, 'amount'::text])),
  valor numeric NOT NULL CHECK (valor >= 0::numeric),
  uso integer NOT NULL DEFAULT 0,
  limite integer,
  inicio timestamp without time zone,
  fim timestamp without time zone,
  pedido_min numeric NOT NULL DEFAULT 0,
  primeira_compra boolean NOT NULL DEFAULT false,
  arquivado boolean NOT NULL DEFAULT false,
  CONSTRAINT is_mkt_cupons_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_mkt_cupons_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id)
);
CREATE TABLE public.is_mkt_cupons_produtos (
  cupom_id uuid NOT NULL,
  produto_id uuid NOT NULL,
  CONSTRAINT is_mkt_cupons_produtos_pkey PRIMARY KEY (cupom_id, produto_id),
  CONSTRAINT fk_is_mkt_cupons_produtos_cupom_id FOREIGN KEY (cupom_id) REFERENCES public.is_mkt_cupons(id),
  CONSTRAINT fk_is_mkt_cupons_produtos_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_mkt_regras (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  desconto numeric NOT NULL,
  regra character varying NOT NULL,
  uso integer NOT NULL DEFAULT 0,
  tipo integer NOT NULL,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_mkt_regras_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_paginas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  slug character varying NOT NULL UNIQUE,
  titulo character varying NOT NULL,
  conteudo text NOT NULL,
  redirect_301 character varying,
  meta_title character varying,
  meta_description character varying,
  CONSTRAINT is_paginas_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_pedidos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  cliente_id uuid NOT NULL,
  usuario_id uuid,
  total numeric NOT NULL,
  acrescimo numeric NOT NULL DEFAULT 0,
  desconto numeric NOT NULL DEFAULT 0,
  desconto_uso numeric NOT NULL DEFAULT 0,
  sinal numeric NOT NULL DEFAULT 0,
  frete_valor numeric NOT NULL DEFAULT 0,
  frete_tipo character varying,
  frete_rastreio character varying,
  frete_balcao_id uuid,
  frete_endereco_id uuid,
  origem integer,
  obs text,
  obs_interna text,
  nf text,
  cupom character varying,
  json text,
  pdv_id uuid,
  caixa_id uuid,
  devolucao_completa boolean NOT NULL DEFAULT false,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id),
  CONSTRAINT fk_is_pedidos_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_pedidos_frete_balcao_id FOREIGN KEY (frete_balcao_id) REFERENCES public.is_entregas_balcoes(id),
  CONSTRAINT fk_is_pedidos_frete_endereco_id FOREIGN KEY (frete_endereco_id) REFERENCES public.is_clientes_enderecos(id),
  CONSTRAINT fk_is_pedidos_pdv_id FOREIGN KEY (pdv_id) REFERENCES public.is_financeiro_pdvs(id),
  CONSTRAINT fk_is_pedidos_caixa_id FOREIGN KEY (caixa_id) REFERENCES public.is_financeiro_caixas(id),
  CONSTRAINT fk_is_pedidos_cupom_codigo FOREIGN KEY (cupom) REFERENCES public.is_mkt_cupons(codigo)
);
CREATE TABLE public.is_pedidos_fretes_detalhes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  pedido_id uuid NOT NULL,
  endereco_json jsonb,
  conteudo_json jsonb,
  CONSTRAINT is_pedidos_fretes_detalhes_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_fretes_detalhes_pedido_id FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id)
);
CREATE TABLE public.is_pedidos_fretes_detalhes_enderecos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  detalhe_id uuid NOT NULL UNIQUE,
  pedido_id uuid NOT NULL,
  destinatario_nome text,
  destinatario_documento text,
  destinatario_tipo text,
  destinatario_pais text,
  cep text,
  logradouro text,
  numero text,
  complemento text,
  bairro text,
  cidade text,
  estado text,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_pedidos_fretes_detalhes_enderecos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_enderecos_detalhe FOREIGN KEY (detalhe_id) REFERENCES public.is_pedidos_fretes_detalhes(id),
  CONSTRAINT fk_enderecos_pedido FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id)
);
CREATE TABLE public.is_pedidos_fretes_detalhes_itens (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  detalhe_id uuid NOT NULL,
  pedido_id uuid NOT NULL,
  item_idx integer NOT NULL,
  entrega text,
  gratis boolean,
  legacy_produto_id text,
  quantidade integer,
  volumes integer,
  valor_declarado numeric,
  altura numeric,
  largura numeric,
  comprimento numeric,
  peso text,
  lc integer,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_pedidos_fretes_detalhes_itens_pkey PRIMARY KEY (id),
  CONSTRAINT fk_itens_detalhe FOREIGN KEY (detalhe_id) REFERENCES public.is_pedidos_fretes_detalhes(id),
  CONSTRAINT fk_itens_pedido FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id)
);
CREATE TABLE public.is_pedidos_fretes_detalhes_pacotes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  detalhe_id uuid NOT NULL UNIQUE,
  pedido_id uuid NOT NULL,
  itens_count integer,
  pacote_altura numeric,
  pacote_largura numeric,
  pacote_comprimento numeric,
  pacote_peso numeric,
  pacote_produtos integer,
  pacote_volumes integer,
  pacote_valor_declarado numeric,
  pacote_produtos_ids jsonb,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_pedidos_fretes_detalhes_pacotes_pkey PRIMARY KEY (id),
  CONSTRAINT fk_pacotes_detalhe FOREIGN KEY (detalhe_id) REFERENCES public.is_pedidos_fretes_detalhes(id),
  CONSTRAINT fk_pacotes_pedido FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id)
);
CREATE TABLE public.is_pedidos_fretes_entregas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  pedido_id uuid NOT NULL,
  envio_id uuid UNIQUE,
  metodo_titulo text,
  modulo text,
  prazo_dias integer,
  valor numeric,
  sucesso boolean,
  hash text,
  descricao text,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_pedidos_fretes_entregas_pkey PRIMARY KEY (id),
  CONSTRAINT fk_entregas_pedido FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id),
  CONSTRAINT fk_entregas_envio FOREIGN KEY (envio_id) REFERENCES public.is_pedidos_fretes_envios(id)
);
CREATE TABLE public.is_pedidos_fretes_envios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  pedido_id uuid,
  tipo character varying,
  detalhes_json jsonb,
  CONSTRAINT is_pedidos_fretes_envios_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_fretes_envios_pedido_id FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id)
);
CREATE TABLE public.is_pedidos_fretes_retiradas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  pedido_id uuid NOT NULL,
  envio_id uuid UNIQUE,
  balcao_id uuid,
  balcao_titulo text,
  balcao_telefone text,
  balcao_logradouro text,
  balcao_cep text,
  balcao_complemento text,
  balcao_bairro text,
  cidade text,
  estado text,
  prazo_dias integer,
  data_snapshot timestamp without time zone,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_pedidos_fretes_retiradas_pkey PRIMARY KEY (id),
  CONSTRAINT fk_retiradas_pedido FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id),
  CONSTRAINT fk_retiradas_envio FOREIGN KEY (envio_id) REFERENCES public.is_pedidos_fretes_envios(id),
  CONSTRAINT fk_retiradas_balcao FOREIGN KEY (balcao_id) REFERENCES public.is_entregas_balcoes(id)
);
CREATE TABLE public.is_pedidos_historico (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  pedido_id uuid,
  item_id uuid,
  status_id integer,
  usuario_id uuid,
  obs character varying,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_historico_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_historico_pedido_id FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id),
  CONSTRAINT fk_is_pedidos_historico_item_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(id),
  CONSTRAINT fk_is_pedidos_historico_status_id FOREIGN KEY (status_id) REFERENCES public.is_extras_status(id),
  CONSTRAINT fk_is_pedidos_historico_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id)
);
CREATE TABLE public.is_pedidos_itens (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  pedido_id uuid NOT NULL,
  produto_id uuid,
  descricao character varying,
  status character varying,
  qtde numeric,
  valor numeric,
  arte_valor numeric NOT NULL DEFAULT 0,
  arte_tipo character varying,
  arte_status integer,
  arte_arquivo character varying,
  arte_data timestamp without time zone,
  arte_nome character varying,
  pago boolean NOT NULL DEFAULT false,
  rastreio character varying,
  previsao_producao timestamp without time zone,
  previsao_entrega timestamp without time zone,
  previa character varying,
  origem integer,
  arquivado boolean NOT NULL DEFAULT false,
  data_modificado timestamp without time zone,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  ftp character varying,
  produto_detalhes text,
  formato character varying,
  formato_detalhes text,
  visto integer,
  vars_raw character varying,
  vars_detalhes text,
  json text,
  categoria integer,
  revendedor integer,
  CONSTRAINT is_pedidos_itens_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_itens_pedido_id FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id),
  CONSTRAINT fk_is_pedidos_itens_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_pedidos_itens_brief_alteracoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  item_id uuid NOT NULL,
  frente character varying,
  verso character varying,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_itens_brief_alteracoes_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_itens_brief_alteracoes_item_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(id)
);
CREATE TABLE public.is_pedidos_itens_brief_conversa (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  item_id uuid NOT NULL,
  operador_id uuid,
  mensagem text NOT NULL,
  anexo character varying,
  origem integer,
  visto boolean NOT NULL DEFAULT false,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_itens_brief_conversa_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_itens_brief_conversa_item_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(id),
  CONSTRAINT fk_is_pedidos_itens_brief_conversa_operador_id FOREIGN KEY (operador_id) REFERENCES public.is_usuarios(id)
);
CREATE TABLE public.is_pedidos_itens_briefings (
  item_id uuid NOT NULL,
  site character varying,
  rede_social character varying,
  nome_empresa character varying,
  visual text,
  sobre text,
  cores character varying,
  info text,
  anexos_raw text,
  visto boolean NOT NULL DEFAULT false,
  aprovacao timestamp without time zone,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  CONSTRAINT is_pedidos_itens_briefings_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_itens_briefings_item_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(id)
);
CREATE TABLE public.is_pedidos_itens_briefings_anexos (
  item_id uuid NOT NULL,
  arquivo_id uuid NOT NULL,
  CONSTRAINT is_pedidos_itens_briefings_anexos_pkey PRIMARY KEY (item_id, arquivo_id),
  CONSTRAINT fk_is_pedidos_itens_briefings_anexos_item_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(id),
  CONSTRAINT fk_is_pedidos_itens_briefings_anexos_arquivo_id FOREIGN KEY (arquivo_id) REFERENCES public.is_arquivos(id)
);
CREATE TABLE public.is_pedidos_itens_reprovados (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  item_id uuid,
  motivo text NOT NULL,
  usuario_id uuid,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_itens_reprovados_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_itens_reprovados_item_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(id),
  CONSTRAINT fk_is_pedidos_itens_reprovados_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id)
);
CREATE TABLE public.is_pedidos_itens_servicos (
  item_id uuid NOT NULL,
  servico_id uuid NOT NULL,
  CONSTRAINT is_pedidos_itens_servicos_pkey PRIMARY KEY (item_id, servico_id),
  CONSTRAINT fk_is_pedidos_itens_servicos_item_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(id),
  CONSTRAINT fk_is_pedidos_itens_servicos_servico_id FOREIGN KEY (servico_id) REFERENCES public.is_produtos_servicos(id)
);
CREATE TABLE public.is_pedidos_itens_vars (
  item_id uuid NOT NULL,
  produto_id uuid NOT NULL,
  produto_var_id uuid NOT NULL,
  CONSTRAINT is_pedidos_itens_vars_pkey PRIMARY KEY (item_id, produto_id, produto_var_id),
  CONSTRAINT fk_is_pedidos_itens_vars_item_id_produto_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(id),
  CONSTRAINT fk_is_pedidos_itens_vars_item_id_produto_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_itens(produto_id),
  CONSTRAINT fk_is_pedidos_itens_vars_item_id_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_pedidos_itens(id),
  CONSTRAINT fk_is_pedidos_itens_vars_item_id_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_pedidos_itens(produto_id),
  CONSTRAINT fk_is_pedidos_itens_vars_produto_var_id_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos_vars(id),
  CONSTRAINT fk_is_pedidos_itens_vars_produto_var_id_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos_vars(produto_id),
  CONSTRAINT fk_is_pedidos_itens_vars_produto_var_id_produto_id FOREIGN KEY (produto_var_id) REFERENCES public.is_produtos_vars(id),
  CONSTRAINT fk_is_pedidos_itens_vars_produto_var_id_produto_id FOREIGN KEY (produto_var_id) REFERENCES public.is_produtos_vars(produto_id)
);
CREATE TABLE public.is_pedidos_orcamentos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  cliente_id uuid,
  status integer,
  obs text,
  json text,
  usuario_id uuid,
  pdv_id uuid,
  nome character varying,
  vencimento date,
  pedido_ref character varying,
  subtotal numeric,
  total numeric,
  acrescimo numeric NOT NULL DEFAULT 0,
  desconto numeric NOT NULL DEFAULT 0,
  sinal numeric NOT NULL DEFAULT 0,
  frete_valor numeric NOT NULL DEFAULT 0,
  frete_tipo character varying,
  frete_balcao_id uuid,
  frete_endereco_id uuid,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_orcamentos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_orcamentos_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id),
  CONSTRAINT fk_is_pedidos_orcamentos_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_pedidos_orcamentos_pdv_id FOREIGN KEY (pdv_id) REFERENCES public.is_financeiro_pdvs(id),
  CONSTRAINT fk_is_pedidos_orcamentos_frete_balcao_id FOREIGN KEY (frete_balcao_id) REFERENCES public.is_entregas_balcoes(id),
  CONSTRAINT fk_is_pedidos_orcamentos_frete_endereco_id FOREIGN KEY (frete_endereco_id) REFERENCES public.is_clientes_enderecos(id)
);
CREATE TABLE public.is_pedidos_orcamentos_fretes_detalhes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  orcamento_id uuid,
  endereco text,
  conteudo text,
  CONSTRAINT is_pedidos_orcamentos_fretes_detalhes_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_orcamentos_fretes_detalhes_orcamento_id FOREIGN KEY (orcamento_id) REFERENCES public.is_pedidos_orcamentos(id)
);
CREATE TABLE public.is_pedidos_orcamentos_fretes_envios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  orcamento_id uuid,
  tipo character varying,
  detalhes text,
  CONSTRAINT is_pedidos_orcamentos_fretes_envios_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_orcamentos_fretes_envios_orcamento_id FOREIGN KEY (orcamento_id) REFERENCES public.is_pedidos_orcamentos(id)
);
CREATE TABLE public.is_pedidos_orcamentos_itens (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  orcamento_id uuid NOT NULL,
  produto_id uuid,
  descricao character varying,
  qtde numeric NOT NULL,
  valor numeric NOT NULL,
  arte_valor numeric NOT NULL DEFAULT 0,
  arte_tipo character varying,
  arte_status integer,
  arte_arquivo text,
  arte_data timestamp without time zone,
  arte_nome character varying,
  vars_raw text,
  vars_detalhes text,
  json text,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  formato character varying,
  formato_detalhes text,
  produto_detalhes text,
  CONSTRAINT is_pedidos_orcamentos_itens_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_orcamento_id FOREIGN KEY (orcamento_id) REFERENCES public.is_pedidos_orcamentos(id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_pedidos_orcamentos_itens_servicos (
  item_id uuid NOT NULL,
  servico_id uuid NOT NULL,
  CONSTRAINT is_pedidos_orcamentos_itens_servicos_pkey PRIMARY KEY (item_id, servico_id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_servicos_item_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_orcamentos_itens(id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_servicos_servico_id FOREIGN KEY (servico_id) REFERENCES public.is_produtos_servicos(id)
);
CREATE TABLE public.is_pedidos_orcamentos_itens_vars (
  item_id uuid NOT NULL,
  produto_id uuid NOT NULL,
  produto_var_id uuid NOT NULL,
  CONSTRAINT is_pedidos_orcamentos_itens_vars_pkey PRIMARY KEY (item_id, produto_id, produto_var_id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_vars_item_id_produto_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_orcamentos_itens(id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_vars_item_id_produto_id FOREIGN KEY (item_id) REFERENCES public.is_pedidos_orcamentos_itens(produto_id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_vars_item_id_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_pedidos_orcamentos_itens(id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_vars_item_id_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_pedidos_orcamentos_itens(produto_id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_vars_produto_var_id_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos_vars(id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_vars_produto_var_id_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos_vars(produto_id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_vars_produto_var_id_produto_id FOREIGN KEY (produto_var_id) REFERENCES public.is_produtos_vars(id),
  CONSTRAINT fk_is_pedidos_orcamentos_itens_vars_produto_var_id_produto_id FOREIGN KEY (produto_var_id) REFERENCES public.is_produtos_vars(produto_id)
);
CREATE TABLE public.is_pedidos_orcamentos_pagamentos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  cliente_id uuid NOT NULL,
  orcamento_id uuid NOT NULL,
  forma character varying NOT NULL,
  condicao character varying,
  valor numeric NOT NULL,
  status integer NOT NULL,
  link character varying,
  usuario_id uuid,
  obs text,
  pdv_id uuid,
  vencimento date,
  data_pagto date,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_orcamentos_pagamentos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_orcamentos_pagamentos_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id),
  CONSTRAINT fk_is_pedidos_orcamentos_pagamentos_orcamento_id FOREIGN KEY (orcamento_id) REFERENCES public.is_pedidos_orcamentos(id),
  CONSTRAINT fk_is_pedidos_orcamentos_pagamentos_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_pedidos_orcamentos_pagamentos_pdv_id FOREIGN KEY (pdv_id) REFERENCES public.is_financeiro_pdvs(id)
);
CREATE TABLE public.is_pedidos_pag_reprovados (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  comprovante_id uuid,
  motivo text NOT NULL,
  usuario_id uuid,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_pag_reprovados_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_pag_reprovados_comprovante_id FOREIGN KEY (comprovante_id) REFERENCES public.is_pedidos_pagamentos(id),
  CONSTRAINT fk_is_pedidos_pag_reprovados_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id)
);
CREATE TABLE public.is_pedidos_pagamentos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  cliente_id uuid NOT NULL,
  pedido_id uuid,
  forma character varying NOT NULL,
  condicao character varying,
  valor numeric NOT NULL,
  status integer NOT NULL,
  link character varying,
  visto boolean NOT NULL DEFAULT false,
  saldo_anterior numeric,
  saldo_atual numeric,
  usuario_id uuid,
  obs text,
  uid character varying,
  oculto boolean NOT NULL DEFAULT false,
  pdv_id uuid,
  caixa_id uuid,
  original_id uuid,
  bandeira character varying,
  parcelas_raw character varying,
  parcelas_qtd integer CHECK (parcelas_qtd IS NULL OR parcelas_qtd >= 1),
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  CONSTRAINT is_pedidos_pagamentos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_pedidos_pagamentos_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id),
  CONSTRAINT fk_is_pedidos_pagamentos_pedido_id FOREIGN KEY (pedido_id) REFERENCES public.is_pedidos(id),
  CONSTRAINT fk_is_pedidos_pagamentos_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_pedidos_pagamentos_pdv_id FOREIGN KEY (pdv_id) REFERENCES public.is_financeiro_pdvs(id),
  CONSTRAINT fk_is_pedidos_pagamentos_caixa_id FOREIGN KEY (caixa_id) REFERENCES public.is_financeiro_caixas(id),
  CONSTRAINT fk_is_pedidos_pagamentos_original_id FOREIGN KEY (original_id) REFERENCES public.is_pedidos_pagamentos(id)
);
CREATE TABLE public.is_producao_setores (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome character varying NOT NULL,
  status character varying,
  CONSTRAINT is_producao_setores_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_produtos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  url character varying,
  titulo character varying NOT NULL,
  sku character varying,
  gtin character varying,
  mpn character varying,
  ncm character varying,
  descricao_curta character varying,
  descricao_html text,
  meta_title character varying,
  meta_description character varying,
  valor_arte numeric NOT NULL DEFAULT 0,
  visivel boolean NOT NULL DEFAULT true,
  arte boolean NOT NULL DEFAULT false,
  vendidos integer NOT NULL DEFAULT 0,
  estoque_controlar boolean NOT NULL DEFAULT false,
  estoque_qtde integer NOT NULL DEFAULT 0,
  estoque_condicao character varying,
  oferta_expira date,
  oferta_condicao character varying,
  mostrar character varying,
  entrega character varying,
  arquivado boolean NOT NULL DEFAULT false,
  video character varying,
  categoria_relatorio integer,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  gabarito character varying,
  material character varying,
  revestimento character varying,
  acabamento character varying,
  extras character varying,
  formato character varying,
  prazo character varying,
  cores character varying,
  selo character varying,
  valor character varying,
  redirect_301 character varying,
  brdraw text,
  revenda_tipo integer,
  revenda_desconto numeric,
  vars_select integer,
  vars_obrig boolean,
  vars_agrupadas integer,
  vars_combinacao integer,
  CONSTRAINT is_produtos_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_produtos_avaliacoes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  cliente_id uuid NOT NULL,
  depoimento text NOT NULL,
  nota integer NOT NULL CHECK (nota >= 1 AND nota <= 5),
  status integer NOT NULL DEFAULT 1,
  foto character varying,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_produtos_avaliacoes_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_avaliacoes_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id),
  CONSTRAINT fk_is_produtos_avaliacoes_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id)
);
CREATE TABLE public.is_produtos_categorias (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  parent_id uuid,
  slug character varying UNIQUE,
  chave character varying,
  titulo character varying NOT NULL,
  title character varying,
  description character varying,
  descricao text,
  status integer NOT NULL DEFAULT 1,
  CONSTRAINT is_produtos_categorias_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_categorias_parent_id FOREIGN KEY (parent_id) REFERENCES public.is_produtos_categorias(id)
);
CREATE TABLE public.is_produtos_categorias_extras (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  categoria character varying,
  subcategoria character varying,
  secao character varying,
  subsecao character varying,
  subsubsecao character varying,
  CONSTRAINT is_produtos_categorias_extras_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_categorias_extras_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_categorias_produtos (
  produto_id uuid NOT NULL,
  categoria_id uuid NOT NULL,
  CONSTRAINT is_produtos_categorias_produtos_pkey PRIMARY KEY (produto_id, categoria_id),
  CONSTRAINT fk_is_produtos_categorias_produtos_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id),
  CONSTRAINT fk_is_produtos_categorias_produtos_categoria_id FOREIGN KEY (categoria_id) REFERENCES public.is_produtos_categorias(id)
);
CREATE TABLE public.is_produtos_dem (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  de integer NOT NULL,
  ate integer NOT NULL,
  preco_unit numeric NOT NULL,
  CONSTRAINT is_produtos_dem_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_dem_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_dem_info (
  produto_id uuid NOT NULL,
  l numeric NOT NULL,
  a numeric NOT NULL,
  c numeric NOT NULL,
  peso character varying,
  CONSTRAINT is_produtos_dem_info_pkey PRIMARY KEY (produto_id),
  CONSTRAINT fk_is_produtos_dem_info_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_fixo (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  preco_atual numeric NOT NULL,
  preco_anterior numeric,
  l integer,
  a integer,
  c integer,
  peso character varying,
  alternativo integer,
  CONSTRAINT is_produtos_fixo_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_fixo_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_fixo_regras (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  apartir numeric NOT NULL,
  desconto_tipo integer NOT NULL,
  desconto_valor numeric NOT NULL,
  CONSTRAINT is_produtos_fixo_regras_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_fixo_regras_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_imagens (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  arquivo_id uuid NOT NULL,
  is_principal boolean NOT NULL DEFAULT false,
  ordem integer NOT NULL DEFAULT 0,
  CONSTRAINT is_produtos_imagens_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_imagens_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id),
  CONSTRAINT fk_is_produtos_imagens_arquivo_id FOREIGN KEY (arquivo_id) REFERENCES public.is_arquivos(id)
);
CREATE TABLE public.is_produtos_mt (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  preco_metro numeric NOT NULL,
  preco_min numeric NOT NULL,
  larguras character varying,
  individual boolean DEFAULT false,
  tipo boolean,
  embalagem boolean,
  largura_max integer,
  altura_max integer,
  mt_c_menor integer,
  encaixe numeric,
  l integer,
  a integer,
  c integer,
  peso character varying,
  CONSTRAINT is_produtos_mt_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_mt_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_mt_regras (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  apartir numeric NOT NULL,
  desconto_tipo integer NOT NULL,
  desconto_valor numeric NOT NULL,
  CONSTRAINT is_produtos_mt_regras_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_mt_regras_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_offset (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid,
  qtde integer NOT NULL,
  preco_base numeric NOT NULL,
  incremento character varying,
  CONSTRAINT is_produtos_offset_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_offset_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_qtd (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  qtde character varying NOT NULL,
  preco_anterior numeric,
  preco_atual numeric NOT NULL,
  peso character varying,
  l integer,
  a integer,
  c integer,
  CONSTRAINT is_produtos_qtd_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_qtd_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_relacoes (
  produto_id uuid NOT NULL,
  produto_relacionado_id uuid NOT NULL,
  tipo_relacao character varying NOT NULL CHECK (lower(tipo_relacao::text) = ANY (ARRAY['relacionado'::text, 'complementar'::text])),
  CONSTRAINT is_produtos_relacoes_pkey PRIMARY KEY (produto_id, produto_relacionado_id, tipo_relacao),
  CONSTRAINT fk_is_produtos_relacoes_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id),
  CONSTRAINT fk_is_produtos_relacoes_produto_relacionado_id FOREIGN KEY (produto_relacionado_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_servicos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome character varying NOT NULL,
  descricao character varying,
  valor numeric NOT NULL DEFAULT 0,
  detalhes text,
  CONSTRAINT is_produtos_servicos_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_produtos_servicos_vinculos (
  produto_id uuid NOT NULL,
  servico_id uuid NOT NULL,
  CONSTRAINT is_produtos_servicos_vinculos_pkey PRIMARY KEY (produto_id, servico_id),
  CONSTRAINT fk_is_produtos_servicos_vinculos_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id),
  CONSTRAINT fk_is_produtos_servicos_vinculos_servico_id FOREIGN KEY (servico_id) REFERENCES public.is_produtos_servicos(id)
);
CREATE TABLE public.is_produtos_skus (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid NOT NULL,
  sku character varying NOT NULL UNIQUE,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_produtos_skus_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_skus_produto FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id)
);
CREATE TABLE public.is_produtos_vars (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  produto_id uuid,
  grupo_id uuid,
  opcao character varying NOT NULL,
  nome character varying,
  valor numeric DEFAULT 0,
  estoque integer DEFAULT 0,
  cobranca integer,
  foto character varying,
  cobranca_val integer,
  CONSTRAINT is_produtos_vars_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_produtos_vars_produto_id FOREIGN KEY (produto_id) REFERENCES public.is_produtos(id),
  CONSTRAINT fk_is_produtos_vars_grupo_id FOREIGN KEY (grupo_id) REFERENCES public.is_produtos_vars_nomes(id)
);
CREATE TABLE public.is_produtos_vars_nomes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome character varying NOT NULL,
  texto_exibicao character varying,
  CONSTRAINT is_produtos_vars_nomes_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_usuarios (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  foto character varying,
  nome character varying NOT NULL,
  sobrenome character varying,
  email_log character varying NOT NULL UNIQUE,
  senha_log character varying NOT NULL,
  acesso integer NOT NULL,
  hora_de character varying,
  hora_ate character varying,
  status integer NOT NULL DEFAULT 1,
  ultimo_acesso timestamp without time zone,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  balcao_id uuid,
  pdv_id uuid,
  comissao_tipo integer,
  comissao_valor integer,
  CONSTRAINT is_usuarios_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_usuarios_balcao_id FOREIGN KEY (balcao_id) REFERENCES public.is_entregas_balcoes(id),
  CONSTRAINT fk_is_usuarios_pdv_id FOREIGN KEY (pdv_id) REFERENCES public.is_financeiro_pdvs(id)
);
CREATE TABLE public.is_usuarios_acessos (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  usuario_id uuid,
  email character varying NOT NULL,
  ip character varying NOT NULL,
  navegador character varying,
  sessao_antiga character varying,
  sessao_nova character varying,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_usuarios_acessos_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_usuarios_acessos_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id)
);
CREATE TABLE public.is_usuarios_historico (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  usuario_id uuid,
  cliente_id uuid,
  acao text,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT is_usuarios_historico_pkey PRIMARY KEY (id),
  CONSTRAINT fk_is_usuarios_historico_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_usuarios_historico_cliente_id FOREIGN KEY (cliente_id) REFERENCES public.is_clientes(id)
);
CREATE TABLE public.is_usuarios_paginas (
  usuario_id uuid NOT NULL,
  pagina_id uuid NOT NULL,
  CONSTRAINT is_usuarios_paginas_pkey PRIMARY KEY (usuario_id, pagina_id),
  CONSTRAINT fk_is_usuarios_paginas_usuario_id FOREIGN KEY (usuario_id) REFERENCES public.is_usuarios(id),
  CONSTRAINT fk_is_usuarios_paginas_pagina_id FOREIGN KEY (pagina_id) REFERENCES public.is_paginas(id)
);
CREATE TABLE public.is_usuarios_tentativas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  navegador character varying,
  ip character varying NOT NULL,
  time integer,
  created_at timestamp without time zone DEFAULT now(),
  email character varying NOT NULL,
  senha character varying,
  sessao character varying,
  CONSTRAINT is_usuarios_tentativas_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_visitas (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  data timestamp without time zone,
  pageviews integer NOT NULL,
  visitas integer NOT NULL,
  CONSTRAINT is_visitas_pkey PRIMARY KEY (id)
);
CREATE TABLE public.is_visitas_online (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  id_session character varying NOT NULL,
  inicio timestamp without time zone,
  fim timestamp without time zone,
  ip character varying,
  url character varying,
  navegador character varying,
  origem character varying,
  pageviews integer NOT NULL CHECK (pageviews >= 0),
  CONSTRAINT is_visitas_online_pkey PRIMARY KEY (id)
);
CREATE TABLE public.raw_text_overrides (
  table_name text NOT NULL,
  column_name text NOT NULL,
  pk_json jsonb NOT NULL,
  raw_bytes bytea NOT NULL,
  CONSTRAINT raw_text_overrides_pkey PRIMARY KEY (table_name, column_name, pk_json)
);
