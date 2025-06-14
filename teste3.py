import os
import time
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process, LLM
from crewai.tools import tool
import sqlite3
import pandas as pd
from datetime import datetime

# Importa a ferramenta RAR do arquivo separado
from tools.rar_tools import RarExtractorTool, create_rar_extractor_tool, check_extraction_tools

# Carrega as variáveis de ambiente
load_dotenv()

import warnings
warnings.filterwarnings("ignore", category=SyntaxWarning)
warnings.filterwarnings("ignore", module="pydantic")

# Configuração da página
st.set_page_config(
    page_title="I2A2 - Análise Inteligente de Notas Fiscais",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuração do LLM
@st.cache_resource
def get_llm():
    return LLM(
        model=os.getenv("MODEL", "gpt-4o-mini"),
        temperature=0.1,
        max_tokens=500,
        top_p=0.9,
        api_key=os.getenv("OPENAI_API_KEY")
    )

LLm = get_llm()

def get_raw_result(result):
    """Extrai o conteúdo raw do resultado do CrewAI."""
    if hasattr(result, 'raw'):
        return result.raw
    elif hasattr(result, 'result'):
        return result.result
    else:
        return str(result)

def get_available_columns(db_path: str) -> dict:
    """Retorna as colunas disponíveis no banco de dados e identifica o tipo"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(notas_fiscais)")
        columns = cursor.fetchall()
        conn.close()
        
        available_columns = [col[1].lower() for col in columns]
        
        # Detecta se é arquivo de cabeçalhos ou itens
        is_header_file = 'valor_nota_fiscal' in available_columns
        is_items_file = 'valor_total' in available_columns and 'descricao_do_produto_servico' in available_columns
        
        return {
            'type': 'header' if is_header_file else 'items' if is_items_file else 'unknown',
            'valor_column': 'valor_nota_fiscal' if is_header_file else 'valor_total' if is_items_file else None,
            'has_products': 'descricao_do_produto_servico' in available_columns,
            'has_quantity': 'quantidade' in available_columns,
            'uf_emitente': 'uf_emitente' in available_columns,
            'razao_social_emitente': 'razao_social_emitente' in available_columns,
            'all_columns': available_columns
        }
        
    except Exception as e:
        return {'type': 'error', 'error': str(e)}

def get_database_statistics(db_path: str) -> dict:
    """Obtém estatísticas adaptadas ao tipo de arquivo"""
    try:
        col_info = get_available_columns(db_path)
        
        if col_info['type'] == 'error':
            return {'error': col_info['error']}
        
        conn = sqlite3.connect(db_path)
        
        # Estatísticas básicas
        total_registros = pd.read_sql_query("SELECT COUNT(*) as total FROM notas_fiscais", conn).iloc[0]['total']
        
        # Valor total (adaptado ao tipo de arquivo)
        valor_total = 0
        if col_info['valor_column']:
            query = f"SELECT SUM({col_info['valor_column']}) as soma FROM notas_fiscais WHERE {col_info['valor_column']} IS NOT NULL"
            result = pd.read_sql_query(query, conn).iloc[0]['soma']
            valor_total = result or 0
        
        # Estados únicos
        estados_unicos = 0
        if col_info['uf_emitente']:
            result = pd.read_sql_query("SELECT COUNT(DISTINCT uf_emitente) as estados FROM notas_fiscais WHERE uf_emitente IS NOT NULL", conn).iloc[0]['estados']
            estados_unicos = result or 0
        
        # Empresas únicas
        empresas_unicas = 0
        if col_info['razao_social_emitente']:
            result = pd.read_sql_query("SELECT COUNT(DISTINCT razao_social_emitente) as empresas FROM notas_fiscais WHERE razao_social_emitente IS NOT NULL", conn).iloc[0]['empresas']
            empresas_unicas = result or 0
        
        # Estatísticas específicas por tipo
        extra_stats = {}
        
        if col_info['type'] == 'header':
            # Para arquivo de cabeçalhos
            extra_stats['tipo'] = 'Arquivo de Cabeçalhos (Notas Fiscais)'
            extra_stats['label_valor'] = 'Valor Total das NFs'
            extra_stats['label_registros'] = 'Total de Notas Fiscais'
            
            # Média por nota fiscal
            if valor_total > 0 and total_registros > 0:
                extra_stats['valor_medio'] = valor_total / total_registros
        
        elif col_info['type'] == 'items':
            # Para arquivo de itens
            extra_stats['tipo'] = 'Arquivo de Itens (Produtos)'
            extra_stats['label_valor'] = 'Valor Total dos Itens'
            extra_stats['label_registros'] = 'Total de Itens'
            
            # Produtos únicos
            if col_info['has_products']:
                result = pd.read_sql_query("SELECT COUNT(DISTINCT descricao_do_produto_servico) as produtos FROM notas_fiscais WHERE descricao_do_produto_servico IS NOT NULL", conn).iloc[0]['produtos']
                extra_stats['produtos_unicos'] = result or 0
        
        else:
            extra_stats['tipo'] = 'Tipo Desconhecido'
            extra_stats['label_valor'] = 'Valor Total'
            extra_stats['label_registros'] = 'Total de Registros'
        
        conn.close()
        
        return {
            'total_registros': total_registros,
            'valor_total': valor_total,
            'estados_unicos': estados_unicos,
            'empresas_unicas': empresas_unicas,
            'column_info': col_info,
            **extra_stats
        }
        
    except Exception as e:
        return {'error': str(e)}

def get_database_schema(db_path: str, info_type: str = "schema") -> str:
    """Função auxiliar para obter informações do esquema"""
    try:
        conn = sqlite3.connect(db_path)
        
        if info_type.lower() == "schema":
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(notas_fiscais)")
            columns = cursor.fetchall()
            
            result = "ESQUEMA DA TABELA 'notas_fiscais':\n\n"
            for col in columns:
                result += f"- {col[1]} ({col[2]})\n"
            
            # Lista também o total de registros
            cursor.execute("SELECT COUNT(*) FROM notas_fiscais")
            total = cursor.fetchone()[0]
            result += f"\nTotal de registros: {total}"
            
            conn.close()
            return result
            
        elif info_type.lower() == "sample":
            df = pd.read_sql_query("SELECT * FROM notas_fiscais LIMIT 3", conn)
            conn.close()
            return f"AMOSTRA DOS DADOS:\n\n{df.to_string(index=False)}"
            
    except Exception as e:
        return f"Erro ao obter informações: {str(e)}"

# Funções SQLite
def execute_sql_query(db_path: str, query: str) -> str:
    """Executa consulta SQL e retorna resultado formatado"""
    try:
        conn = sqlite3.connect(db_path)
        
        if query.strip().upper().startswith('SELECT'):
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                return "Nenhum resultado encontrado."
            
            result = f"Encontrados {len(df)} registros:\n\n"
            result += df.to_string(index=False, max_rows=20)
            
            if len(df) > 20:
                result += f"\n\n... e mais {len(df) - 20} registros."
            
            return result
        else:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()
            return f"Consulta executada. {rows_affected} linhas afetadas."
            
    except Exception as e:
        return f"Erro na consulta: {str(e)}"

def create_database_from_csv(csv_path: str, db_path: str) -> bool:
    """Converte CSV para SQLite com feedback detalhado"""
    try:
        # Carrega CSV
        df = pd.read_csv(csv_path, encoding='utf-8')
        original_rows = len(df)
        
        # Limpa nomes das colunas
        original_columns = df.columns.tolist()
        df.columns = [clean_column_name(col) for col in df.columns]
        cleaned_columns = df.columns.tolist()
        
        # Processa dados
        df = clean_data(df)
        processed_rows = len(df)
        
        # Salva no SQLite
        conn = sqlite3.connect(db_path)
        df.to_sql('notas_fiscais', conn, if_exists='replace', index=False)
        
        # Cria índices básicos
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_data_emissao ON notas_fiscais(data_emissao);",
            "CREATE INDEX IF NOT EXISTS idx_uf_emitente ON notas_fiscais(uf_emitente);",
            "CREATE INDEX IF NOT EXISTS idx_valor_total ON notas_fiscais(valor_total);",
            "CREATE INDEX IF NOT EXISTS idx_produto ON notas_fiscais(descricao_do_produto_servico);"
        ]
        
        indexes_created = 0
        for index in indexes:
            try:
                conn.execute(index)
                indexes_created += 1
            except:
                pass
        
        # Verifica estatísticas finais
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM notas_fiscais")
        final_count = cursor.fetchone()[0]
        
        # Verifica colunas específicas antes de consultar
        available_cols = get_available_columns(db_path)
        
        estados_count = 0
        if available_cols['uf_emitente']:
            cursor.execute("SELECT COUNT(DISTINCT uf_emitente) FROM notas_fiscais WHERE uf_emitente IS NOT NULL")
            estados_count = cursor.fetchone()[0]
        
        total_value = 0
        if available_cols['valor_column']:
            cursor.execute(f"SELECT SUM({available_cols['valor_column']}) FROM notas_fiscais WHERE {available_cols['valor_column']} IS NOT NULL")
            total_value = cursor.fetchone()[0] or 0
        
        conn.close()
        
        # Log de sucesso com estatísticas
        st.info(f"""
        📊 **Processamento concluído:**
        - Registros processados: {final_count:,}
        - Estados únicos: {estados_count}
        - Valor total: R$ {total_value:,.2f}
        - Índices criados: {indexes_created}/4
        - Colunas processadas: {len(cleaned_columns)}
        """)
        
        return True
        
    except Exception as e:
        st.error(f"Erro ao criar banco: {str(e)}")
        return False

def clean_column_name(col_name: str) -> str:
    """Limpa nome da coluna para uso no SQL"""
    return (col_name.lower()
            .replace(' ', '_')
            .replace('/', '_')
            .replace('-', '_')
            .replace('(', '')
            .replace(')', '')
            .replace('ç', 'c')
            .replace('ã', 'a')
            .replace('õ', 'o'))

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepara os dados"""
    
    # Converte data
    if 'data_emissao' in df.columns:
        df['data_emissao'] = pd.to_datetime(df['data_emissao'], errors='coerce')
        df['ano'] = df['data_emissao'].dt.year
        df['mes'] = df['data_emissao'].dt.month
        df['dia_semana'] = df['data_emissao'].dt.day_name()
    
    # Valores numéricos - verifica se as colunas existem antes
    numeric_columns = ['quantidade', 'valor_unitario', 'valor_total', 'valor_nota_fiscal']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

# Tools para CrewAI
def create_database_tools(db_path: str):
    """Cria as tools para acesso ao banco de dados"""
    
    @tool("nf_database_tool")
    def query_database(query: str) -> str:
        """
        Ferramenta para consultas SQL no banco de dados de notas fiscais.
        
        ESQUEMA DINÂMICO DO BANCO:
        
        ARQUIVO DE CABEÇALHOS (se aplicável):
        - chave_de_acesso, modelo, serie, numero
        - natureza_da_operacao, data_emissao 
        - razao_social_emitente, uf_emitente, municipio_emitente
        - nome_destinatario, uf_destinatario
        - valor_nota_fiscal (em vez de valor_total)
        - consumidor_final, presenca_do_comprador
        
        ARQUIVO DE ITENS (se aplicável):
        - chave_de_acesso, data_emissao, ano, mes, dia_semana
        - razao_social_emitente, uf_emitente, municipio_emitente  
        - nome_destinatario, uf_destinatario
        - descricao_do_produto_servico, ncm_sh_tipo_de_produto
        - quantidade, valor_unitario, valor_total
        - cfop, natureza_da_operacao
        
        IMPORTANTE: 
        - Use valor_nota_fiscal para arquivos de cabeçalho
        - Use valor_total para arquivos de itens
        - Nem todos os arquivos têm todas as colunas
        
        Args:
            query: Consulta SQL para executar
            
        Returns:
            Resultado da consulta formatado
        """
        return execute_sql_query(db_path, query)

    @tool("nf_schema_info_tool") 
    def get_schema_info(info_type: str = "schema") -> str:
        """
        Obtém informações sobre o esquema do banco de dados.
        
        Args:
            info_type: Tipo de informação ('schema', 'sample', ou 'columns')
            
        Returns:
            Informações sobre o esquema, dados de exemplo ou detalhes das colunas
        """
        if info_type == "columns":
            col_info = get_available_columns(db_path)
            if col_info['type'] == 'error':
                return f"Erro ao obter colunas: {col_info['error']}"
            
            result = f"TIPO DE ARQUIVO: {col_info['type'].upper()}\n\n"
            result += "COLUNAS DISPONÍVEIS:\n"
            for col in col_info['all_columns']:
                result += f"- {col}\n"
            
            if col_info['type'] == 'header':
                result += "\nNOTA: Este é um arquivo de CABEÇALHOS - use 'valor_nota_fiscal' para valores monetários"
            elif col_info['type'] == 'items':
                result += "\nNOTA: Este é um arquivo de ITENS - use 'valor_total' para valores monetários"
            
            return result
        else:
            return get_database_schema(db_path, info_type)
    
    return query_database, get_schema_info

@st.cache_resource
def create_rar_extractor_agent():
    """Cria o agente de extração RAR com a ferramenta personalizada."""
    rar_tool = create_rar_extractor_tool()
    
    return Agent(
        role='Especialista em Descompactação RAR',
        goal='Descompactar arquivos RAR na pasta dados usando ferramentas especializadas',
        backstory="""
        Você é um especialista em descompactação de arquivos RAR equipado com ferramentas 
        especializadas. Sua função é usar a ferramenta 'rar_extractor' para extrair 
        arquivos RAR na pasta 'dados', sempre verificando se o arquivo existe e 
        fornecendo feedback detalhado sobre o processo.
        
        Quando receber uma tarefa para extrair um arquivo RAR, você deve:
        1. Usar a ferramenta rar_extractor com o caminho do arquivo especificado
        2. A ferramenta criará automaticamente a pasta 'dados' se necessário
        3. Relatar os resultados da operação de forma clara e detalhada
        
        IMPORTANTE: Sempre use a ferramenta rar_extractor para realizar a extração!
        """,
        verbose=False,  
        allow_delegation=False,
        tools=[rar_tool],
        llm=LLm
    )

def create_csv_analyzer_agent(db_path: str):
    """Cria o agente de análise usando SQLite."""
    query_tool, schema_tool = create_database_tools(db_path)
    
    # Detecta o tipo de arquivo para ajustar o backstory
    col_info = get_available_columns(db_path)
    file_type_info = ""
    
    if col_info['type'] == 'header':
        file_type_info = """
        IMPORTANTE: Você está analisando um arquivo de CABEÇALHOS de notas fiscais.
        - Use 'valor_nota_fiscal' para valores monetários (não 'valor_total')
        - Cada registro representa uma NOTA FISCAL completa
        - NÃO há informações de produtos individuais
        - Foque em análises de notas fiscais, empresas, fluxo entre estados
        """
    elif col_info['type'] == 'items':
        file_type_info = """
        IMPORTANTE: Você está analisando um arquivo de ITENS de notas fiscais.
        - Use 'valor_total' e 'valor_unitario' para valores monetários
        - Cada registro representa um ITEM/PRODUTO de uma nota fiscal
        - HÁ informações detalhadas de produtos (descrição, NCM, quantidade)
        - Pode fazer análises de produtos, ranking de vendas por item
        """
    
    return Agent(
        role='Especialista SQL em Dados Fiscais',
        goal='Converter perguntas em consultas SQL precisas no banco de notas fiscais',
        backstory=f"""Você é um especialista em análise de dados fiscais e SQL com profundo 
        conhecimento sobre notas fiscais eletrônicas. Você entende perfeitamente o esquema 
        do banco de dados de notas fiscais e consegue traduzir qualquer pergunta de negócio 
        em consultas SQL otimizadas.
        
        {file_type_info}
        
        Suas especialidades incluem:
        - Consultas de agregação (SUM, COUNT, AVG, GROUP BY)
        - Análises temporais (por data, mês, ano, dia da semana)
        - Análises geográficas (por UF, município)
        - Análises de produtos (quando disponível)
        - Análises de operações (CFOP, natureza da operação)
        
        SEMPRE use get_schema_info com parâmetro 'columns' PRIMEIRO para entender 
        exatamente quais colunas estão disponíveis antes de gerar consultas SQL.""",
        tools=[query_tool, schema_tool],
        verbose=False,
        allow_delegation=False,
        llm=LLm
    )

def create_business_analyst_agent():
    """Cria o agente analista de negócios."""
    return Agent(
        role='Analista de Negócios Fiscais',
        goal='Interpretar resultados de consultas SQL e fornecer insights de negócio relevantes',
        backstory="""Você é um analista de negócios especializado em interpretar 
        dados fiscais para gerar insights estratégicos. Você consegue transformar 
        números em histórias e recomendações práticas.
        
        Suas competências incluem:
        - Interpretação de tendências de vendas
        - Análise de performance por região
        - Identificação de oportunidades de mercado
        - Análise de sazonalidade
        - Recomendações estratégicas baseadas em dados
        - Identificação de riscos e oportunidades
        
        Você sempre fornece contexto e significado aos números apresentados.""",
        verbose=False,
        llm=LLm
    )

def create_extraction_task(rar_filename: str, agent: Agent) -> Task:
    """Cria uma task para extração de RAR."""
    return Task(
        description=f"""
        Use a ferramenta rar_extractor para extrair o arquivo RAR '{rar_filename}' 
        para a pasta 'dados'.
        
        Instruções:
        1. Execute a ferramenta rar_extractor com os seguintes parâmetros:
           - rar_file_path: '{rar_filename}'
           - destination_folder: 'dados'
        
        2. A ferramenta irá:
           - Verificar se o arquivo '{rar_filename}' existe
           - Criar a pasta 'dados' se necessário
           - Extrair todos os arquivos mantendo a estrutura
           - Contar os arquivos extraídos
        
        3. Relate o resultado da operação de forma detalhada
        
        IMPORTANTE: Use APENAS a ferramenta rar_extractor para esta tarefa!
        """,
        expected_output=f"""
        Relatório completo da extração do arquivo '{rar_filename}' contendo:
        - Confirmação de que a ferramenta rar_extractor foi utilizada
        - Status da operação (sucesso ou falha)
        - Caminho completo da pasta de destino
        - Número total de arquivos extraídos
        - Detalhes de qualquer erro encontrado
        - Confirmação de que a pasta 'dados' foi criada/utilizada
        """,
        agent=agent
    )

def create_analysis_task(pergunta: str, sql_agent: Agent, business_agent: Agent) -> tuple:
    """Cria tasks para análise SQL e de negócios."""
    
    sql_task = Task(
        description=f"""
        Pergunta do usuário: "{pergunta}"
        
        Você deve:
        1. Se necessário, use get_schema_info para ver o esquema
        2. Analise a pergunta e identifique dados necessários
        3. Gere consulta SQL apropriada usando query_database
        4. Execute a consulta e organize os resultados
        
        REGRAS:
        - Use nomes corretos das colunas
        - Para análises temporais: ano, mes, dia_semana
        - Para valores monetários: valor_total
        - Para geografia: uf_emitente, uf_destinatario  
        - Para produtos: descricao_do_produto_servico
        - Use LIMIT quando apropriado
        - Use ORDER BY para organizar resultados
        """,
        agent=sql_agent,
        expected_output="Consulta SQL executada com dados organizados"
    )
    
    business_task = Task(
        description=f"""
        Com base nos dados extraídos para "{pergunta}", 
        forneça análise de negócio completa.
        
        Inclua:
        - Resumo dos principais achados
        - Interpretação dos números
        - Insights e tendências
        - Recomendações quando apropriado
        """,
        agent=business_agent,
        expected_output="Análise de negócio com insights práticos",
        context=[sql_task]
    )
    
    return sql_task, business_task

def find_csv_files():
    """Encontra todos os arquivos CSV na pasta dados."""
    dados_path = Path("dados")
    if not dados_path.exists():
        return []
    
    csv_files = list(dados_path.glob("*.csv"))
    return [f.name for f in csv_files]

def find_db_files():
    """Encontra todos os arquivos SQLite na pasta dados."""
    dados_path = Path("dados")
    if not dados_path.exists():
        return []
    
    db_files = list(dados_path.glob("*.db"))
    return [f.name for f in db_files]

def save_uploaded_file(uploaded_file, destination_folder="dados"):
    """Salva o arquivo enviado na pasta especificada."""
    destination_path = Path(destination_folder)
    destination_path.mkdir(parents=True, exist_ok=True)
    
    file_path = destination_path / uploaded_file.name
    
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    return str(file_path)

def execute_with_retry(crew, inputs=None):
    """Executa a crew com retry em caso de rate limit."""
    try:
        if inputs:
            return crew.kickoff(inputs=inputs)
        else:
            return crew.kickoff()
    except Exception as e:
        if "rate_limit_exceeded" in str(e):
            st.warning("⏳ Rate limit excedido. Aguardando 60 segundos...")
            time.sleep(60)
            if inputs:
                return crew.kickoff(inputs=inputs)
            else:
                return crew.kickoff()
        else:
            raise e

def main():
    # Header
    st.title("🗂️ I2A2 - Análise Inteligente de Notas Fiscais")
    st.markdown("### Sistema avançado com SQLite para extração de arquivos RAR e análise de dados de notas fiscais")
    
    # Sidebar
    st.sidebar.title("⚙️ Configurações")
    
    # Verificação da API Key
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        st.sidebar.error("❌ OPENAI_API_KEY não configurada!")
        st.sidebar.info("Configure sua API key no arquivo .env")
        return
    else:
        st.sidebar.success("✅ API Key configurada")
    
    # Status da pasta dados
    dados_path = Path("dados")
    if dados_path.exists():
        csv_files = find_csv_files()
        db_files = find_db_files()
        st.sidebar.info(f"📁 Pasta dados: {len(csv_files)} CSV, {len(db_files)} DB")
        
        # Mostra bancos disponíveis para análise
        if db_files:
            st.sidebar.success("🗄️ **Bancos prontos para análise:**")
            for db_file in db_files:
                st.sidebar.write(f"   📊 {db_file}")
        else:
            st.sidebar.warning("⏳ Nenhum banco pronto ainda")
    else:
        st.sidebar.warning("📁 Pasta dados não existe")
    
    # Verifica ferramentas de extração RAR
    rar_status = check_extraction_tools()
    if rar_status["available"]:
        st.sidebar.success(f"🔧 Ferramenta RAR: {os.path.basename(rar_status['command'])}")
    else:
        st.sidebar.error("🔧 Nenhuma ferramenta RAR encontrada")
        st.sidebar.warning("Instale WinRAR ou 7-Zip")
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📤 Upload & Extração", "📊 Análise", "📋 Histórico"])
    
    with tab1:
        st.header("📤 Upload e Extração de Arquivo RAR")
        
        # Upload do arquivo RAR
        uploaded_rar = st.file_uploader(
            "Selecione um arquivo RAR", 
            type=['rar'],
            help="Selecione o arquivo RAR que contém os dados para análise"
        )
        
        if uploaded_rar is not None:
            st.success(f"✅ Arquivo selecionado: {uploaded_rar.name}")
            
            if st.button("🚀 Descompactar o arquivo", type="primary", key="process_rar_button"):
                
                with st.spinner("Salvando arquivo..."):
                    # Salva o arquivo na pasta dados
                    rar_path = save_uploaded_file(uploaded_rar, "dados")
                    st.success(f"✅ Arquivo salvo em: {rar_path}")
                
                with st.spinner("Extraindo arquivo RAR..."):
                    try:
                        # Cria o agente extrator
                        rar_agent = create_rar_extractor_agent()
                        extraction_task = create_extraction_task(rar_path, rar_agent)
                        
                        extraction_crew = Crew(
                            agents=[rar_agent],
                            tasks=[extraction_task],
                            verbose=False
                        )
                        
                        # Executa a extração
                        extraction_result = extraction_crew.kickoff()
                        
                        # Extrai apenas o conteúdo raw
                        extraction_raw = get_raw_result(extraction_result)
                        
                        # Verifica se a extração foi bem-sucedida
                        success_indicators = [
                            "✅" in extraction_raw,
                            "Sucesso" in extraction_raw,
                            "sucesso" in extraction_raw,
                            "extraídos" in extraction_raw,
                            "Arquivos extraídos" in extraction_raw
                        ]
                        
                        error_indicators = [
                            "❌" in extraction_raw,
                            "Erro" in extraction_raw,
                            "erro" in extraction_raw,
                            "falha" in extraction_raw,
                            "Falha" in extraction_raw
                        ]
                        
                        # Verifica se há arquivos CSV na pasta dados após extração
                        csv_files_after = find_csv_files()
                        extraction_created_files = len(csv_files_after) > 0
                        
                        if any(success_indicators) and not any(error_indicators) or extraction_created_files:
                            st.success("🎉 Extração concluída com sucesso!")
                            
                            if extraction_created_files:
                                st.balloons()
                                st.success(f"📦 Arquivo RAR descompactado com sucesso!")
                                st.info(f"📊 {len(csv_files_after)} arquivo(s) CSV encontrado(s):")
                                
                                for csv_file in csv_files_after:
                                    st.write(f"   📄 {csv_file}")
                                
                                # PROCESSAMENTO AUTOMÁTICO DOS CSVs
                                st.markdown("---")
                                st.info("🔄 **Processando arquivos CSV automaticamente...**")
                                
                                processed_count = 0
                                failed_count = 0
                                
                                for csv_file in csv_files_after:
                                    with st.spinner(f"Processando {csv_file}..."):
                                        csv_path = f"dados/{csv_file}"
                                        db_name = csv_file.replace('.csv', '.db')
                                        db_path = f"dados/{db_name}"
                                        
                                        if create_database_from_csv(csv_path, db_path):
                                            st.success(f"✅ Banco criado: {db_name}")
                                            processed_count += 1
                                            
                                            # Mostra informações do banco
                                            schema_info = get_database_schema(db_path, "schema")
                                            with st.expander(f"📋 Informações do banco {db_name}"):
                                                st.code(schema_info, language="text")
                                        else:
                                            st.error(f"❌ Falha ao processar: {csv_file}")
                                            failed_count += 1
                                
                                # Resumo do processamento
                                st.markdown("---")
                                if processed_count > 0:
                                    st.success(f"🎉 **Processamento concluído!**")
                                    st.success(f"✅ {processed_count} banco(s) SQLite criado(s) com sucesso!")
                                    if failed_count > 0:
                                        st.warning(f"⚠️ {failed_count} arquivo(s) falharam no processamento")
                                    
                                    st.markdown("---")
                                    st.success("✅ **Sistema pronto!** Vá para a aba 'Análise' para fazer perguntas sobre os dados!")
                                else:
                                    st.error("❌ Nenhum arquivo pôde ser processado")
                            
                            with st.expander("📋 Ver detalhes da extração"):
                                st.code(extraction_raw, language="text")
                            
                            st.session_state['extraction_success'] = True
                            st.rerun()
                            
                        else:
                            st.error("❌ Falha na extração")
                            st.code(extraction_raw, language="text")
                            
                    except Exception as e:
                        st.error(f"❌ Erro durante a extração: {str(e)}")
    
    with tab2:
        st.header("📊 Análise Inteligente de Dados")
        
        # Lista os bancos SQLite disponíveis
        db_files = find_db_files()
        
        if not db_files:
            st.warning("🗄️ Nenhum banco de dados encontrado na pasta dados.")
            st.info("Faça o upload e extração de um arquivo RAR primeiro.")
        else:
            # Seleção do banco de dados
            selected_db = st.selectbox(
                "🗄️ Selecione o banco de dados para análise:",
                db_files,
                index=0
            )
            
            db_path = f"dados/{selected_db}"
            
            # Estatísticas rápidas do banco selecionado
            st.markdown("### 📈 Estatísticas Rápidas")
            
            stats = get_database_statistics(db_path)
            
            if 'error' in stats:
                st.warning(f"Não foi possível carregar estatísticas: {stats['error']}")
            else:
                # Mostra o tipo de arquivo
                if 'tipo' in stats:
                    st.info(f"📋 **{stats['tipo']}**")
                
                # Exibe métricas adaptadas
                if stats['column_info']['type'] == 'header':
                    # Métricas para arquivo de cabeçalhos
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("📄 Total de Notas Fiscais", f"{stats['total_registros']:,}")
                    
                    with col2:
                        st.metric("💰 Valor Total das NFs", f"R$ {stats['valor_total']:,.2f}")
                    
                    with col3:
                        st.metric("🗺️ Estados Emitentes", stats['estados_unicos'])
                    
                    with col4:
                        if 'valor_medio' in stats:
                            st.metric("📊 Valor Médio por NF", f"R$ {stats['valor_medio']:,.2f}")
                        else:
                            st.metric("🏢 Empresas", stats['empresas_unicas'])
                
                elif stats['column_info']['type'] == 'items':
                    # Métricas para arquivo de itens
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("📦 Total de Itens", f"{stats['total_registros']:,}")
                    
                    with col2:
                        st.metric("💰 Valor Total dos Itens", f"R$ {stats['valor_total']:,.2f}")
                    
                    with col3:
                        if 'produtos_unicos' in stats:
                            st.metric("📋 Produtos Únicos", stats['produtos_unicos'])
                        else:
                            st.metric("🗺️ Estados", stats['estados_unicos'])
                    
                    with col4:
                        st.metric("🏢 Empresas", stats['empresas_unicas'])
                
                else:
                    # Métricas genéricas
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("📊 Total de Registros", f"{stats['total_registros']:,}")
                    
                    with col2:
                        if stats['valor_total'] > 0:
                            st.metric("💰 Valor Total", f"R$ {stats['valor_total']:,.2f}")
                        else:
                            st.metric("💰 Valor Total", "N/A")
                    
                    with col3:
                        st.metric("🗺️ Estados", stats['estados_unicos'])
                    
                    with col4:
                        st.metric("🏢 Empresas", stats['empresas_unicas'])
            
            st.markdown("---")
            
            # Mostra informações do banco selecionado
            with st.expander("📋 Informações do Banco de Dados"):
                schema_info = get_database_schema(db_path, "schema")
                st.code(schema_info, language="text")
                
                sample_info = get_database_schema(db_path, "sample")
                st.code(sample_info, language="text")
            
            # Campo para a pergunta
            pergunta = st.text_area(
                "❓ Digite sua pergunta sobre os dados:",
                placeholder="Ex: Quais são os 5 produtos mais vendidos por valor total?\nQual foi o faturamento por estado emitente?\nComo estão distribuídas as vendas por mês?",
                height=100
            )
            
            # Sugestões de perguntas adaptadas ao tipo de arquivo
            col_info = get_available_columns(db_path)
            
            st.markdown("💡 **Sugestões de perguntas:**")
            
            col1, col2 = st.columns(2)
            
            if col_info['type'] == 'header':
                # Sugestões para arquivo de cabeçalhos
                with col1:
                    st.markdown("""
                    **📄 Análises de Notas Fiscais:**
                    - Qual foi o valor total das notas fiscais?
                    - Quantas notas fiscais foram emitidas?
                    - Qual é o valor médio por nota fiscal?
                    - Quais empresas emitiram mais notas?
                    
                    **📅 Análises Temporais:**
                    - Como estão distribuídas as emissões por mês?
                    - Qual dia da semana tem mais emissões?
                    - Evolução das emissões ao longo do tempo
                    """)
                
                with col2:
                    st.markdown("""
                    **🗺️ Análises Geográficas:**
                    - Quais estados mais emitem notas fiscais?
                    - Para quais estados as notas são destinadas?
                    - Fluxo de notas fiscais entre estados
                    
                    **🏢 Análises de Empresas:**
                    - Ranking de empresas por valor de notas
                    - Empresas por quantidade de notas emitidas
                    - Análise por natureza da operação
                    """)
                
                # Botões de exemplo para cabeçalhos
                example_questions = [
                    "Qual foi o valor total das notas fiscais?",
                    "Quais empresas emitiram mais notas fiscais?",
                    "Como estão distribuídas as emissões por estado?",
                    "Qual é o valor médio por nota fiscal?"
                ]
            
            elif col_info['type'] == 'items':
                # Sugestões para arquivo de itens
                with col1:
                    st.markdown("""
                    **📦 Análises de Produtos:**
                    - Quais são os 10 produtos mais vendidos?
                    - Produtos com maior valor unitário
                    - Análise por tipo de NCM
                    - Ranking por quantidade vendida
                    
                    **💰 Análises Financeiras:**
                    - Valor total de vendas por produto
                    - Valor médio por item
                    - Produtos mais lucrativos
                    """)
                
                with col2:
                    st.markdown("""
                    **🗺️ Análises Geográficas:**
                    - Vendas por estado de origem
                    - Principais destinos por produto
                    - Fluxo de produtos entre estados
                    
                    **📊 Análises Operacionais:**
                    - Análise por CFOP
                    - Natureza das operações
                    - Quantidade vs Valor
                    """)
                
                # Botões de exemplo para itens
                example_questions = [
                    "Quais são os 5 produtos mais vendidos por valor?",
                    "Qual foi o faturamento total por estado?",
                    "Quais produtos têm maior valor unitário?",
                    "Como estão distribuídas as vendas por NCM?"
                ]
            
            else:
                # Sugestões genéricas
                with col1:
                    st.markdown("""
                    **📊 Análises Básicas:**
                    - Qual o total de registros?
                    - Distribuição por estado
                    - Análise temporal dos dados
                    """)
                
                with col2:
                    st.markdown("""
                    **🔍 Análises Exploratórias:**
                    - Principais empresas
                    - Padrões nos dados
                    - Estatísticas gerais
                    """)
                
                example_questions = [
                    "Quantos registros temos no total?",
                    "Quais são os principais estados?",
                    "Como estão distribuídos os dados?",
                    "Quais são as principais empresas?"
                ]
            
            # Botões de exemplo
            st.markdown("🚀 **Clique para testar:**")
            cols = st.columns(2)
            for i, question in enumerate(example_questions):
                with cols[i % 2]:
                    if st.button(f"💭 {question[:30]}...", key=f"example_{i}"):
                        pergunta = question
                        st.rerun()
            
            # Botão para iniciar a análise
            if st.button("🔍 Analisar Dados", type="primary", key="analyze_button"):
                if not pergunta:
                    st.warning("⚠️ Por favor, digite uma pergunta antes de analisar.")
                else:
                    with st.spinner("🤖 Processando com IA avançada..."):
                        try:
                            # Cria os agentes
                            sql_agent = create_csv_analyzer_agent(db_path)
                            business_agent = create_business_analyst_agent()
                            
                            # Cria as tasks
                            sql_task, business_task = create_analysis_task(pergunta, sql_agent, business_agent)
                            
                            analysis_crew = Crew(
                                name="Tripulação de Análise Inteligente",
                                agents=[sql_agent, business_agent],
                                tasks=[sql_task, business_task],
                                process=Process.sequential,
                                verbose=False
                            )
                            
                            # Executa a análise
                            analysis_result = execute_with_retry(analysis_crew, {"pergunta": pergunta})
                            
                            # Extrai apenas o conteúdo raw
                            analysis_raw = get_raw_result(analysis_result)
                            
                            # Exibe o resultado
                            st.success("✅ Análise concluída!")
                            st.markdown("### 📋 Resultado da Análise:")
                            st.write(analysis_raw)
                            
                            # Salva no histórico
                            if 'analysis_history' not in st.session_state:
                                st.session_state['analysis_history'] = []
                            
                            st.session_state['analysis_history'].append({
                                'pergunta': pergunta,
                                'banco': selected_db,
                                'resultado': analysis_raw,
                                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
                            })
                            
                        except Exception as e:
                            st.error(f"❌ Erro durante a análise: {str(e)}")
                            st.exception(e)
    
    with tab3:
        st.header("📋 Histórico de Análises")
        
        if 'analysis_history' in st.session_state and st.session_state['analysis_history']:
            for i, analysis in enumerate(reversed(st.session_state['analysis_history'])):
                with st.expander(f"📊 Análise {len(st.session_state['analysis_history']) - i} - {analysis['timestamp']}"):
                    st.write(f"**Banco:** {analysis['banco']}")
                    st.write(f"**Pergunta:** {analysis['pergunta']}")
                    st.write(f"**Resultado:**")
                    st.write(analysis['resultado'])
            
            if st.button("🗑️ Limpar Histórico", key="clear_history_button"):
                st.session_state['analysis_history'] = []
                st.success("✅ Histórico limpo!")
                st.rerun()
        else:
            st.info("📝 Nenhuma análise realizada ainda.")
            st.write("As análises aparecerão aqui conforme você for utilizando o sistema.")

if __name__ == "__main__":
    main()