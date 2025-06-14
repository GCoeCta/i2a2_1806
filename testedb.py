#!/usr/bin/env python3
"""
Versão simplificada para teste - Sistema SQLite + CrewAI para Notas Fiscais
"""

import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()
import sqlite3
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any
from crewai import Agent, Task, Crew, Process, LLM

from crewai.tools import tool

# Variável global para o caminho do banco
DB_PATH = "notas_fiscais.db"

# Configuração do LLM
def get_llm():
    return LLM(
        model=os.getenv("MODEL", "gpt-4o-mini"),
        temperature=0.1,
        max_tokens=500,
        top_p=0.9,
        api_key=os.getenv("OPENAI_API_KEY")
    )

LLm = get_llm()

def execute_sql_query(query: str) -> str:
    """Função auxiliar para executar queries SQL"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
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

def get_database_schema(info_type: str = "schema") -> str:
    """Função auxiliar para obter informações do esquema"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        if info_type.lower() == "schema":
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(notas_fiscais)")
            columns = cursor.fetchall()
            
            result = "ESQUEMA DA TABELA 'notas_fiscais':\n\n"
            for col in columns:
                result += f"- {col[1]} ({col[2]})\n"
            
            conn.close()
            return result
            
        elif info_type.lower() == "sample":
            df = pd.read_sql_query("SELECT * FROM notas_fiscais LIMIT 3", conn)
            conn.close()
            return f"AMOSTRA DOS DADOS:\n\n{df.to_string(index=False)}"
            
    except Exception as e:
        return f"Erro ao obter informações: {str(e)}"

@tool("nf_database_tool")
def query_database(query: str) -> str:
    """
    Ferramenta para consultas SQL no banco de dados de notas fiscais.
    
    ESQUEMA DO BANCO:
    Tabela: notas_fiscais
    Principais colunas:
    - chave_de_acesso, data_emissao, ano, mes, dia_semana
    - razao_social_emitente, uf_emitente, municipio_emitente  
    - nome_destinatario, uf_destinatario
    - descricao_do_produto_servico, ncm_sh_tipo_de_produto
    - quantidade, valor_unitario, valor_total
    - cfop, natureza_da_operacao
    
    Args:
        query: Consulta SQL para executar
        
    Returns:
        Resultado da consulta formatado
    """
    return execute_sql_query(query)

@tool("nf_schema_info_tool") 
def get_schema_info(info_type: str = "schema") -> str:
    """
    Obtém informações sobre o esquema do banco de dados.
    
    Args:
        info_type: Tipo de informação ('schema' ou 'sample')
        
    Returns:
        Informações sobre o esquema ou dados de exemplo
    """
    return get_database_schema(info_type)

class SimpleNFAnalyzer:
    """Versão simplificada do analisador de notas fiscais"""
    
    def __init__(self, csv_path: str, db_path: str = "notas_fiscais.db"):
        self.csv_path = csv_path
        self.db_path = db_path
        global DB_PATH
        DB_PATH = db_path
    
    def setup_database(self) -> bool:
        """Converte CSV para SQLite"""
        try:
            print("📊 Carregando CSV...")
            
            # Carrega CSV
            df = pd.read_csv(self.csv_path, encoding='utf-8')
            print(f"✅ CSV carregado: {len(df)} registros")
            
            # Limpa nomes das colunas
            df.columns = [self._clean_column_name(col) for col in df.columns]
            
            # Processa dados
            df = self._clean_data(df)
            
            # Salva no SQLite
            conn = sqlite3.connect(self.db_path)
            df.to_sql('notas_fiscais', conn, if_exists='replace', index=False)
            
            # Cria índices básicos
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_data_emissao ON notas_fiscais(data_emissao);",
                "CREATE INDEX IF NOT EXISTS idx_uf_emitente ON notas_fiscais(uf_emitente);",
                "CREATE INDEX IF NOT EXISTS idx_valor_total ON notas_fiscais(valor_total);"
            ]
            
            for index in indexes:
                try:
                    conn.execute(index)
                except:
                    pass
            
            conn.close()
            print(f"✅ Banco SQLite criado: {self.db_path}")
            return True
            
        except Exception as e:
            print(f"❌ Erro: {str(e)}")
            return False
    
    def _clean_column_name(self, col_name: str) -> str:
        """Limpa nome da coluna"""
        return (col_name.lower()
                .replace(' ', '_')
                .replace('/', '_')
                .replace('-', '_')
                .replace('(', '')
                .replace(')', '')
                .replace('ç', 'c')
                .replace('ã', 'a')
                .replace('õ', 'o'))
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepara os dados"""
        
        # Converte data
        if 'data_emissao' in df.columns:
            df['data_emissao'] = pd.to_datetime(df['data_emissao'], errors='coerce')
            df['ano'] = df['data_emissao'].dt.year
            df['mes'] = df['data_emissao'].dt.month
            df['dia_semana'] = df['data_emissao'].dt.day_name()
        
        # Valores numéricos
        numeric_columns = ['quantidade', 'valor_unitario', 'valor_total']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
    
    def analyze_question(self, user_question: str) -> str:
        """Analisa pergunta do usuário"""
        
        # Cria agentes
        sql_expert = Agent(
            role='Especialista SQL',
            goal='Converter perguntas em consultas SQL para dados de notas fiscais',
            backstory="""Você é especialista em SQL e dados fiscais. 
            Converte perguntas naturais em consultas SQL precisas.
            
            IMPORTANTE: Use sempre os nomes corretos das colunas conforme o esquema.
            Para datas use: data_emissao, ano, mes, dia_semana
            Para valores: valor_total, valor_unitario, quantidade
            Para geografia: uf_emitente, uf_destinatario
            Para produtos: descricao_do_produto_servico""",
            tools=[query_database, get_schema_info],
            verbose=True,
            llm=LLm
        )
        
        business_analyst = Agent(
            role='Analista de Negócios',
            goal='Interpretar dados e fornecer insights estratégicos',
            backstory="""Você interpreta resultados de consultas SQL e 
            gera insights de negócio relevantes sobre dados fiscais.""",
            verbose=True
        )
        
        # Cria tasks
        sql_task = Task(
            description=f"""
            Pergunta do usuário: "{user_question}"
            
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
            agent=sql_expert,
            expected_output="Consulta SQL executada com dados organizados"
        )
        
        business_task = Task(
            description=f"""
            Com base nos dados extraídos para "{user_question}", 
            forneça análise de negócio completa.
            
            Inclua:
            - Resumo dos principais achados
            - Interpretação dos números
            - Insights e tendências
            - Recomendações quando apropriado
            """,
            agent=business_analyst,
            expected_output="Análise de negócio com insights práticos",
            context=[sql_task]
        )
        
        # Executa
        crew = Crew(
            agents=[sql_expert, business_analyst],
            tasks=[sql_task, business_task],
            verbose=True
        )
        
        result = crew.kickoff()
        return str(result)
    
    def quick_query(self, sql: str) -> str:
        """Executa SQL direto usando a função auxiliar"""
        return execute_sql_query(sql)
    
    def show_schema(self) -> str:
        """Mostra esquema do banco"""
        return get_database_schema("schema")
    
    def show_sample(self) -> str:
        """Mostra amostra dos dados"""
        return get_database_schema("sample")

def interactive_mode():
    """Modo interativo para fazer perguntas"""
    csv_path = "dados/202401_NFs_Itens.csv"
    
    if not os.path.exists(csv_path):
        print(f"❌ Arquivo não encontrado: {csv_path}")
        return
    
    analyzer = SimpleNFAnalyzer(csv_path)
    
    if not analyzer.setup_database():
        return
    
    print("\n" + "="*60)
    print("🎯 MODO INTERATIVO - ANÁLISE DE NOTAS FISCAIS")
    print("="*60)
    print("💡 Digite suas perguntas ou comandos:")
    print("   /schema - Ver estrutura do banco")
    print("   /sample - Ver amostra dos dados")
    print("   /sql <consulta> - SQL direto")
    print("   /quit - Sair")
    print("="*60)
    
    while True:
        try:
            user_input = input("\n🔍 Sua pergunta: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['/quit', 'quit', 'exit']:
                print("👋 Até logo!")
                break
            
            elif user_input.lower() == '/schema':
                print("\n📋 ESQUEMA DO BANCO:")
                print(analyzer.show_schema())
            
            elif user_input.lower() == '/sample':
                print("\n📊 AMOSTRA DOS DADOS:")
                print(analyzer.show_sample())
            
            elif user_input.startswith('/sql '):
                sql_query = user_input[5:]
                print(f"\n🔍 Executando: {sql_query}")
                print(analyzer.quick_query(sql_query))
            
            else:
                print(f"\n🤖 Analisando: {user_input}")
                print("(Processando com CrewAI...)")
                try:
                    result = analyzer.analyze_question(user_input)
                    print(f"\n✅ RESULTADO:\n{result}")
                except Exception as e:
                    print(f"❌ Erro na análise: {e}")
                    
        except KeyboardInterrupt:
            print("\n\n👋 Até logo!")
            break
        except Exception as e:
            print(f"❌ Erro: {e}")

def main():
    """Teste básico"""
    csv_path = "dados/202401_NFs_Itens.csv"
    
    if not os.path.exists(csv_path):
        print(f"❌ Arquivo não encontrado: {csv_path}")
        return
    
    # Inicializa
    analyzer = SimpleNFAnalyzer(csv_path)
    
    # Setup banco
    print("🚀 Configurando sistema...")
    if not analyzer.setup_database():
        return
    
    print("\n✅ Sistema pronto!")
    
    # Teste básico
    print("\n🔍 Teste básico:")
    result = analyzer.quick_query("SELECT COUNT(*) as total_registros FROM notas_fiscais")
    print(result)
    
    # Mostra esquema
    print("\n📋 Esquema do banco:")
    print(analyzer.show_schema())
    
    # Exemplo de análise
    print("\n❓ Exemplo de análise:")
    try:
        question = "Quantos registros temos no total?"
        print(f"Pergunta: {question}")
        answer = analyzer.analyze_question(question)
        print(f"\n✅ Resposta:\n{answer}")
    except Exception as e:
        print(f"❌ Erro na análise: {e}")
    
    print("\n" + "="*60)
    print("🎯 SISTEMA FUNCIONANDO!")
    print("="*60)
    print("Agora você pode:")
    print("1. analyzer.analyze_question('sua pergunta') - Análise com IA")
    print("2. analyzer.quick_query('SELECT ...') - SQL direto")
    print("3. interactive_mode() - Modo interativo")
    print("="*60)
    
    # Pergunta se quer modo interativo
    try:
        choice = input("\nDeseja entrar no modo interativo? (s/n): ").lower()
        if choice in ['s', 'sim', 'y', 'yes']:
            interactive_mode()
    except:
        pass

if __name__ == "__main__":
    main()