import os
import time
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process, LLM
from crewai_tools import CSVSearchTool

# Importa a ferramenta RAR do arquivo separado
from tools.rar_tools import RarExtractorTool, create_rar_extractor_tool, check_extraction_tools

# Carrega as variáveis de ambiente
load_dotenv()

import warnings
warnings.filterwarnings("ignore", category=SyntaxWarning)
warnings.filterwarnings("ignore", module="pydantic")

# Configuração da página
st.set_page_config(
    page_title="I2A2 - Agentes Autônomos - Atividade de 18-06-2025",
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

def cache_func(args, result):
    return False

def create_csv_analyzer_agent(csv_filename):
    """Cria o agente de análise CSV."""
    csv_path = f"dados/{csv_filename}"
    
    csvTool = CSVSearchTool(
        csv=csv_path,
        description="Ferramenta para pesquisar informações detalhadas dos itens/produtos das notas fiscais, incluindo descrições, quantidades, valores unitários e totais"
    )
    
    CSVSearchTool.cache_function = cache_func  # Desabilita o cache para evitar problemas com CSVs grandes
    return Agent(
        role="Analista de compras",
        goal="Analisar o arquivo csv fornecido e responder a perguntas que serão feitas sobre valores e produtos destas compras",
        backstory="Você é um especialista em análise de compras.",
        tools=[csvTool],
        verbose=False,  
        allow_code_execution=True,
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


def create_analysis_task(pergunta: str, agent: Agent) -> Task:
    """Cria uma task para análise CSV."""
    return Task(
        description=f"""
        Para responder '{pergunta}', execute o seguinte processo:
        1. Use o arquivo csv com as compras para realizar sua análise
        2. Analise TODOS os registros do arquivo csv, verificando os valores e quantidades dos itens comprados.   
        3. Responda à pergunta com base na análise completa
       
        Sempre confirme quantos registros foram analisados.
        """,
        expected_output="Relatório detalhado com a resposta à pergunta e confirmação de análise completa. Apresenta valores, quantidades e informações relevantes dos itens comprados.",
        agent=agent
    )


def find_csv_files():
    """Encontra todos os arquivos CSV na pasta dados."""
    dados_path = Path("dados")
    if not dados_path.exists():
        return []
    
    csv_files = list(dados_path.glob("*.csv"))
    return [f.name for f in csv_files]


def save_uploaded_file(uploaded_file, destination_folder="dados"):
    """Salva o arquivo enviado na pasta especificada."""
    destination_path = Path(destination_folder)
    destination_path.mkdir(parents=True, exist_ok=True)
    
    file_path = destination_path / uploaded_file.name
    
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    return str(file_path)


def execute_with_retry(crew, inputs):
    """Executa a crew com retry em caso de rate limit."""
    try:
        return crew.kickoff(inputs=inputs)
    except Exception as e:
        if "rate_limit_exceeded" in str(e):
            st.warning("⏳ Rate limit excedido. Aguardando 60 segundos...")
            time.sleep(60)
            return crew.kickoff(inputs=inputs)
        else:
            raise e


def main():
    # Header
    #
    st.title("🗂️ I2A2 - Agentes Autônomos - Atividade de 18-06-2025")
    st.markdown("### Sistema integrado para extração de arquivos RAR e análise de dados CSV")
    
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
        st.sidebar.info(f"📁 Pasta dados: {len(csv_files)} arquivo(s) CSV")
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
                        
                        # Verifica se a extração foi bem-sucedida usando o conteúdo raw
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
                            
                            # Mensagem específica sobre a descompactação
                            if extraction_created_files:
                                st.balloons()  # Animação de celebração
                                st.success(f"📦 Arquivo RAR descompactado com sucesso!")
                                st.info(f"📊 {len(csv_files_after)} arquivo(s) CSV encontrado(s) e prontos para análise:")
                                
                                # Lista os arquivos CSV encontrados
                                for csv_file in csv_files_after:
                                    st.write(f"   📄 {csv_file}")
                                
                                st.markdown("---")
                                st.success("✅ **Próximo passo:** Vá para a aba 'Análise' para fazer perguntas sobre os dados!")
                            else:
                                st.warning("⚠️ Extração realizada, mas nenhum arquivo CSV foi encontrado")
                            
                            # Mostra resultado detalhado da extração (apenas raw)
                            with st.expander("📋 Ver detalhes da extração"):
                                st.code(extraction_raw, language="text")
                            
                            # Atualiza a lista de CSVs
                            st.session_state['extraction_success'] = True
                            st.rerun()
                            
                        else:
                            st.error("❌ Falha na extração")
                            st.code(extraction_raw, language="text")
                            
                            # Mostra debug info
                            st.info("🔍 Informações de debug:")
                            st.write(f"Indicadores de sucesso encontrados: {any(success_indicators)}")
                            st.write(f"Indicadores de erro encontrados: {any(error_indicators)}")
                            st.write(f"Arquivos CSV criados: {extraction_created_files}")
                            
                    except Exception as e:
                        st.error(f"❌ Erro durante a extração: {str(e)}")
    
    with tab2:
        st.header("📊 Análise de Dados CSV")
        
        # Lista os arquivos CSV disponíveis
        csv_files = find_csv_files()
        
        if not csv_files:
            st.warning("📁 Nenhum arquivo CSV encontrado na pasta dados.")
            st.info("Faça o upload e extração de um arquivo RAR primeiro.")
        else:
            # Seleção do arquivo CSV
            selected_csv = st.selectbox(
                "📄 Selecione o arquivo CSV para análise:",
                csv_files,
                index=0
            )
            
            # Campo para a pergunta
            pergunta = st.text_input(
                "❓ Digite sua pergunta sobre os dados:",
                placeholder="Ex: Qual a compra mais cara?"
            )
            
            # Botão para iniciar a análise
            if st.button("🔍 Analisar Dados", type="primary", key="analyze_button"):
                if not pergunta:
                    st.warning("⚠️ Por favor, digite uma pergunta antes de analisar.")
                else:
                    with st.spinner("Analisando dados..."):
                        try:
                            # Cria o agente analisador
                            csv_agent = create_csv_analyzer_agent(selected_csv)
                            analysis_task = create_analysis_task(pergunta, csv_agent)
                            
                            analysis_crew = Crew(
                                name="Tripulação de Análise de Dados",
                                agents=[csv_agent],
                                tasks=[analysis_task],
                                process=Process.sequential,
                                verbose=False
                            )
                            
                            # Executa a análise
                            analysis_result = execute_with_retry(analysis_crew, {"pergunta": pergunta})
                            
                            # Extrai apenas o conteúdo raw
                            analysis_raw = get_raw_result(analysis_result)
                            
                            # Exibe o resultado (apenas raw)
                            st.success("✅ Análise concluída!")
                            st.markdown("### 📋 Resultado da Análise:")
                            st.write(analysis_raw)
                            
                            # Salva no histórico (apenas raw)
                            if 'analysis_history' not in st.session_state:
                                st.session_state['analysis_history'] = []
                            
                            st.session_state['analysis_history'].append({
                                'pergunta': pergunta,
                                'arquivo': selected_csv,
                                'resultado': analysis_raw,
                                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
                            })
                            
                        except Exception as e:
                            st.error(f"❌ Erro durante a análise: {str(e)}")    
    
    with tab3:
        st.header("📋 Histórico de Análises")
        
        if 'analysis_history' in st.session_state and st.session_state['analysis_history']:
            for i, analysis in enumerate(reversed(st.session_state['analysis_history'])):
                with st.expander(f"📊 Análise {len(st.session_state['analysis_history']) - i} - {analysis['timestamp']}"):
                    st.write(f"**Arquivo:** {analysis['arquivo']}")
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