"""
Ferramentas para extração de arquivos RAR
Arquivo: rar_tools.py
"""

import os
import subprocess
from pathlib import Path
from typing import Type, Optional
from pydantic import BaseModel, Field
from crewai.tools import BaseTool


class RarExtractorInput(BaseModel):
    """Schema de entrada para a ferramenta de extração RAR."""
    rar_file_path: str = Field(..., description="Caminho para o arquivo RAR")
    destination_folder: str = Field(default="dados", description="Pasta de destino")


class RarExtractorTool(BaseTool):
    """Ferramenta personalizada para descompactar arquivos RAR."""
    
    name: str = "rar_extractor"
    description: str = "Descompacta arquivos RAR na pasta dados, criando a pasta se necessário"
    args_schema: Type[BaseModel] = RarExtractorInput
    
    def _run(self, rar_file_path: str, destination_folder: str = "dados") -> str:
        """Executa a descompactação do arquivo RAR."""
        try:
            # Verifica se o arquivo RAR existe
            if not os.path.exists(rar_file_path):
                return f"❌ Erro: Arquivo RAR não encontrado: {rar_file_path}"
            
            # Verifica se é um arquivo RAR válido
            if not rar_file_path.lower().endswith('.rar'):
                return f"❌ Erro: O arquivo não possui extensão .rar: {rar_file_path}"
            
            # Cria a pasta de destino se não existir
            destination_path = Path(destination_folder)
            destination_path.mkdir(parents=True, exist_ok=True)
            
            # Verifica se o comando unrar está disponível
            unrar_cmd = self._find_unrar_command()
            if not unrar_cmd:
                return "❌ Erro: Comando para descompactar RAR não encontrado. Instale o WinRAR ou 7-Zip"
            
            # Executa a descompactação
            if "winrar" in unrar_cmd.lower() or "rar.exe" in unrar_cmd.lower():
                cmd = [unrar_cmd, 'x', '-y', rar_file_path, str(destination_path) + '\\']
            elif "7z" in unrar_cmd.lower():
                cmd = [unrar_cmd, 'x', f'-o{destination_path}', '-y', rar_file_path]
            else:
                cmd = [unrar_cmd, 'x', '-y', rar_file_path, str(destination_path) + '/']
            
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            
            if result.returncode == 0:
                # Conta os arquivos extraídos
                extracted_files = list(destination_path.rglob('*'))
                file_count = len([f for f in extracted_files if f.is_file()])
                
                return f"✅ Sucesso: Arquivo RAR descompactado com sucesso!\n" \
                       f"📁 Pasta de destino: {destination_path.absolute()}\n" \
                       f"📄 Arquivos extraídos: {file_count}\n" \
                       f"🔧 Ferramenta CrewAI executada corretamente!"
            else:
                return f"❌ Erro na descompactação:\nCódigo: {result.returncode}\nErro: {result.stderr}"
                
        except Exception as e:
            return f"❌ Erro inesperado: {str(e)}"
    
    def _find_unrar_command(self) -> Optional[str]:
        """Encontra o comando apropriado para descompactar RAR no sistema."""
        possible_commands = [
            'unrar',
            'rar',
            '7z',
            'C:\\Program Files\\WinRAR\\WinRAR.exe',
            'C:\\Program Files (x86)\\WinRAR\\WinRAR.exe',
            'C:\\Program Files\\WinRAR\\Rar.exe',
            'C:\\Program Files (x86)\\WinRAR\\Rar.exe',
            'C:\\Program Files\\7-Zip\\7z.exe',
            'C:\\Program Files (x86)\\7-Zip\\7z.exe',
        ]
        
        for cmd in possible_commands:
            try:
                if os.path.exists(cmd):
                    return cmd
                else:
                    result = subprocess.run([cmd], capture_output=True, check=False)
                    if result.returncode != 127:
                        return cmd
            except (FileNotFoundError, subprocess.SubprocessError):
                continue
        
        return None


def create_rar_extractor_tool():
    """
    Factory function para criar uma instância da ferramenta RAR.
    
    Returns:
        RarExtractorTool: Instância da ferramenta de extração RAR
    """
    return RarExtractorTool()


# Função de conveniência para uso direto
def extract_rar_file(rar_path: str, destination: str = "dados") -> str:
    """
    Função de conveniência para extrair arquivo RAR diretamente.
    
    Args:
        rar_path (str): Caminho para o arquivo RAR
        destination (str): Pasta de destino (padrão: "dados")
    
    Returns:
        str: Resultado da extração
    """
    tool = RarExtractorTool()
    return tool._run(rar_path, destination)


# Função para verificar se as ferramentas de extração estão disponíveis
def check_extraction_tools() -> dict:
    """
    Verifica quais ferramentas de extração RAR estão disponíveis no sistema.
    
    Returns:
        dict: Dicionário com status das ferramentas
    """
    tool = RarExtractorTool()
    unrar_cmd = tool._find_unrar_command()
    
    return {
        "available": unrar_cmd is not None,
        "command": unrar_cmd,
        "tools_found": [
            cmd for cmd in [
                'unrar', 'rar', '7z',
                'C:\\Program Files\\WinRAR\\WinRAR.exe',
                'C:\\Program Files (x86)\\WinRAR\\WinRAR.exe',
                'C:\\Program Files\\7-Zip\\7z.exe'
            ] if os.path.exists(cmd)
        ]
    }


if __name__ == "__main__":
    # Teste básico da ferramenta
    print("🔧 Testando ferramenta RAR...")
    
    # Verifica ferramentas disponíveis
    status = check_extraction_tools()
    print(f"Status das ferramentas: {status}")
    
    # Teste de extração (descomente para testar com arquivo real)
    # result = extract_rar_file("test.rar")
    # print(f"Resultado: {result}")