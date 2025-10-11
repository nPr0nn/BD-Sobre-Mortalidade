import pandas as pd
import numpy as np
from pathlib import Path

# --- CONFIGURAÇÕES ---
PASTA_CSVS = "Tables"
ARQUIVO_SCHEMA = 'schema.sql'
ARQUIVO_SAIDA = 'bdsim.sql'


ORDEM_DE_CARGA = [
    'Sexo', 'Raca_Cor', 'Situacao_Conjugal', 'Escolaridade', 'Tipo_de_Gravidez',
    'Tipo_de_Parto', 'Atestante', 'Acidente_de_Trabalho', 'Localidade',
    'Tipo_de_Morte', 'Recebeu_Assist_Medica', 'Feito_Necropsia', 'Obito_Gravidez',
    'Obito_Puerperio', 'Situacao_Gestacional', 'Fonte', 'Nivel_Investigador',
    'Alteracao', 'Foi_Investigado', 'Resgate',
    'Ocupacao', 'Municipio', 'CID',
    'Mae',
    'Atestado_de_Obito',
    'Atestado_Causa',
    'Investigacao',
    'Estabelecimento_de_Saude',
    'Obito',
    'Falecido'
]

# --- MODIFICAÇÃO PRINCIPAL ---
# O dicionário agora contém a chave primária para TODAS as tabelas do schema.
# Isso aplicará a regra ON CONFLICT para todos os inserts.
COLUNAS_DE_CONFLITO = {
    "Sexo": '"id_sexo"',
    "Raca_Cor": '"id_cor"',
    "Situacao_Conjugal": '"id_situacao_conjugal"',
    "Escolaridade": '"id_escolaridade"',
    "Tipo_de_Gravidez": '"id"',
    "Tipo_de_Parto": '"id"',
    "Atestante": '"id_atestante"',
    "Acidente_de_Trabalho": '"id_acidente"',
    "Localidade": '"id_localidade"',
    "Tipo_de_Morte": '"id"',
    "Recebeu_Assist_Medica": '"id_assist_medica"',
    "Feito_Necropsia": '"id_necropsia"',
    "Obito_Gravidez": '"id_obito_gravidez"',
    "Obito_Puerperio": '"id_puerperio"',
    "Situacao_Gestacional": '"id_gestacional"',
    "Fonte": '"id_fonte"',
    "Nivel_Investigador": '"id_investigador"',
    "Alteracao": '"id_alteracao"',
    "Foi_Investigado": '"id_investigado"',
    "Resgate": '"id_resgate"',
    "Ocupacao": '"id_ocupacao"',
    "Municipio": '"codigo_do_municipio"',
    "CID": '"id_cid"',
    "Mae": '"id_mae"',
    "Atestado_de_Obito": '"id_atestado_obito"',
    "Atestado_Causa": '"atestado_de_obito_id", "cid_id", "linha"',
    "Investigacao": '"id"',
    "Estabelecimento_de_Saude": '"codigo_cnes"',
    "Obito": '"id"',
    "Falecido": '"id"'
}


def formatar_valor_sql(valor, dtype):
    """
    Formata um valor Python para uma string SQL válida, tratando NULOs,
    números e strings (com escape de aspas simples).
    """
    if pd.isna(valor):
        return "NULL"

    if np.issubdtype(dtype, np.number):
        if isinstance(valor, float) and valor.is_integer():
            return str(int(valor))
        return str(valor)

    str_valor = str(valor).replace("'", "''")
    return f"'{str_valor}'"


def gerar_script_sql_com_inserts():
    """
    Gera um único arquivo .sql que cria o schema e insere os dados
    usando comandos INSERT INTO com tratamento de duplicatas para todas as tabelas.
    """
    print(f"Iniciando a geração do arquivo '{ARQUIVO_SAIDA}'...")

    if not Path(ARQUIVO_SCHEMA).exists():
        print(f"ERRO: O arquivo '{ARQUIVO_SCHEMA}' não foi encontrado.")
        return
    if not Path(PASTA_CSVS).exists():
        print(f"ERRO: A pasta '{PASTA_CSVS}' não foi encontrada.")
        return

    with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f_out:
        print(f"Lendo e escrevendo o schema de '{ARQUIVO_SCHEMA}'...")
        with open(ARQUIVO_SCHEMA, 'r', encoding='utf-8') as f_in:
            f_out.write(f_in.read())
            f_out.write("\n\n")

        print("Gerando comandos INSERT para cada arquivo CSV...")
        f_out.write(
            "-- ===================================================================\n")
        f_out.write("-- INÍCIO DA CARGA DE DADOS COM COMANDOS INSERT\n")
        f_out.write(
            "-- ===================================================================\n\n")

        for nome_tabela in ORDEM_DE_CARGA:
            caminho_csv = Path(PASTA_CSVS) / f"{nome_tabela}.csv"

            if not caminho_csv.exists():
                print(f"AVISO: Arquivo '{caminho_csv}' não encontrado. Pulando a tabela '{nome_tabela}'.")
                continue

            print(f"Processando '{caminho_csv}' para a tabela '{nome_tabela}'...")
            f_out.write(f"-- Dados para a tabela: {nome_tabela}\n")

            df = pd.read_csv(caminho_csv)

            if df.empty:
                print(f"AVISO: O arquivo '{caminho_csv}' está vazio.")
                continue
                
            for col in df.columns:
                if 'id' in col.lower():
                    df[col] = pd.to_numeric(df[col], errors='ignore')

            nomes_colunas_sql = ', '.join([f'"{col}"' for col in df.columns])
            insert_inicio = f"INSERT INTO bdsm.{nome_tabela} ({nomes_colunas_sql}) VALUES "
            
            for _, linha in df.iterrows():
                valores_formatados = [formatar_valor_sql(linha[col], df[col].dtype) for col in df.columns]
                valores_sql = ', '.join(valores_formatados)
                
                comando_final = f"{insert_inicio}({valores_sql})"
                
                # A verificação agora funcionará para todas as tabelas definidas no dicionário
                if nome_tabela in COLUNAS_DE_CONFLITO:
                    colunas_conflito = COLUNAS_DE_CONFLITO[nome_tabela]
                    comando_final += f" ON CONFLICT ({colunas_conflito}) DO NOTHING"
                
                f_out.write(f"{comando_final};\n")

            f_out.write("\n")

    print("-" * 50)
    print(f"✅ Arquivo '{ARQUIVO_SAIDA}' gerado com sucesso!")
    print("AVISO: Este arquivo pode ser muito grande e sua execução no banco de dados pode demorar.")
    print("-" * 50)


if __name__ == '__main__':
    gerar_script_sql_com_inserts()
