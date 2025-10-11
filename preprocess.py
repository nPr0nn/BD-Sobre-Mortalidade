import pandas as pd
from pathlib import Path
import logging
import numpy as np
import sys
import os

# --- CONFIGURAÇÕES ---
ARQUIVO_CSV_ENTRADA = os.path.join("Data","DO24OPEN.csv")
PASTA_SAIDA = "Tables"
NUMERO_DE_LINHAS = 10000

# --- ARQUIVOS DE CONSULTA (LOOKUP) ---
ARQUIVO_LOOKUP_CID = os.path.join("Codigos", "CID.csv")
ARQUIVO_LOOKUP_CNES = os.path.join("Codigos", "cnes_estabelecimentos.csv")
ARQUIVO_LOOKUP_OCUPACAO = os.path.join("Codigos", "ocupacao.csv")
ARQUIVO_LOOKUP_MUNICIPIO = os.path.join("Codigos", "Municipios.csv")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

COLUNAS_NECESSARIAS = [
    'DTNASC', 'IDADE', 'SEXO', 'RACACOR', 'PESO', 'ESTCIV', 'OCUP', 'CODMUNRES', 'CODMUNNATU', 'ESC2010',
    'DTOBITO', 'HORAOBITO', 'LOCOCOR', 'CODMUNOCOR', 'ASSISTMED', 'NECROPSIA', 'CIRCOBITO', 'OBITOGRAV', 'OBITOPUER', 'CODESTAB', 'TPMORTEOCO',
    'IDADEMAE', 'OCUPMAE', 'GRAVIDEZ', 'ESCMAE2010', 'QTDFILVIVO', 'QTDFILMORT', 'SEMAGESTAC', 'PARTO',
    'DTCADASTRO', 'DTATESTADO', 'LINHAA', 'LINHAB', 'LINHAC', 'LINHAD', 'LINHAII', 'CAUSABAS', 'ATESTANTE', 'ACIDTRAB',
    'DTINVESTIG', 'DTCONINV', 'DTCONCASO', 'FONTEINV', 'TPNIVELINV', 'ALTCAUSA', 'TPPOSTP', 'TPRESGINFO'
]

def carregar_dados_de_consulta():
    """Carrega todos os arquivos de consulta em memória e retorna dicionários de mapeamento."""
    logging.info("Carregando arquivos de consulta (lookup) em memória...")
    mapas = {}
    try:
        df_cid = pd.read_csv(ARQUIVO_LOOKUP_CID, sep=';', dtype=str, encoding='latin1')
        mapas['cid'] = pd.Series(df_cid.descricao_cid.values, index=df_cid.id_cid).to_dict()

        df_ocupacao = pd.read_csv(ARQUIVO_LOOKUP_OCUPACAO, sep=',', dtype=str, encoding='utf-8')
        # CORREÇÃO: Normaliza os códigos de ocupação removendo zeros à esquerda antes de criar o mapa
        df_ocupacao['id_ocupacao_norm'] = df_ocupacao['id_ocupacao'].str.lstrip('0')
        mapas['ocupacao'] = pd.Series(df_ocupacao.descricao_ocupacao.values, index=df_ocupacao.id_ocupacao_norm).to_dict()


        df_cnes = pd.read_csv(ARQUIVO_LOOKUP_CNES, sep=';', dtype=str, encoding='utf-8')
        mapas['cnes'] = pd.Series(df_cnes['NO_FANTASIA'].values, index=df_cnes.CNES).to_dict()
        
        df_municipio = pd.read_csv(ARQUIVO_LOOKUP_MUNICIPIO, sep = ',', dtype=str, encoding='utf-8')
        
        coluna_codigo = 'CÓDIGO DO MUNICÍPIO - IBGE'
        coluna_nome = 'MUNICÍPIO - IBGE'

        if coluna_codigo not in df_municipio.columns or coluna_nome not in df_municipio.columns:
             logging.error(f"ERRO: As colunas '{coluna_codigo}' ou '{coluna_nome}' não foram encontradas no arquivo de municípios.")
             logging.error(f"Colunas encontradas: {df_municipio.columns.tolist()}")
             sys.exit(1)

        df_municipio['codigo_6_digitos'] = df_municipio[coluna_codigo].str.slice(0, 6)
        mapas['municipio'] = pd.Series(df_municipio[coluna_nome].values, index=df_municipio.codigo_6_digitos).to_dict()

        logging.info("Arquivos de consulta carregados com sucesso.")
        return mapas
    except FileNotFoundError as e:
        logging.error(f"ERRO CRÍTICO: Arquivo de consulta não encontrado: {e.filename}. Encerrando o script.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"ERRO CRÍTICO ao ler os arquivos de consulta: {e}. Encerrando o script.")
        sys.exit(1)


def formatar_data(series: pd.Series) -> pd.Series:
    # O '%m' é o código correto para o mês numérico (01-12).
    return pd.to_datetime(series, format='%d%m%Y', errors='coerce').dt.strftime('%Y-%m-%d')

def formatar_hora(series: pd.Series) -> pd.Series:
    series_str = pd.to_numeric(series, errors='coerce').fillna(0).astype(int).astype(str).str.zfill(4)
    return pd.to_datetime(series_str, format='%H%M', errors='coerce').dt.strftime('%H:%M:00')

def transformar_dados(df: pd.DataFrame, pasta_saida: Path, mapas_lookup: dict):
    logging.info("Iniciando a transformação e enriquecimento dos dados...")
    
    # --- 1. Geração das Tabelas de Dimensão Estáticas ---
    (pasta_saida / 'Sexo.csv').parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({'id_sexo': [1, 2, 0, 9], 'descricao_sexo': ['Masculino', 'Feminino', 'Ignorado', 'Ignorado']}).to_csv(pasta_saida / 'Sexo.csv', index=False)
    pd.DataFrame({'id_cor': [1, 2, 3, 4, 5, 9], 'descricao_cor': ['Branca', 'Preta', 'Amarela', 'Parda', 'Indígena', 'Ignorado']}).to_csv(pasta_saida / 'Raca_Cor.csv', index=False)
    pd.DataFrame({'id_situacao_conjugal': [1, 2, 3, 4, 5, 9], 'descricao_conjugal': ['Solteiro', 'Casado', 'Viúvo', 'Separado', 'União estável', 'Ignorado']}).to_csv(pasta_saida / 'Situacao_Conjugal.csv', index=False)
    pd.DataFrame({'id_escolaridade': [0, 1, 2, 3, 4, 5, 9], 'nivel_escolaridade': ['Sem escolaridade', 'Fundamental I', 'Fundamental II', 'Médio', 'Superior incompleto', 'Superior completo', 'Ignorado']}).to_csv(pasta_saida / 'Escolaridade.csv', index=False)
    pd.DataFrame({'id': [1, 2, 3, 9], 'descricao_gravidez': ['Única', 'Dupla', 'Tripla e mais', 'Ignorada']}).to_csv(pasta_saida / 'Tipo_de_Gravidez.csv', index=False)
    pd.DataFrame({'id': [1, 2, 9], 'descricao_parto': ['Vaginal', 'Cesáreo', 'Ignorado']}).to_csv(pasta_saida / 'Tipo_de_Parto.csv', index=False)
    pd.DataFrame({'id_atestante': [1, 2, 3, 4, 5, 9], 'descricao_atestante': ['Assistente', 'Substituto', 'IML', 'SVO', 'Outro', 'Ignorado']}).to_csv(pasta_saida / 'Atestante.csv', index=False)
    pd.DataFrame({'id_acidente': [1, 2, 9], 'descricao_acidente': ['Sim', 'Não', 'Ignorado']}).to_csv(pasta_saida / 'Acidente_de_Trabalho.csv', index=False)
    pd.DataFrame({'id_localidade': [1, 2, 3, 4, 5, 6, 9], 'tipo_localidade': ['Hospital', 'Outros estabelecimentos de saúde', 'Domicílio', 'Via pública', 'Outros', 'Aldeia indígena', 'Ignorado']}).to_csv(pasta_saida / 'Localidade.csv', index=False)
    pd.DataFrame({'id': [1, 2, 3, 4, 9], 'descricao_morte': ['Acidente', 'Suicídio', 'Homicídio', 'Outros', 'Ignorado']}).to_csv(pasta_saida / 'Tipo_de_Morte.csv', index=False)
    pd.DataFrame({'id_assist_medica': [1, 2, 9], 'descricao_assist_med': ['Sim', 'Não', 'Ignorado']}).to_csv(pasta_saida / 'Recebeu_Assist_Medica.csv', index=False)
    pd.DataFrame({'id_necropsia': [1, 2, 9], 'descricao_necropsia': ['Sim', 'Não', 'Ignorado']}).to_csv(pasta_saida / 'Feito_Necropsia.csv', index=False)
    pd.DataFrame({'id_obito_gravidez': [1, 2, 9], 'descricao_obito_gravidez': ['Sim', 'Não', 'Ignorado']}).to_csv(pasta_saida / 'Obito_Gravidez.csv', index=False)
    pd.DataFrame({'id_puerperio': [1, 2, 3, 9], 'descricao_puerperio': ['Sim, até 42 dias', 'Sim, de 43 dias a 1 ano', 'Não', 'Ignorado']}).to_csv(pasta_saida / 'Obito_Puerperio.csv', index=False)
    pd.DataFrame({'id_gestacional': [1, 2, 3, 4, 5, 8, 9], 'descricao_gestacional': ['Na gravidez', 'No parto', 'No abortamento', 'Até 42 dias pós-parto', 'De 43 dias a 1 ano pós-gestação', 'Não ocorreu nestes períodos', 'Ignorado']}).to_csv(pasta_saida / 'Situacao_Gestacional.csv', index=False)
    pd.DataFrame({'id_fonte': [1, 2, 3, 4, 5, 6, 7, 8, 9], 'descricao_fonte': ['Comitê', 'Visita domiciliar', 'Prontuário', 'BD', 'SVO', 'IML', 'Outra', 'Múltiplas', 'Ignorado']}).to_csv(pasta_saida / 'Fonte.csv', index=False)
    pd.DataFrame({'id_investigador': ['E', 'R', 'M'], 'descricao_nivel': ['Estadual', 'Regional', 'Municipal']}).to_csv(pasta_saida / 'Nivel_Investigador.csv', index=False)
    pd.DataFrame({'id_alteracao': [1, 2], 'descricao_alteracao': ['Sim', 'Não']}).to_csv(pasta_saida / 'Alteracao.csv', index=False)
    pd.DataFrame({'id_investigado': [1, 2], 'descricao_investigado': ['Sim', 'Não']}).to_csv(pasta_saida / 'Foi_Investigado.csv', index=False)
    pd.DataFrame({'id_resgate': [1, 2, 3], 'descricao_resgate': ['Não acrescentou/corrigiu', 'Permitiu resgate', 'Permitiu correção']}).to_csv(pasta_saida / 'Resgate.csv', index=False)

    # --- 2. Geração das Tabelas de Dimensão Dinâmicas ---
    logging.info("Gerando e enriquecendo tabelas de dimensão dinâmicas...")
    
    # CORREÇÃO: Normaliza os códigos de ocupação nos dados brutos antes de usá-los
    df['OCUP'] = df['OCUP'].str.lstrip('0')
    df['OCUPMAE'] = df['OCUPMAE'].str.lstrip('0')

    ocupacoes_ids = pd.concat([df['OCUP'], df['OCUPMAE']]).dropna().unique()
    df_ocupacoes = pd.DataFrame({'id_ocupacao': ocupacoes_ids})
    df_ocupacoes['descricao_ocupacao'] = df_ocupacoes['id_ocupacao'].map(mapas_lookup['ocupacao']).fillna('DESCONHECIDO')
    df_ocupacoes.to_csv(pasta_saida / 'Ocupacao.csv', index=False)

    mapa_uf = {
        '11': 'RO', '12': 'AC', '13': 'AM', '14': 'RR', '15': 'PA', '16': 'AP', '17': 'TO',
        '21': 'MA', '22': 'PI', '23': 'CE', '24': 'RN', '25': 'PB', '26': 'PE', '27': 'AL', '28': 'SE', '29': 'BA',
        '31': 'MG', '32': 'ES', '33': 'RJ', '35': 'SP',
        '41': 'PR', '42': 'SC', '43': 'RS',
        '50': 'MS', '51': 'MT', '52': 'GO', '53': 'DF'
    }
    municipios_ids = pd.concat([df['CODMUNRES'], df['CODMUNNATU'], df['CODMUNOCOR']]).dropna().unique()
    df_municipios = pd.DataFrame({'codigo_do_municipio': municipios_ids})
    df_municipios['nome'] = df_municipios['codigo_do_municipio'].map(mapas_lookup['municipio']).fillna('DESCONHECIDO')
    df_municipios['estado'] = df_municipios['codigo_do_municipio'].astype(str).str[:2].map(mapa_uf).fillna('DESCONHECIDO')
    df_municipios.to_csv(pasta_saida / 'Municipio.csv', index=False)

    # --- 3. Geração das Tabelas de Fatos e Relacionadas ---
    logging.info("Gerando tabelas de fatos e relacionadas...")
    
    df['id_sequencial'] = range(1, len(df) + 1)
    
    # --- Normalização dos CIDs ---
    logging.info("Processando e normalizando os CIDs para a tabela 'Atestado_Causa'...")
    
    cid_cols = ['LINHAA', 'LINHAB', 'LINHAC', 'LINHAD', 'LINHAII', 'CAUSABAS']
    
    df_causas = df.melt(
        id_vars=['id_sequencial'], value_vars=cid_cols,
        var_name='linha_original', value_name='cid_raw'
    )
    
    df_causas.dropna(subset=['cid_raw'], inplace=True)
    
    df_causas['cid_id'] = df_causas['cid_raw'].str.split('[/*]')
    df_causas = df_causas.explode('cid_id')

    df_causas['cid_id'] = df_causas['cid_id'].str.strip()

    df_causas['cid_id'] = df_causas['cid_id'].str.replace('X$', '', regex=True)
    
    mapa_linhas = {
        'LINHAA': 'A', 'LINHAB': 'B', 'LINHAC': 'C',
        'LINHAD': 'D', 'LINHAII': 'II', 'CAUSABAS': 'CB'
    }
    df_causas['linha'] = df_causas['linha_original'].map(mapa_linhas)
    
    df_causas_final = df_causas[['id_sequencial', 'cid_id', 'linha']].copy()
    df_causas_final.rename(columns={'id_sequencial': 'atestado_de_obito_id'}, inplace=True)

    df_causas_final = df_causas_final[df_causas_final['cid_id'] != '']
    
    df_causas_final.to_csv(pasta_saida / 'Atestado_Causa.csv', index=False)
    logging.info(f"Tabela 'Atestado_Causa.csv' gerada com {len(df_causas_final)} registros.")

    logging.info("Gerando tabela de dimensão 'CID' a partir dos dados processados...")
    cids_unicos = df_causas_final['cid_id'].dropna().unique()
    df_cids = pd.DataFrame({'id_cid': cids_unicos})
    df_cids['descricao_cid'] = df_cids['id_cid'].map(mapas_lookup['cid']).fillna('DESCONHECIDO')
    df_cids.to_csv(pasta_saida / 'CID.csv', index=False)
    
    # --- Continuação da geração das outras tabelas ---
    df_estab = df[['CODESTAB', 'CODMUNOCOR']].dropna(subset=['CODESTAB']).drop_duplicates().copy()
    df_estab.rename(columns={'CODESTAB': 'codigo_cnes', 'CODMUNOCOR': 'codigo_municipio_id'}, inplace=True)
    df_estab['nome'] = df_estab['codigo_cnes'].astype(str).map(mapas_lookup['cnes']).fillna('NULL')
    df_estab[['codigo_cnes', 'nome', 'codigo_municipio_id']].to_csv(pasta_saida / 'Estabelecimento_de_Saude.csv', index=False)
    
    df_investigacao = pd.DataFrame({
        'id': df['id_sequencial'], 'data_inicio': formatar_data(df['DTINVESTIG']),
        'data_conclusao_invest': formatar_data(df['DTCONINV']), 'data_conclusao_caso': formatar_data(df['DTCONCASO']),
        'fonte_id': pd.to_numeric(df['FONTEINV'], errors='coerce'), 'nivel_investigador': df['TPNIVELINV'],
        'ocorreu_alteracao_id': pd.to_numeric(df['ALTCAUSA'], errors='coerce'), 'foi_investigado': pd.to_numeric(df['TPPOSTP'], errors='coerce'),
        'resgate_de_info': pd.to_numeric(df['TPRESGINFO'], errors='coerce'),
    })
    df_investigacao.to_csv(pasta_saida / 'Investigacao.csv', index=False)

    df_mae = pd.DataFrame({
        'id_mae': df['id_sequencial'], 'idade': pd.to_numeric(df['IDADEMAE'], errors='coerce'), 'ocupacao_habitual': df['OCUPMAE'],
        'tipo_de_gravidez_id': pd.to_numeric(df['GRAVIDEZ'], errors='coerce'), 'escolaridade_nivel_id': pd.to_numeric(df['ESCMAE2010'], errors='coerce'),
        'numero_de_filhos_vivos': pd.to_numeric(df['QTDFILVIVO'], errors='coerce'), 'numero_de_filhos_mortos': pd.to_numeric(df['QTDFILMORT'], errors='coerce'),
        'semanas_gestacao': pd.to_numeric(df['SEMAGESTAC'], errors='coerce'), 'tipo_de_parto_id': pd.to_numeric(df['PARTO'], errors='coerce')
    })
    df_mae.to_csv(pasta_saida / 'Mae.csv', index=False)

    df_atestado = pd.DataFrame({
        'id_atestado_obito': df['id_sequencial'],
        'data_cadastro': formatar_data(df['DTCADASTRO']),
        'data_atestado': formatar_data(df['DTATESTADO']),
        'atestante_id': pd.to_numeric(df['ATESTANTE'], errors='coerce'),
        'acidente_de_trabalho_id': pd.to_numeric(df['ACIDTRAB'], errors='coerce')
    })
    df_atestado.to_csv(pasta_saida / 'Atestado_de_Obito.csv', index=False)

    df_obito = pd.DataFrame({
        'id': df['id_sequencial'], 'atestado_de_obito_id': df['id_sequencial'],
        'local_obito_id': pd.to_numeric(df['LOCOCOR'], errors='coerce'), 'tipo_de_morte_id': pd.to_numeric(df['CIRCOBITO'], errors='coerce'),
        'data_ocorrencia': formatar_data(df['DTOBITO']), 'hora_ocorrencia': formatar_hora(df['HORAOBITO']),
        'codigo_municipio_ocorrencia_id': pd.to_numeric(df['CODMUNOCOR'], errors='coerce'), 'recebeu_assist_med_id': pd.to_numeric(df['ASSISTMED'], errors='coerce'),
        'foi_feita_necrospia_id': pd.to_numeric(df['NECROPSIA'], errors='coerce'), 'obito_gravidez_id': pd.to_numeric(df['OBITOGRAV'], errors='coerce'),
        'obito_puerperio_id': pd.to_numeric(df['OBITOPUER'], errors='coerce'),
        'estabelecimento_de_saude_id': pd.to_numeric(df['CODESTAB'], errors='coerce'),
        'situacao_gestacional': pd.to_numeric(df['TPMORTEOCO'], errors='coerce'), 'investigacao_id': df['id_sequencial']
    })
    df_obito.to_csv(pasta_saida / 'Obito.csv', index=False)

    df_falecido = pd.DataFrame({
        'id': df['id_sequencial'], 'obito_id': df['id_sequencial'],
        'data_nascimento': formatar_data(df['DTNASC']), 'idade_original': df['IDADE'],
        'sexo_id': pd.to_numeric(df['SEXO'].replace({'M': 1, 'F': 2, 'I': 0}), errors='coerce'), 'cor_id': pd.to_numeric(df['RACACOR'], errors='coerce'),
        'peso_ao_nascer': pd.to_numeric(df['PESO'], errors='coerce'), 'situacao_conjugal_id': pd.to_numeric(df['ESTCIV'], errors='coerce'),
        'ocupacao_habitual': df['OCUP'], 'municipio_residencia_id': pd.to_numeric(df['CODMUNRES'], errors='coerce'),
        'municipio_naturalidade_id': pd.to_numeric(df['CODMUNNATU'], errors='coerce'), 'mae_id': df['id_sequencial'],
        'escolaridade_nivel_id': pd.to_numeric(df['ESC2010'], errors='coerce')
    })
    df_falecido.to_csv(pasta_saida / 'Falecido.csv', index=False)
    
    logging.info("Transformação concluída. Todos os arquivos CSV foram gerados.")


def main():
    mapas_lookup = carregar_dados_de_consulta()
    
    pasta_saida_path = Path(PASTA_SAIDA)
    pasta_saida_path.mkdir(exist_ok=True)

    arquivo_entrada_path = Path(ARQUIVO_CSV_ENTRADA)
    if not arquivo_entrada_path.exists():
        logging.error(f"Arquivo de entrada não encontrado: '{arquivo_entrada_path}'")
        return

    try:
        logging.info(f"Lendo as primeiras {NUMERO_DE_LINHAS} linhas de '{ARQUIVO_CSV_ENTRADA}'...")
        df_bruto = pd.read_csv(
            arquivo_entrada_path, sep=';', header=0, nrows=NUMERO_DE_LINHAS,
            dtype=str, encoding='latin1',
            usecols=lambda column: column in COLUNAS_NECESSARIAS
        )
        
        colunas_faltando = set(COLUNAS_NECESSARIAS) - set(df_bruto.columns)
        if colunas_faltando:
            logging.warning(f"Colunas não encontradas no CSV: {colunas_faltando}. Serão preenchidas com nulo.")
            for col in colunas_faltando:
                df_bruto[col] = np.nan

        transformar_dados(df_bruto, pasta_saida_path, mapas_lookup)

        print("\n" + "="*60)
        print("✅ Processo de pré-processamento finalizado com sucesso!")
        print(f"Os arquivos CSV enriquecidos foram salvos na pasta: '{PASTA_SAIDA}'")
        print("="*60)

    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado no processamento: {e}")

if __name__ == '__main__':
    main()
