CREATE SCHEMA bdsm;

SET search_path TO bdsm ;

/* Realiza o armazenamento das opções de sexo */
CREATE TABLE Sexo (
id_sexo INT PRIMARY KEY,
descricao_sexo VARCHAR (20) NOT NULL
) ;

/* Realiza o armazenamento das opções de raça/cor */
CREATE TABLE Raca_Cor (
id_cor INT PRIMARY KEY,
descricao_cor VARCHAR (20) NOT NULL UNIQUE
) ;

/* Armazena as ocupações de acordo com a Classificação Brasileira de Ocupações (CBO) */
CREATE TABLE Ocupacao (
id_ocupacao VARCHAR (6) PRIMARY KEY,
descricao_ocupacao VARCHAR (255) NOT NULL
) ;

/* Armazena as opções de situação conjugal */
CREATE TABLE Situacao_Conjugal (
id_situacao_conjugal INT PRIMARY KEY,
descricao_conjugal VARCHAR (50) NOT NULL UNIQUE
) ;

/* Armazena os níveis de escolaridade  */
CREATE TABLE Escolaridade (
id_escolaridade INT PRIMARY KEY,
nivel_escolaridade VARCHAR (50) NOT NULL UNIQUE
) ;

/* Armazena informações dos municípios conforme o código de municípios do IBGE */
CREATE TABLE Municipio (
codigo_do_municipio INT PRIMARY KEY,
nome VARCHAR (255) NOT NULL,
estado VARCHAR (2) NOT NULL
) ;

/* Armazena os tipos de gravidez */
CREATE TABLE Tipo_de_Gravidez (
id INT PRIMARY KEY,
descricao_gravidez VARCHAR (50) NOT NULL UNIQUE
) ;

/* Armazena os tipos de parto. */
CREATE TABLE Tipo_de_Parto (
id INT PRIMARY KEY,
descricao_parto VARCHAR (50) NOT NULL UNIQUE
) ;

/* Armazena informações sobre a mãe do falecido. */
CREATE TABLE Mae (
id_mae SERIAL PRIMARY KEY,
idade INT,
ocupacao_habitual VARCHAR (6),
tipo_de_gravidez_id INT,
escolaridade_nivel_id INT,
numero_de_filhos_vivos INT,
numero_de_filhos_mortos INT,
semanas_gestacao INT,
tipo_de_parto_id INT,

CONSTRAINT fk_mae_ocupacao FOREIGN KEY (ocupacao_habitual) REFERENCES Ocupacao (id_ocupacao),
CONSTRAINT fk_mae_tipo_gravidez FOREIGN KEY (tipo_de_gravidez_id) REFERENCES Tipo_de_Gravidez (id),
CONSTRAINT fk_mae_escolaridade FOREIGN KEY (escolaridade_nivel_id) REFERENCES Escolaridade (id_escolaridade),
CONSTRAINT fk_mae_tipo_parto FOREIGN KEY (tipo_de_parto_id) REFERENCES Tipo_de_Parto (id)
) ;

/* Armazena os códigos da Classificação Internacional de Doenças. */
CREATE TABLE CID (
id_cid VARCHAR (5) PRIMARY KEY,
descricao_cid VARCHAR (300) NOT NULL
) ;

/* Armazena as opções de médico atestante. */
CREATE TABLE Atestante (
id_atestante INT PRIMARY KEY,
descricao_atestante VARCHAR (50) NOT NULL UNIQUE
) ;

/* Armazena as opções para acidente de trabalho. */
CREATE TABLE Acidente_de_Trabalho (
id_acidente INT PRIMARY KEY,
descricao_acidente VARCHAR (10) NOT NULL UNIQUE
) ;

/* MODIFICADO: Armazena os dados do atestado de óbito, agora sem as causas diretas. */
CREATE TABLE Atestado_de_Obito (
id_atestado_obito SERIAL PRIMARY KEY,
data_cadastro DATE,
data_atestado DATE,
atestante_id INT,
acidente_de_trabalho_id INT,

CONSTRAINT fk_atestado_atestante FOREIGN KEY (atestante_id) REFERENCES Atestante (id_atestante),
CONSTRAINT fk_atestado_acidente_trabalho FOREIGN KEY (acidente_de_trabalho_id) REFERENCES Acidente_de_Trabalho (id_acidente)
) ;

/* NOVA TABELA: Tabela associativa para armazenar múltiplos CIDs por atestado. */
CREATE TABLE Atestado_Causa (
atestado_de_obito_id INT NOT NULL,
cid_id VARCHAR (5) NOT NULL,
-- Para armazenar 'A', 'B', 'C', 'D', 'II' ou 'CB' (Causa Básica)
linha VARCHAR (3) NOT NULL,

CONSTRAINT pk_atestado_causa PRIMARY KEY (atestado_de_obito_id, cid_id, linha),
CONSTRAINT fk_causa_atestado FOREIGN KEY (atestado_de_obito_id) REFERENCES Atestado_de_Obito (id_atestado_obito),
CONSTRAINT fk_causa_cid FOREIGN KEY (cid_id) REFERENCES CID (id_cid)
) ;


/* Armazena os locais de ocorrência do óbito. */
CREATE TABLE Localidade (
id_localidade INT PRIMARY KEY,
tipo_localidade VARCHAR (50) NOT NULL UNIQUE
) ;

-- Descreve o tipo de morte (acidente, suicídio, etc).
CREATE TABLE Tipo_de_Morte (
id INT PRIMARY KEY,
descricao_morte VARCHAR (50) NOT NULL UNIQUE
) ;

/* Armazena informações dos estabelecimentos de saúde. */
CREATE TABLE Estabelecimento_de_Saude (
codigo_cnes VARCHAR (8) PRIMARY KEY,
nome VARCHAR (255),
codigo_municipio_id INT NOT NULL,

CONSTRAINT fk_est_saude_municipio FOREIGN KEY (codigo_municipio_id) REFERENCES Municipio (codigo_do_municipio)
) ;

/* Opções para a variável se recebeu assistência médica. */
CREATE TABLE Recebeu_Assist_Medica (
id_assist_medica INT PRIMARY KEY,
descricao_assist_med VARCHAR (20) NOT NULL UNIQUE
) ;

/* Opções para a variável se foi feita necropsia. */
CREATE TABLE Feito_Necropsia (
id_necropsia INT PRIMARY KEY,
descricao_necropsia VARCHAR (20) NOT NULL UNIQUE
) ;

/* Opções para a variável de óbito na gravidez. */
CREATE TABLE Obito_Gravidez (
id_obito_gravidez INT PRIMARY KEY,
descricao_obito_gravidez VARCHAR (20) NOT NULL UNIQUE
) ;

/* Opções para a variável de óbito no puerpério. */
CREATE TABLE Obito_Puerperio (
id_puerperio INT PRIMARY KEY,
descricao_puerperio VARCHAR (50) NOT NULL UNIQUE
) ;

/* Opções para a situação gestacional em que ocorreu a morte. */
CREATE TABLE Situacao_Gestacional (
id_gestacional INT PRIMARY KEY,
descricao_gestacional VARCHAR (100) NOT NULL UNIQUE
) ;

/* Armazena as fontes de investigação. */
CREATE TABLE Fonte (
id_fonte INT PRIMARY KEY,
descricao_fonte VARCHAR (100) NOT NULL UNIQUE
) ;

/* Armazena os níveis do investigador. */
CREATE TABLE Nivel_Investigador (
id_investigador CHAR (1) PRIMARY KEY,
descricao_nivel VARCHAR (50) NOT NULL UNIQUE
) ;

/* Indica se houve alteração na causa do óbito. */
CREATE TABLE Alteracao (
id_alteracao INT PRIMARY KEY,
descricao_alteracao VARCHAR (20) NOT NULL UNIQUE
) ;

/* Indica se o óbito foi investigado. */
CREATE TABLE Foi_Investigado (
id_investigado INT PRIMARY KEY,
descricao_investigado VARCHAR (20) NOT NULL UNIQUE
) ;

/* Indica se a investigação permitiu resgate de informações. */
CREATE TABLE Resgate (
id_resgate INT PRIMARY KEY,
descricao_resgate VARCHAR (100) NOT NULL UNIQUE
) ;

/* Armazena os dados da investigação do óbito. */
CREATE TABLE Investigacao (
id SERIAL PRIMARY KEY,
data_inicio DATE,
data_conclusao_invest DATE,
data_conclusao_caso DATE,
fonte_id INT,
nivel_investigador CHAR (1),
ocorreu_alteracao_id INT,
foi_investigado INT,
resgate_de_info INT,

CONSTRAINT fk_investigacao_fonte FOREIGN KEY (fonte_id) REFERENCES Fonte (id_fonte),
CONSTRAINT fk_investigacao_nivel FOREIGN KEY (nivel_investigador) REFERENCES Nivel_Investigador (id_investigador),
CONSTRAINT fk_investigacao_alteracao FOREIGN KEY (ocorreu_alteracao_id) REFERENCES Alteracao (id_alteracao),
CONSTRAINT fk_investigacao_foi_investigado FOREIGN KEY (foi_investigado) REFERENCES Foi_Investigado (id_investigado),
CONSTRAINT fk_investigacao_resgate FOREIGN KEY (resgate_de_info) REFERENCES Resgate (id_resgate)
) ;

/* Tabela central que armazena as informações do evento do óbito. */
CREATE TABLE Obito (
id SERIAL PRIMARY KEY,
atestado_de_obito_id INT NOT NULL,
local_obito_id INT NOT NULL,
tipo_de_morte_id INT,
data_ocorrencia DATE NOT NULL,
hora_ocorrencia TIME,
codigo_municipio_ocorrencia_id INT NOT NULL,
recebeu_assist_med_id INT,
foi_feita_necrospia_id INT,
obito_gravidez_id INT,
obito_puerperio_id INT,
estabelecimento_de_saude_id VARCHAR (8),
situacao_gestacional INT,
investigacao_id INT,

CONSTRAINT fk_obito_atestado FOREIGN KEY (atestado_de_obito_id) REFERENCES Atestado_de_Obito (id_atestado_obito),
CONSTRAINT fk_obito_localidade FOREIGN KEY (local_obito_id) REFERENCES Localidade (id_localidade),
CONSTRAINT fk_obito_tipo_morte FOREIGN KEY (tipo_de_morte_id) REFERENCES Tipo_de_Morte (id),
CONSTRAINT fk_obito_municipio FOREIGN KEY (codigo_municipio_ocorrencia_id) REFERENCES Municipio (codigo_do_municipio),
CONSTRAINT fk_obito_assist_med FOREIGN KEY (recebeu_assist_med_id) REFERENCES Recebeu_Assist_Medica (id_assist_medica),
CONSTRAINT fk_obito_necropsia FOREIGN KEY (foi_feita_necrospia_id) REFERENCES Feito_Necropsia (id_necropsia),
CONSTRAINT fk_obito_gravidez FOREIGN KEY (obito_gravidez_id) REFERENCES Obito_Gravidez (id_obito_gravidez),
CONSTRAINT fk_obito_puerperio FOREIGN KEY (obito_puerperio_id) REFERENCES Obito_Puerperio (id_puerperio),
CONSTRAINT fk_obito_est_saude FOREIGN KEY (estabelecimento_de_saude_id) REFERENCES Estabelecimento_de_Saude (codigo_cnes),
CONSTRAINT fk_obito_sit_gestacional FOREIGN KEY (situacao_gestacional) REFERENCES Situacao_Gestacional (id_gestacional),
CONSTRAINT fk_obito_investigacao FOREIGN KEY (investigacao_id) REFERENCES Investigacao (id)
) ;

/* Tabela com as informações demográficas do falecido. */
CREATE TABLE Falecido (
id SERIAL PRIMARY KEY,
obito_id INT NOT NULL UNIQUE,
data_nascimento DATE,
idade_original VARCHAR (3),
sexo_id INT NOT NULL,
cor_id INT,
peso_ao_nascer INT,
situacao_conjugal_id INT,
ocupacao_habitual VARCHAR (6),
municipio_residencia_id INT,
municipio_naturalidade_id INT,
mae_id INT,
escolaridade_nivel_id INT,

CONSTRAINT fk_falecido_obito FOREIGN KEY (obito_id) REFERENCES Obito (id),
CONSTRAINT fk_falecido_sexo FOREIGN KEY (sexo_id) REFERENCES Sexo (id_sexo),
CONSTRAINT fk_falecido_raca_cor FOREIGN KEY (cor_id) REFERENCES Raca_Cor (id_cor),
CONSTRAINT fk_falecido_situacao_conjugal FOREIGN KEY (situacao_conjugal_id) REFERENCES Situacao_Conjugal (id_situacao_conjugal),
CONSTRAINT fk_falecido_ocupacao FOREIGN KEY (ocupacao_habitual) REFERENCES Ocupacao (id_ocupacao),
CONSTRAINT fk_falecido_mun_residencia FOREIGN KEY (municipio_residencia_id) REFERENCES Municipio (codigo_do_municipio),
CONSTRAINT fk_falecido_mun_naturalidade FOREIGN KEY (municipio_naturalidade_id) REFERENCES Municipio (codigo_do_municipio),
CONSTRAINT fk_falecido_mae FOREIGN KEY (mae_id) REFERENCES Mae (id_mae),
CONSTRAINT fk_falecido_escolaridade FOREIGN KEY (escolaridade_nivel_id) REFERENCES Escolaridade (id_escolaridade)
) ;
