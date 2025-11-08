-- 1. Causas mais frequentes de óbito, por sexo biologico - CID
SELECT
    s.descricao_sexo,
    c.descricao_cid,
    COUNT(*) AS total_obitos
FROM
    bdsm.falecido f
JOIN bdsm.sexo s ON f.sexo_id = s.id_sexo
JOIN bdsm.obito o ON f.obito_id = o.id
JOIN bdsm.atestado_causa ac ON o.atestado_de_obito_id = ac.atestado_de_obito_id
JOIN bdsm.cid c ON ac.cid_id = c.id_cid
WHERE ac.linha = 'CB'
GROUP BY s.descricao_sexo, c.descricao_cid
ORDER BY total_obitos DESC
LIMIT 10;

-- 2. Análise geográfica de disparidades
SELECT
    m.nome AS municipio,
    m.estado,
    COUNT(DISTINCT f.id) AS total_obitos,
    COUNT(DISTINCT CASE WHEN e.id_escolaridade <= 3 THEN f.id END)
        AS obitos_baixa_escolaridade,
    ROUND(
        COUNT(CASE WHEN ram.id_assist_medica = 2 THEN 1 END) * 100.0 / COUNT(*),
        2
    ) AS pct_sem_assist_medica,
    ROUND(AVG(CASE
        WHEN f.data_nascimento IS NOT NULL
            THEN EXTRACT(YEAR FROM AGE(o.data_ocorrencia, f.data_nascimento))
    END), 1) AS idade_media_obito
FROM bdsm.municipio m
JOIN bdsm.falecido f ON m.codigo_do_municipio = f.municipio_residencia_id
JOIN bdsm.obito o ON f.obito_id = o.id
LEFT JOIN bdsm.escolaridade e ON f.escolaridade_nivel_id = e.id_escolaridade
LEFT JOIN
    bdsm.recebeu_assist_medica ram
    ON o.recebeu_assist_med_id = ram.id_assist_medica
GROUP BY m.nome, m.estado
HAVING COUNT(DISTINCT f.id) > 100
ORDER BY pct_sem_assist_medica DESC, obitos_baixa_escolaridade DESC
LIMIT 20;

-- 3. Óbitos relacionados a acidente de trabalho por ocupação
SELECT
    o.descricao_ocupacao,
    COUNT(*) AS total
FROM bdsm.falecido f
JOIN bdsm.ocupacao o ON f.ocupacao_habitual = o.id_ocupacao
JOIN bdsm.obito ob ON f.obito_id = ob.id
JOIN bdsm.atestado_de_obito a ON ob.atestado_de_obito_id = a.id_atestado_obito
JOIN bdsm.acidente_de_trabalho at ON a.acidente_de_trabalho_id = at.id_acidente
WHERE at.descricao_acidente = 'Sim'
GROUP BY o.descricao_ocupacao
ORDER BY total DESC
LIMIT 10;

-- 4. Mortalidade infantil por condições maternas
SELECT
    'Peso ao nascer' AS fator,
    CASE
        WHEN f.peso_ao_nascer < 1500 THEN 'Muito baixo peso (<1500g)'
        WHEN f.peso_ao_nascer < 2500 THEN 'Baixo peso (1500-2499g)'
        WHEN f.peso_ao_nascer < 4000 THEN 'Normal (2500-3999g)'
        ELSE 'Macrossomia (≥4000g)'
    END AS categoria,
    COUNT(*) AS total_obitos,
    ROUND(AVG(m.idade), 1) AS idade_media_mae
FROM bdsm.falecido f
JOIN bdsm.mae m ON f.mae_id = m.id_mae
JOIN bdsm.obito o ON f.obito_id = o.id
WHERE
    f.peso_ao_nascer IS NOT NULL
    AND EXTRACT(YEAR FROM AGE(o.data_ocorrencia, f.data_nascimento)) < 1
GROUP BY fator, categoria

UNION ALL

SELECT
    'Semanas de gestação' AS fator,
    CASE
        WHEN m.semanas_gestacao < 28 THEN 'Extremamente prematuro (<28 sem)'
        WHEN m.semanas_gestacao < 32 THEN 'Muito prematuro (28-31 sem)'
        WHEN m.semanas_gestacao < 37 THEN 'Prematuro (32-36 sem)'
        ELSE 'A termo (≥37 sem)'
    END AS categoria,
    COUNT(*) AS total_obitos,
    ROUND(AVG(m.idade), 1) AS idade_media_mae
FROM bdsm.falecido f
JOIN bdsm.mae m ON f.mae_id = m.id_mae
JOIN bdsm.obito o ON f.obito_id = o.id
WHERE
    m.semanas_gestacao IS NOT NULL
    AND EXTRACT(YEAR FROM AGE(o.data_ocorrencia, f.data_nascimento)) < 1
GROUP BY fator, categoria
ORDER BY fator, total_obitos DESC;


-- 5. Mortalidade por faixa etária e escolaridade
SELECT
    CASE
        WHEN idade < 20 THEN 'Menos de 20'
        WHEN idade BETWEEN 20 AND 39 THEN '20-39'
        WHEN idade BETWEEN 40 AND 59 THEN '40-59'
        WHEN idade BETWEEN 60 AND 79 THEN '60-79'
        ELSE '80+'
    END AS faixa_etaria,
    e.nivel_escolaridade,
    COUNT(*) AS total
FROM (
    SELECT
        f.*,
        EXTRACT(YEAR FROM AGE(o.data_ocorrencia, f.data_nascimento))::INT
            AS idade
    FROM bdsm.falecido f
    JOIN bdsm.obito o ON f.obito_id = o.id
    WHERE f.data_nascimento IS NOT NULL
) AS sub
JOIN bdsm.escolaridade e ON sub.escolaridade_nivel_id = e.id_escolaridade
GROUP BY faixa_etaria, e.nivel_escolaridade
ORDER BY faixa_etaria, e.nivel_escolaridade
LIMIT 10;
