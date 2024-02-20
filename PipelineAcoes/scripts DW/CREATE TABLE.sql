CREATE TABLE acoes (
    id SERIAL PRIMARY KEY,
    companyid INT NOT NULL,
    companyname VARCHAR(100) NOT NULL,
    ticker VARCHAR(10),
    price DECIMAL(7,2),
    p_l DECIMAL(10,2),
    p_vp DECIMAL(10,2),
    p_ebit DECIMAL(7,2),
    p_ativo DECIMAL(7,2),
    ev_ebit DECIMAL(7,2),
    margembruta DECIMAL(7,2),
    margemebit DECIMAL(7,2),
    margemliquida DECIMAL(10,2),
    p_sr DECIMAL(7,2),
    p_capitalgiro DECIMAL(10,2),
    p_ativocirculante DECIMAL(7,2),
    giroativos DECIMAL(7,2),
    roe DECIMAL(8,2),
    roa DECIMAL(8,2),
    roic DECIMAL(8,2),
    dividaliquidapatrimonioliquido DECIMAL(8,2),
    dividaliquidaebit DECIMAL(8,2),
    pl_ativo DECIMAL(7,2),
    passivo_ativo DECIMAL(7,2),
    liquidezcorrente DECIMAL(7,2),
    peg_ratio DECIMAL(7,2),
    receitas_cagr5 DECIMAL(7,2),
    liquidezmediadiaria DECIMAL(14,2),
    vpa DECIMAL(10,2),
    lpa DECIMAL(10,2),
    valormercado DECIMAL(15,2),
    segmentid INT,
    sectorid INT,
    subsectorid INT,
    subsectorname VARCHAR(100),
    segmentname VARCHAR(100),
    sectorname VARCHAR(100),
    dy DECIMAL(6,2),
    lucros_cagr5 DECIMAL(7,2)
);