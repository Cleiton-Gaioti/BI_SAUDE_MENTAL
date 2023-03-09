CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.stg_cbo (
    cod     INT,
    nome    TEXT
);

CREATE TABLE IF NOT EXISTS stg.stg_cid10_capitulos (
	codigo          text,
	descricao       text,
	descricao_breve text
);

CREATE TABLE IF NOT EXISTS stg.stg_cid10_categorias (
	cat           text,
	classif       text,
	descricao     text,
	descrabrev    text,
	refer         text,
	excluidos     float,
	"unnamed: 6"    float
);

CREATE TABLE IF NOT EXISTS stg.stg_cid10_grupos (
	codigo          text,
	descricao       text,
	descricao_breve text
);

CREATE TABLE IF NOT EXISTS stg.stg_cid10_subcategorias (
	subcat        text,
	classif       text,
	restrsexo     text,
	causaobito    text,
	descricao     text,
	descrabrev    text,
	refer         text,
	excluidos     text,
	"unnamed: 8"    float
);

CREATE TABLE IF NOT EXISTS stg.stg_municipios (
	muncod      int,
	muncoddv    int,
	situacao    text,
	munsinp     int,
	munsiafi    int,
	munnome     text,
	munnomex    text,
	observ      text,
	munsinon    text,
	munsinondv  text,
	amazonia    text,
	fronteira   text,
	capital     text,
	ufcod       int,
	mesocod     int,
	microcod    int,
	msaudcod    int,
	rsaudcod    int,
	csaudcod    int,
	rmetrcod    int,
	aglcod      int,
	anoinst     int,
	anoext      int,
	sucessor    int,
	latitude    float,
	longitude   float,
	altitude    float,
	area        float
);

CREATE TABLE IF NOT EXISTS stg.stg_naturalidade (
	cod     int,
	nome    text
);

CREATE TABLE IF NOT EXISTS stg.stg_ocupacao (
	cod     int,
	nome    text
);

CREATE TABLE IF NOT EXISTS stg.stg_sim (
	contador    text,
	origem      text,
	tipobito    text,
	dtobito     text,
	horaobito   text,
	"natural"   text,
	dtnasc      text,
	idade       text,
	sexo        text,
	racacor     text,
	estciv      text,
	esc         text,
	ocup        text,
	codmunres   text,
	codbaires   text,
	lococor     text,
	codestab    text,
	codmunocor  text,
	codbaiocor  text,
	idademae    text,
	escmae      text,
	ocupmae     text,
	qtdfilvivo  text,
	qtdfilmort  text,
	gravidez    text,
	gestacao    text,
	parto       text,
	obitoparto  text,
	peso        text,
	obitograv   text,
	obitopuerp  text,
	assistmed   text,
	exame       text,
	cirurgia    text,
	necropsia   text,
	linhaa      text,
	linhab      text,
	linhac      text,
	linhad      text,
	linhaii     text,
	causabas    text,
	dtatestado  text,
	circobito   text,
	acidtrab    text,
	fonte       text,
	tppos       text,
	dtinvestig  text,
	causabas_o  text,
	dtcadastro  text,
	atestante   text,
	fonteinv    text,
	dtrecebim   text,
	ufinform    text,
	cb_pre      text,
	morteparto  text,
	dtcadinf    text,
	tpobitocor  text,
	dtcadinv    text,
	comunsvoim  text,
	dtrecorig   text,
	dtrecoriga  text,
	causamat    text,
	esc2010     text,
	escmae2010  text,
	stdoepidem  text,
	stdonova    text,
	semagestac  text,
	tpmorteoco  text,
	difdata     text,
	dtconcaso   text,
	nudiasobin  text,
	seriescfal  text,
	seriescmae  text,
	codmuncart  text,
	codcart     text,
	numregcart  text,
	dtregcart   text,
	dtconinv    text,
	codmunnatu  text,
	estabdescr  text,
	crm         text,
	numerolote  text,
	stcodifica  text,
	codificado  text,
	versaosist  text,
	versaoscb   text,
	atestado    text,
	escmaeagr1  text,
	escfalagr1  text,
	nudiasobco  text,
	fontes      text,
	tpresginfo  text,
	tpnivelinv  text,
	nudiasinf   text,
	fontesinf   text,
	altcausa    text
);

CREATE TABLE IF NOT EXISTS stg.stg_uf (
	sigla_uf    text,
	codigo      int,
	descricao   text
);
