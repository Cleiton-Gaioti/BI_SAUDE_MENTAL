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
	contador    int,
	origem      int,
	tipobito    int,
	dtobito     text,
	horaobito   text,
	"natural"   int,
	dtnasc      text,
	idade       text,
	sexo        int,
	racacor     int,
	estciv      int,
	esc         int,
	ocup        int,
	codmunres   int,
	codbaires   int,
	lococor     int,
	codestab    int,
	codmunocor  int,
	codbaiocor  int,
	idademae    int,
	escmae      int,
	ocupmae     int,
	qtdfilvivo  int,
	qtdfilmort  int,
	gravidez    int,
	gestacao    int,
	parto       int,
	obitoparto  int,
	peso        int,
	obitograv   int,
	obitopuerp  int,
	assistmed   int,
	exame       int,
	cirurgia    int,
	necropsia   int,
	linhaa      text,
	linhab      text,
	linhac      text,
	linhad      text,
	linhaii     text,
	causabas    text,
	dtatestado  text,
	circobito   int,
	acidtrab    int,
	fonte       int,
	tppos       text,
	dtinvestig  text,
	causabas_o  text,
	dtcadastro  text,
	atestante   int,
	fonteinv    int,
	dtrecebim   text,
	ufinform    int,
	cb_pre      text,
	morteparto  int,
	dtcadinf    int,
	tpobitocor  int,
	dtcadinv    text,
	comunsvoim  int,
	dtrecorig   text,
	dtrecoriga  text,
	causamat    text,
	esc2010     int,
	escmae2010  int,
	stdoepidem  int,
	stdonova    int,
	semagestac  int,
	tpmorteoco  int,
	difdata     text,
	dtconcaso   text,
	nudiasobin  text,
	seriescfal  int,
	seriescmae  int,
	codmuncart  int,
	codcart     int,
	numregcart  int,
	dtregcart   text,
	dtconinv    text,
	codmunnatu  int,
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
