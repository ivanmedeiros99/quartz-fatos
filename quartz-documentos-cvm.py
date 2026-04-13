#!/usr/bin/env python3
"""
QUARTZ FIA — Fatos Relevantes, Comunicados e Resultados (RAD/CVM)

Consulta o sistema RAD da CVM em tempo real e gera até 3 PDFs:
  1. Fatos Relevantes         (sempre gerado)
  2. Comunicados ao Mercado   (sempre gerado)
  3. Release de Resultados    (gerado somente se houver registros)

Uso:
  python quartz_fatos_relevantes.py                    # ontem
  python quartz_fatos_relevantes.py --data 2026-04-09  # data específica
  python quartz_fatos_relevantes.py --hoje              # hoje

Dependências:
  pip install requests pdfplumber fpdf2
"""

import sys, io, re, copy, argparse
from pathlib import Path
from datetime import date, datetime, timedelta

try:
    import requests, pdfplumber
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos
except ImportError as e:
    print(f"Dependência faltando: {e}")
    print("Instale com: pip install requests pdfplumber fpdf2")
    sys.exit(1)

PASTA_SAIDA = Path(__file__).parent / "relatorios"
PASTA_SAIDA.mkdir(exist_ok=True)

RAD_URL = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"

# ---------------------------------------------------------------------------
# Tipos de documento
# Uma única consulta ampla (IPE_-1_-1_-1) é feita ao RAD, e os resultados
# são filtrados no código pelo texto da coluna "Categoria".
# ---------------------------------------------------------------------------
TIPOS_DOCUMENTO = {
    "fatos_relevantes": {
        "filtro_categoria": ["Fato Relevante"],
        "label": "Fatos Relevantes",
        "label_curto": "FR",
        "sempre_gerar": True,
    },
    "comunicados": {
        "filtro_categoria": ["Comunicado ao Mercado"],
        "excluir_palavras": ["Apresentações a analistas", "Apresentacoes a analistas"],
        "label": "Comunicados ao Mercado",
        "label_curto": "CM",
        "sempre_gerar": True,
    },
    "resultados": {
        "filtro_categoria": ["Dados Econômico-Financeiros", "Dados Economico-Financeiros",
                             "Comunicado ao Mercado"],
        "manter_palavras": ["Press", "press", "Release", "release", "Resultado",
                            "resultado", "Apresentaç", "apresentaç", "Apresentac",
                            "apresentac", "Earnings", "earnings", "Guidance",
                            "guidance", "Projeç", "projeç", "Projecoes", "projecoes"],
        "label": "Release de Resultados e Apresentações",
        "label_curto": "RR",
        "sempre_gerar": False,
    },
}

# ---------------------------------------------------------------------------
# Portfólio QUARTZ FIA
# ---------------------------------------------------------------------------
PORTFOLIO = {
    "ASAI3":  {"nome": "Sendas Distribuidora",   "cvm": "2537",  "cnpj": "03.560.312/0001-24"},
    "AXIA6":  {"nome": "Eletrobras",              "cvm": "243",   "cnpj": "00.001.180/0001-26"},
    "CEAB3":  {"nome": "C&A Modas",               "cvm": "2484",  "cnpj": "45.242.914/0001-05"},
    "CSAN3":  {"nome": "Cosan",                   "cvm": "1983",  "cnpj": "50.746.577/0001-15"},
    "EQTL3":  {"nome": "Equatorial",              "cvm": "2001",  "cnpj": "03.220.438/0001-73"},
    "GGPS3":  {"nome": "GPS Participações",       "cvm": "2571",  "cnpj": "09.229.201/0001-30"},
    "HAPV3":  {"nome": "Hapvida",                 "cvm": "2439",  "cnpj": "63.554.067/0001-98"},
    "INTB3":  {"nome": "Intelbras",               "cvm": "2545",  "cnpj": "82.901.000/0001-27"},
    "ITUB4":  {"nome": "Itaú Unibanco",           "cvm": "1934",  "cnpj": "60.872.504/0001-23"},
    "MULT3":  {"nome": "Multiplan",               "cvm": "2098",  "cnpj": "07.816.890/0001-53"},
    "ORVR3":  {"nome": "Orizon",                  "cvm": "2555",  "cnpj": "11.421.994/0001-36"},
    "RADL3":  {"nome": "Raia Drogasil",           "cvm": "525",   "cnpj": "61.585.865/0001-51"},
    "RENT3":  {"nome": "Localiza",                "cvm": "1973",  "cnpj": "16.670.085/0001-55"},
    "SMFT3":  {"nome": "Smartfit",                "cvm": "2426",  "cnpj": "31.613.412/0001-72"},
}

# Mapa de código CVM → ticker, normalizado (sem zeros à esquerda)
# A API retorna "02437-7" ou "25780-2", extraímos o número antes do "-"
# e convertemos para int para comparar sem problemas de zero-padding.
_COD_CVM_INT_TICKER = {int(v["cvm"]): k for k, v in PORTFOLIO.items()}


def _normalizar_cod_cvm(cod_raw: str) -> int:
    """Extrai o código CVM numérico de strings como '02437-7' ou '25780'."""
    parte = cod_raw.strip().split("-")[0].strip()
    try:
        return int(parte)
    except ValueError:
        return -1


# ---------------------------------------------------------------------------
# Mapa B3: codeCVM (str, sem zeros à esquerda, sem hífen) → ticker (ex: "HAPV3")
# ---------------------------------------------------------------------------
_TICKERS_B3: dict[str, str] = {}


def _carregar_tickers_b3() -> None:
    global _TICKERS_B3
    if _TICKERS_B3:
        return

    try:
        url = (
            "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/"
            "CompanyCall/GetInitialCompanies/"
            "eyJsYW5ndWFnZSI6InB0LWJyIn0="
        )
        resp = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()

        data = resp.json()
        empresas = data if isinstance(data, list) else data.get("results", [])

        _TICKERS_B3 = {}
        for e in empresas:
            code_cvm = str(e.get("codeCVM", "")).strip()
            ticker = (
                e.get("issuingCompany")
                or e.get("companyName")
                or e.get("tradingName")
                or ""
            )
            ticker = str(ticker).strip()

            if code_cvm and ticker:
                _TICKERS_B3[code_cvm] = ticker

        print(f"  B3: {len(_TICKERS_B3)} empresas carregadas para lookup de tickers")

    except Exception as exc:
        print(f"  Aviso: não foi possível carregar tickers B3 ({exc}). Usando código CVM como fallback.")

        

def _ticker_b3(cod_raw: str) -> str:
    """Converte '02439-1' → 'HAPV3' corretamente."""
    cod_limpo = cod_raw.split("-")[0].lstrip("0") or "0"
    return _TICKERS_B3.get(cod_limpo, cod_raw.strip())


# ---------------------------------------------------------------------------
# 1. Consultar RAD/CVM via API REST
# ---------------------------------------------------------------------------
_CACHE_TODOS: dict[str, list[dict]] = {}


def _buscar_todos_ipe(data_alvo: date) -> list[dict]:
    """Busca TODOS os documentos eventuais (IPE) do dia na CVM."""
    dt_str = data_alvo.strftime("%d/%m/%Y")
    payload = (
        f"{{ dataDe: '{dt_str}', dataAte: '{dt_str}', empresa: '', "
        f"setorAtividade: '-1', categoriaEmissor: '-1', situacaoEmissor: '-1', "
        f"tipoParticipante: '1', dataReferencia: '', categoria: 'IPE_-1_-1_-1', "
        f"periodo: '2', horaIni: '', horaFim: '', palavraChave: '', "
        f"ultimaDtRef: 'false', tipoEmpresa: '0', token: '', versaoCaptcha: '' }}"
    )

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    resp = session.post(
        RAD_URL + "/ListarDocumentos",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=60,
    )
    resp.raise_for_status()
    inner = resp.json()["d"]

    if inner.get("SolicitarCaptcha") == "S":
        raise RuntimeError("CVM solicitou CAPTCHA — tente novamente em instantes.")
    if inner.get("temErro"):
        raise RuntimeError(inner.get("msgErro", "Erro desconhecido na API da CVM."))

    dados = inner.get("dados", "")
    if not dados:
        return []

    linhas = [l for l in dados.split("&*") if l]
    fatos = []

    for linha in linhas:
        cols = linha.split("$&")
        if len(cols) < 7:
            continue

        cod_cvm_int = _normalizar_cod_cvm(cols[0])

        dt_match = re.search(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2})", cols[6])
        dt_entrega = dt_match.group(1) if dt_match else ""

        dl_match = re.search(
            r"OpenDownloadDocumentos\('(\d+)','(\d+)','(\d+)'",
            cols[10] if len(cols) > 10 else ""
        )
        seq       = dl_match.group(1) if dl_match else ""
        versao    = dl_match.group(2) if dl_match else ""
        protocolo = dl_match.group(3) if dl_match else ""

        link = (
            f"https://www.rad.cvm.gov.br/ENET/frmDownloadDocumento.aspx?"
            f"Tela=ext&numSequencia={seq}&numVersao={versao}"
            f"&numProtocolo={protocolo}&descTipo=IPE&CodigoInstituicao=1"
        ) if seq else ""

        fatos.append({
            "cod_cvm_int":  cod_cvm_int,
            "cod_cvm_raw":  cols[0].strip(),
            "nome":         cols[1].strip(),
            "categoria":    cols[2].strip() if len(cols) > 2 else "",
            "tipo_doc":     cols[3].strip() if len(cols) > 3 else "",
            "especie":      cols[4].strip() if len(cols) > 4 else "",
            "data_ref":     re.sub(r"<[^>]+>", "", cols[5]).strip(),
            "data_entrega": dt_entrega,
            "assunto":      re.sub(r"<[^>]+>", "", cols[4]).strip() if len(cols) > 4 else "",
            "link":         link,
        })

    return fatos


def _texto_buscavel(fato: dict) -> str:
    return " ".join([
        fato.get("categoria", ""),
        fato.get("tipo_doc", ""),
        fato.get("especie", ""),
        fato.get("assunto", ""),
    ])


def consultar_tipo(data_alvo: date, tipo_config: dict) -> tuple[list[dict], list[dict]]:
    """Busca todos os IPE do dia (com cache) e filtra conforme tipo_config."""
    chave_cache = data_alvo.isoformat()
    if chave_cache not in _CACHE_TODOS:
        print("  Buscando TODOS os documentos IPE do dia na CVM...")
        _CACHE_TODOS[chave_cache] = _buscar_todos_ipe(data_alvo)
        total = len(_CACHE_TODOS[chave_cache])
        print(f"  {total} documento(s) encontrado(s) no total")

        # Debug: mostrar categorias únicas encontradas
        cats = sorted(set(f["categoria"] for f in _CACHE_TODOS[chave_cache] if f["categoria"]))
        if cats:
            print(f"  Categorias encontradas: {', '.join(cats)}")

    todos = copy.deepcopy(_CACHE_TODOS[chave_cache])  # cópia profunda para não contaminar o cache

    # Filtro 1: categoria
    filtro_cat = tipo_config.get("filtro_categoria", [])
    if filtro_cat:
        todos = [f for f in todos if any(c in f.get("categoria", "") for c in filtro_cat)]

    # Filtro 2: manter apenas docs com certas palavras
    manter = tipo_config.get("manter_palavras")
    if manter:
        todos = [f for f in todos if any(p in _texto_buscavel(f) for p in manter)]

    # Filtro 3: excluir docs com certas palavras
    excluir = tipo_config.get("excluir_palavras")
    if excluir:
        todos = [f for f in todos if not any(p in _texto_buscavel(f) for p in excluir)]

    # Separar portfolio vs outros — usando comparação numérica (int)
    portfolio_fatos, outros_fatos = [], []
    for fato in todos:
        ticker = _COD_CVM_INT_TICKER.get(fato["cod_cvm_int"])
        if ticker:
            fato["ticker"] = ticker
            portfolio_fatos.append(fato)
        else:
            fato["ticker"] = _ticker_b3(fato["cod_cvm_raw"])
            outros_fatos.append(fato)

    return portfolio_fatos, outros_fatos

# ---------------------------------------------------------------------------
# 2. Extrair texto de um PDF
# ---------------------------------------------------------------------------
def extrair_texto(url: str) -> str:
    if not url:
        return "Link do documento não disponível."
    resp = requests.get(url, timeout=90)
    resp.raise_for_status()
    paginas = []
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for p in pdf.pages:
            t = p.extract_text()
            if t:
                paginas.append(t)
    return "\n\n".join(paginas) if paginas else "Não foi possível extrair texto do PDF."

# ---------------------------------------------------------------------------
# 3. Gerar PDF
# ---------------------------------------------------------------------------
class Relatorio(FPDF):
    def __init__(self, data_ref: str, tipo_label: str):
        super().__init__()
        self._data = data_ref
        self._tipo = tipo_label
        self.set_auto_page_break(auto=True, margin=25)
        self.add_font("Arial", "",  r"C:\Windows\Fonts\arial.ttf")
        self.add_font("Arial", "B", r"C:\Windows\Fonts\arialbd.ttf")

    def header(self):
        self.set_fill_color(26, 54, 93)
        self.rect(0, 0, 210, 16, "F")
        self.set_font("Arial", "B", 9)
        self.set_text_color(255, 255, 255)
        self.set_xy(10, 3)
        self.cell(120, 10, f"QUARTZ FIA — {self._tipo}")
        self.cell(70, 10, self._data, align="R")
        self.ln(16)

    def footer(self):
        self.set_y(-12)
        self.set_font("Arial", "", 7)
        self.set_text_color(150, 150, 150)
        self.cell(0, 8, f"Fonte: RAD/CVM (tempo real)  |  Pág. {self.page_no()}/{{nb}}", align="C")

    def secao_header(self, titulo: str, subtitulo: str = ""):
        if self.get_y() > 240:
            self.add_page()
        self.ln(4)
        self.set_fill_color(26, 54, 93)
        self.set_text_color(255, 255, 255)
        self.set_font("Arial", "B", 10)
        self.cell(0, 8, f"  {titulo}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        if subtitulo:
            self.set_fill_color(49, 80, 120)
            self.set_font("Arial", "", 8)
            self.cell(0, 6, f"  {subtitulo}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(4)
        self.set_text_color(45, 55, 72)


def _banner(pdf, n, texto):
    pdf.set_fill_color(235, 248, 255)
    pdf.set_draw_color(49, 130, 206)
    pdf.set_font("Arial", "B", 9)
    pdf.set_text_color(26, 54, 93)
    y = pdf.get_y()
    pdf.rect(10, y, 190, 10, "FD")
    pdf.set_xy(14, y + 1)
    pdf.cell(0, 8, f"{n} registro(s)  |  {texto}")
    pdf.ln(14)


def _bloco_portfolio(pdf, fato):
    if pdf.get_y() > 260:
        pdf.add_page()

    pdf.set_font("Arial", "B", 9)
    pdf.set_text_color(26, 54, 93)
    pdf.cell(0, 5, f"[{fato.get('ticker', '?')}]  {fato['nome']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Arial", "", 7)
    pdf.set_text_color(100, 100, 100)
    meta = f"Assunto: {fato.get('assunto', 'N/D')}  |  Entrega: {fato.get('data_entrega', '')}"
   
    pdf.cell(0, 4, meta, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    if fato.get("link"):
        pdf.set_text_color(0, 0, 100)
        pdf.multi_cell(0, 4, fato["link"], link=fato["link"])
        pdf.set_text_color(100, 100, 100)

    pdf.set_draw_color(200, 200, 200)
    pdf.line(10, pdf.get_y() + 1, 200, pdf.get_y() + 1)
    pdf.ln(5)


def _bloco_outros(pdf, fato):
    if pdf.get_y() > 260:
        pdf.add_page()

    pdf.set_font("Arial", "B", 9)
    pdf.set_text_color(26, 54, 93)
    pdf.cell(0, 5, f"{fato['nome']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Arial", "", 7)
    pdf.set_text_color(100, 100, 100)
    meta = f"Assunto: {fato.get('assunto', 'N/D')}  |  Entrega: {fato.get('data_entrega', '')}"
   
    pdf.cell(0, 4, meta, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    if fato.get("link"):
        pdf.set_text_color(0, 0, 100)
        pdf.multi_cell(0, 4, fato["link"], link=fato["link"])
        pdf.set_text_color(100, 100, 100)

    pdf.set_draw_color(200, 200, 200)
    pdf.line(10, pdf.get_y() + 1, 200, pdf.get_y() + 1)
    pdf.ln(5)



def gerar_pdf(fatos_portfolio, fatos_outros, data_ref, tipo_label, sufixo) -> Path:
    pdf = Relatorio(data_ref.strftime("%d/%m/%Y"), tipo_label)
    pdf.alias_nb_pages()
    pdf.add_page()

    # Seção 1 — Portfólio
    pdf.secao_header(
        "Seção 1 — Portfólio Quartz FIA",
        f"{len(fatos_portfolio)} registro(s) das empresas em carteira"
    )

    if not fatos_portfolio:
        pdf.set_font("Arial", "", 9)
        pdf.set_text_color(160, 160, 160)
        pdf.cell(0, 8, "Nenhum registro publicado nesta data pelas empresas do portfólio.", align="C")
        pdf.ln(6)
    else:
        tickers = ", ".join(sorted(set(f.get("ticker", "?") for f in fatos_portfolio)))
        _banner(pdf, len(fatos_portfolio), tickers)
        for fato in fatos_portfolio:
            _bloco_portfolio(pdf, fato)

    # Seção 2 — Outros
    pdf.add_page()
    pdf.secao_header(
        "Seção 2 — Demais Empresas da B3",
        f"{len(fatos_outros)} registro(s) de outras companhias"
    )

    if not fatos_outros:
        pdf.set_font("Arial", "", 9)
        pdf.set_text_color(160, 160, 160)
        pdf.cell(0, 8, "Nenhum registro de outras empresas nesta data.", align="C")
    else:
        _banner(pdf, len(fatos_outros), "ordenado por nome")
        for fato in sorted(fatos_outros, key=lambda x: x["nome"]):
            _bloco_outros(pdf, fato)

    nome = f"quartz_{sufixo}_{data_ref.strftime('%Y%m%d')}.pdf"
    arquivo = PASTA_SAIDA / nome
    try:
        pdf.output(str(arquivo))
    except PermissionError:
        arquivo = PASTA_SAIDA / f"quartz_{sufixo}_{data_ref.strftime('%Y%m%d')}_{datetime.now().strftime('%H%M')}.pdf"
        pdf.output(str(arquivo))
    return arquivo

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="QUARTZ FIA — Fatos Relevantes, Comunicados e Resultados")
    parser.add_argument("--data", type=str, help="Data AAAA-MM-DD (padrão: ontem)")
    parser.add_argument("--hoje", action="store_true", help="Buscar registros de hoje")
    parser.add_argument("--debug", action="store_true", help="Mostrar dados brutos para diagnóstico")
    args = parser.parse_args()

    if args.hoje:
        data_alvo = date.today()
    elif args.data:
        data_alvo = date.fromisoformat(args.data)
    else:
        data_alvo = date.today() - timedelta(days=1)

    print(f"Data alvo: {data_alvo.strftime('%d/%m/%Y')}")
    print("=" * 60)

    _carregar_tickers_b3()

    arquivos_gerados = []

    for chave, tipo in TIPOS_DOCUMENTO.items():
        print(f"\n[{tipo['label_curto']}] {tipo['label']}")

        try:
            raw_portfolio, raw_outros = consultar_tipo(data_alvo, tipo)
        except Exception as e:
            print(f"  ERRO na consulta: {e}")
            continue

        total = len(raw_portfolio) + len(raw_outros)
        print(f"  Portfólio: {len(raw_portfolio)}  |  Outras: {len(raw_outros)}")

        # Debug: mostrar quais empresas foram identificadas no portfólio
        if args.debug and raw_portfolio:
            for f in raw_portfolio:
                print(f"    [PORTFOLIO] {f['ticker']} | CVM={f['cod_cvm_raw']} | {f['nome']} | {f['categoria']}")
        if args.debug and raw_outros[:5]:
            for f in raw_outros[:5]:
                print(f"    [OUTROS]    CVM={f['cod_cvm_raw']} | {f['nome']} | {f['categoria']}")
            if len(raw_outros) > 5:
                print(f"    ... e mais {len(raw_outros)-5}")

        if not tipo["sempre_gerar"] and total == 0:
            print(f"  Nenhum registro — PDF não gerado.")
            continue

        # Gerar PDF
        arquivo = gerar_pdf(
            raw_portfolio, raw_outros, data_alvo,
            tipo["label"], chave
        )
        arquivos_gerados.append(arquivo)
        print(f"  PDF: {arquivo.name}")

    print("\n" + "=" * 60)
    print(f"  {len(arquivos_gerados)} relatório(s) gerado(s) em: {PASTA_SAIDA.resolve()}")
    for a in arquivos_gerados:
        print(f"    - {a.name}")
    print("=" * 60)


if __name__ == "__main__":
    main()
