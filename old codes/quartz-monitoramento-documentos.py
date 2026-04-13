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

import sys, io, re, argparse
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
# Tipos de documento e seus códigos na API do RAD
# O formato é IPE_{categoria}_{tipo}_{especie}
# Mapeamento obtido do <select id="cboCategorias"> do formulário RAD
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Tipos de documento — cada consulta ao RAD
#
# O formato do código é: IPE_{categoria}_{tipo}_{especie}
#   -1 = todos os tipos/espécies dentro daquela categoria
#
# Para "resultados", fazemos 2 consultas separadas (press-release em
# "Dados Econômico-Financeiros" + apresentações em "Comunicado ao Mercado")
# e juntamos num único PDF.
# ---------------------------------------------------------------------------
TIPOS_DOCUMENTO = {
    "fatos_relevantes": {
        "consultas": [
            {"codigo": "IPE_4_-1_-1", "descricao": "Fato Relevante"},
        ],
        "label": "Fatos Relevantes",
        "label_curto": "FR",
        "sempre_gerar": True,
    },
    "comunicados": {
        "consultas": [
            {"codigo": "IPE_21_-1_-1", "descricao": "Comunicado ao Mercado"},
        ],
        # Filtro: excluir apresentações a analistas (elas vão para o relatório de resultados)
        "excluir_tipos": ["Apresentações"],
        "label": "Comunicados ao Mercado",
        "label_curto": "CM",
        "sempre_gerar": True,
    },
    "resultados": {
        "consultas": [
            {"codigo": "IPE_6_-1_-1",  "descricao": "Dados Econômico-Financeiros (Press-Release)"},
            {"codigo": "IPE_21_-1_-1", "descricao": "Comunicado ao Mercado (Apresentações)"},
        ],
        # Filtro: manter apenas documentos cujo tipo/assunto contenha essas palavras
        "manter_tipos": ["Press", "press", "Release", "release", "Resultado",
                         "resultado", "Apresentaç", "apresentaç", "Earnings",
                         "earnings", "Guidance", "guidance", "Projeç", "projeç"],
        "label": "Release de Resultados e Apresentações",
        "label_curto": "RR",
        "sempre_gerar": False,
    },
}

# ---------------------------------------------------------------------------
# Portfólio QUARTZ FIA
# ---------------------------------------------------------------------------
PORTFOLIO = {
    "ASAI3":  {"nome": "Sendas Distribuidora",   "cvm": "25780", "cnpj": "03.560.312/0001-24"},
    "AXIA6":  {"nome": "Eletrobras",              "cvm": "02437", "cnpj": "00.001.180/0001-26"},
    "CEAB3":  {"nome": "C&A Modas",               "cvm": "24600", "cnpj": "45.242.914/0001-05"},
    "CSAN3":  {"nome": "Cosan",                   "cvm": "21652", "cnpj": "50.746.577/0001-15"},
    "EQTL3":  {"nome": "Equatorial",              "cvm": "21580", "cnpj": "03.220.438/0001-73"},
    "GGPS3":  {"nome": "GPS Participações",       "cvm": "25712", "cnpj": "09.229.201/0001-30"},
    "HAPV3":  {"nome": "Hapvida",                 "cvm": "24217", "cnpj": "63.554.067/0001-98"},
    "INTB3":  {"nome": "Intelbras",               "cvm": "25607", "cnpj": "82.901.000/0001-27"},
    "ITUB4":  {"nome": "Itaú Unibanco",           "cvm": "19348", "cnpj": "60.872.504/0001-23"},
    "MULT3":  {"nome": "Multiplan",               "cvm": "21300", "cnpj": "07.816.890/0001-53"},
    "ORVR3":  {"nome": "Orizon",                  "cvm": "25429", "cnpj": "11.421.994/0001-36"},
    "RADL3":  {"nome": "Raia Drogasil",           "cvm": "17973", "cnpj": "61.585.865/0001-51"},
    "RENT3":  {"nome": "Localiza",                "cvm": "17078", "cnpj": "16.670.085/0001-55"},
    "SMFT3":  {"nome": "Smartfit",                "cvm": "25550", "cnpj": "31.613.412/0001-72"},
}

_COD_CVM_TICKER = {v["cvm"]: k for k, v in PORTFOLIO.items()}

# ---------------------------------------------------------------------------
# 1. Consultar RAD/CVM via API REST
# ---------------------------------------------------------------------------
def _consulta_unica(data_alvo: date, codigo_categoria: str) -> list[dict]:
    """Faz uma consulta ao RAD e retorna lista de fatos (sem classificar portfolio/outros)."""
    dt_str = data_alvo.strftime("%d/%m/%Y")
    payload = (
        f"{{ dataDe: '{dt_str}', dataAte: '{dt_str}', empresa: '', "
        f"setorAtividade: '-1', categoriaEmissor: '-1', situacaoEmissor: '-1', "
        f"tipoParticipante: '1', dataReferencia: '', categoria: '{codigo_categoria}', "
        f"periodo: '2', horaIni: '', horaFim: '', palavraChave: '', "
        f"ultimaDtRef: 'false', tipoEmpresa: '0', token: '', versaoCaptcha: '' }}"
    )
    # tipoParticipante: '1' = Companhia Aberta (exclui BDRs, incentivadas, dispensadas etc.)

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    resp = session.post(
        RAD_URL + "/ListarDocumentos",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=30,
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

        cod_raw = cols[0].strip().split("-")[0]
        cod_norm = cod_raw.zfill(5)

        dt_match = re.search(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2})", cols[6])
        dt_entrega = dt_match.group(1) if dt_match else ""

        dl_match = re.search(r"OpenDownloadDocumentos\('(\d+)','(\d+)','(\d+)'", cols[10] if len(cols) > 10 else "")
        seq       = dl_match.group(1) if dl_match else ""
        versao    = dl_match.group(2) if dl_match else ""
        protocolo = dl_match.group(3) if dl_match else ""

        link = (
            f"https://www.rad.cvm.gov.br/ENET/frmDownloadDocumento.aspx?"
            f"Tela=ext&numSequencia={seq}&numVersao={versao}"
            f"&numProtocolo={protocolo}&descTipo=IPE&CodigoInstituicao=1"
        ) if seq else ""

        # cols[2] = Categoria, cols[3] = Tipo, cols[4] = Espécie
        tipo_doc = cols[3].strip() if len(cols) > 3 else ""
        especie  = cols[4].strip() if len(cols) > 4 else ""

        fatos.append({
            "cod_cvm":      cod_norm,
            "nome":         cols[1].strip(),
            "categoria":    cols[2].strip(),
            "tipo_doc":     tipo_doc,
            "especie":      especie,
            "data_ref":     re.sub(r"<[^>]+>", "", cols[5]).strip(),
            "data_entrega": dt_entrega,
            "assunto":      cols[11].strip() if len(cols) > 11 else "",
            "link":         link,
        })

    return fatos


def _texto_buscavel(fato: dict) -> str:
    """Concatena campos relevantes para filtro por palavra-chave."""
    return " ".join([
        fato.get("categoria", ""),
        fato.get("tipo_doc", ""),
        fato.get("especie", ""),
        fato.get("assunto", ""),
    ])


def consultar_tipo(data_alvo: date, tipo_config: dict) -> tuple[list[dict], list[dict]]:
    """
    Executa todas as consultas definidas em tipo_config["consultas"],
    aplica filtros (manter_tipos / excluir_tipos) e separa portfolio/outros.
    Retorna (fatos_portfolio, fatos_outros).
    """
    todos = []
    protocolos_vistos = set()

    for consulta in tipo_config["consultas"]:
        print(f"    Consultando: {consulta['descricao']}...")
        try:
            resultados = _consulta_unica(data_alvo, consulta["codigo"])
            # Desduplicar (quando 2 consultas retornam o mesmo doc)
            for f in resultados:
                chave = f"{f['cod_cvm']}_{f['data_entrega']}_{f['link']}"
                if chave not in protocolos_vistos:
                    protocolos_vistos.add(chave)
                    todos.append(f)
        except Exception as e:
            print(f"    ERRO: {e}")

    # Filtro: manter apenas documentos que contenham certas palavras
    manter = tipo_config.get("manter_tipos")
    if manter:
        todos = [f for f in todos if any(p in _texto_buscavel(f) for p in manter)]

    # Filtro: excluir documentos que contenham certas palavras
    excluir = tipo_config.get("excluir_tipos")
    if excluir:
        todos = [f for f in todos if not any(p in _texto_buscavel(f) for p in excluir)]

    # Separar portfolio vs outros
    portfolio_fatos, outros_fatos = [], []
    for fato in todos:
        ticker = _COD_CVM_TICKER.get(fato["cod_cvm"])
        if ticker:
            fato["ticker"] = ticker
            portfolio_fatos.append(fato)
        else:
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
    if pdf.get_y() > 230:
        pdf.add_page()

    pdf.set_font("Arial", "B", 11)
    pdf.set_text_color(26, 54, 93)
    pdf.cell(0, 7, f"{fato.get('ticker', '?')} — {fato['nome']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_draw_color(49, 130, 206)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(2)

    pdf.set_font("Arial", "", 7)
    pdf.set_text_color(130, 130, 130)
    meta_line = f"Assunto: {fato.get('assunto', 'N/D')}  |  Entrega: {fato.get('data_entrega', '')}"
    tipo_doc = fato.get("tipo_doc", "")
    if tipo_doc:
        meta_line += f"  |  Tipo: {tipo_doc}"
    pdf.cell(0, 4, meta_line,
             new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    if fato.get("link"):
        pdf.set_text_color(0, 0, 200)
        pdf.multi_cell(0, 4, fato["link"], link=fato["link"])
        pdf.set_text_color(130, 130, 130)
    pdf.ln(2)

    pdf.set_font("Arial", "", 8)
    pdf.set_text_color(45, 55, 72)
    pdf.multi_cell(0, 4, fato.get("texto", "Texto não extraído."))
    pdf.ln(5)


def _bloco_outros(pdf, fato):
    if pdf.get_y() > 260:
        pdf.add_page()

    pdf.set_font("Arial", "B", 9)
    pdf.set_text_color(26, 54, 93)
    pdf.cell(0, 5, fato["nome"], new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Arial", "", 7)
    pdf.set_text_color(100, 100, 100)
    meta_line = f"Assunto: {fato.get('assunto', 'N/D')}  |  Entrega: {fato.get('data_entrega', '')}  |  CVM: {fato.get('cod_cvm', '')}"
    tipo_doc = fato.get("tipo_doc", "")
    if tipo_doc:
        meta_line += f"  |  Tipo: {tipo_doc}"
    pdf.cell(0, 4, meta_line,
             new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    if fato.get("link"):
        pdf.set_text_color(0, 0, 200)
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
        f"Seção 1 — Portfólio Quartz FIA",
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
        f"Seção 2 — Demais Empresas da B3",
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
    args = parser.parse_args()

    if args.hoje:
        data_alvo = date.today()
    elif args.data:
        data_alvo = date.fromisoformat(args.data)
    else:
        data_alvo = date.today() - timedelta(days=1)

    print(f"Data alvo: {data_alvo.strftime('%d/%m/%Y')}")
    print("=" * 60)

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

        # Se não deve gerar quando vazio e está vazio, pula
        if not tipo["sempre_gerar"] and total == 0:
            print(f"  Nenhum registro — PDF não gerado.")
            continue

        # Extrair texto dos PDFs do portfólio
        for fato in raw_portfolio:
            ticker = fato.get("ticker", "?")
            print(f"  Extraindo texto: {ticker} — {fato['nome']}")
            try:
                fato["texto"] = extrair_texto(fato.get("link", ""))
            except Exception as e:
                print(f"    Erro: {e}")
                fato["texto"] = "Não foi possível extrair o conteúdo deste documento."

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
