#!/usr/bin/env python3
"""
QUARTZ FIA — Fatos Relevantes em Tempo Real (RAD/CVM)

Consulta o sistema RAD da CVM via Selenium, busca fatos relevantes
publicados na data escolhida e gera um PDF com o resultado.

Uso:
  python quartz_fatos_relevantes.py                    # fatos de ontem
  python quartz_fatos_relevantes.py --data 2026-04-09  # data específica
  python quartz_fatos_relevantes.py --hoje              # fatos de hoje

Dependências:
  pip install selenium pdfplumber fpdf2 webdriver-manager
  (+ Google Chrome instalado no computador)
"""

import sys, io, re, argparse
from pathlib import Path
from datetime import date, timedelta

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
# Portfólio QUARTZ FIA — ticker: código CVM
# O código CVM é o identificador usado no sistema RAD
# ---------------------------------------------------------------------------
PORTFOLIO = {
    "ASAI3":  {"nome": "Sendas Distribuidora",          "cvm": "25780", "cnpj": "03.560.312/0001-24"},
    "AXIA6":  {"nome": "Eletrobras",                     "cvm": "2437",  "cnpj": "00.001.180/0001-26"},
    "CEAB3":  {"nome": "C&A Modas",                      "cvm": "24600", "cnpj": "45.242.914/0001-05"},
    "CSAN3":  {"nome": "Cosan",                          "cvm": "21652", "cnpj": "50.746.577/0001-15"},
    "EQTL3":  {"nome": "Equatorial",                     "cvm": "21580", "cnpj": "03.220.438/0001-73"},
    "GGPS3":  {"nome": "GPS Participações",              "cvm": "25712", "cnpj": "09.229.201/0001-30"},
    "HAPV3":  {"nome": "Hapvida",                        "cvm": "24217", "cnpj": "63.554.067/0001-98"},
    "INTB3":  {"nome": "Intelbras",                      "cvm": "25607", "cnpj": "82.901.000/0001-27"},
    "ITUB4":  {"nome": "Itaú Unibanco",                  "cvm": "19348", "cnpj": "60.872.504/0001-23"},
    "MULT3":  {"nome": "Multiplan",                      "cvm": "21300", "cnpj": "07.816.890/0001-53"},
    "ORVR3":  {"nome": "Orizon",                         "cvm": "25429", "cnpj": "11.421.994/0001-36"},
    "RADL3":  {"nome": "Raia Drogasil",                  "cvm": "17973", "cnpj": "61.585.865/0001-51"},
    "RENT3":  {"nome": "Localiza",                       "cvm": "17078", "cnpj": "16.670.085/0001-55"},
    "SMFT3":  {"nome": "Smartfit",                       "cvm": "25550", "cnpj": "31.613.412/0001-72"},
}

CNPJS = {v["cnpj"] for v in PORTFOLIO.values()}
CNPJ_TICKER = {v["cnpj"]: k for k, v in PORTFOLIO.items()}

# Códigos CVM 6 dígitos para cada empresa do portfólio
_COD_CVM_TICKER = {v["cvm"]: k for k, v in PORTFOLIO.items()}

# ---------------------------------------------------------------------------
# 1. Consultar RAD/CVM via API REST (sem Selenium)
# ---------------------------------------------------------------------------
def consultar_rad(data_alvo: date) -> tuple[list[dict], list[dict]]:
    """
    Chama a API REST do RAD/CVM (frmConsultaExternaCVM.aspx/ListarDocumentos)
    e retorna (fatos_portfolio, fatos_outros).
    """
    dt_str = data_alvo.strftime("%d/%m/%Y")
    payload = (
        f"{{ dataDe: '{dt_str}', dataAte: '{dt_str}', empresa: '', "
        f"setorAtividade: '-1', categoriaEmissor: '-1', situacaoEmissor: '-1', "
        f"tipoParticipante: '-1', dataReferencia: '', categoria: 'IPE_4_-1_-1', "
        f"periodo: '2', horaIni: '', horaFim: '', palavraChave: '', "
        f"ultimaDtRef: 'false', tipoEmpresa: '0', token: '', versaoCaptcha: '' }}"
    )

    print(f"Consultando RAD/CVM ({dt_str})...")
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

    linhas = [l for l in inner["dados"].split("&*") if l]
    portfolio_fatos, outros_fatos = [], []

    for linha in linhas:
        cols = linha.split("$&")
        if len(cols) < 11:
            continue

        # col[0] ex: "02008-7" → código CVM normalizado 6 dígitos: "002008"
        cod_norm = cols[0].strip().split("-")[0].zfill(6)

        dt_match = re.search(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2})", cols[6])
        dt_entrega = dt_match.group(1) if dt_match else ""

        dl_match = re.search(r"OpenDownloadDocumentos\('(\d+)','(\d+)','(\d+)'", cols[10])
        seq       = dl_match.group(1) if dl_match else ""
        versao    = dl_match.group(2) if dl_match else ""
        protocolo = dl_match.group(3) if dl_match else ""

        link = (
            f"{RAD_URL.rsplit('/', 1)[0]}/frmDownloadDocumento.aspx?"
            f"Tela=ext&numSequencia={seq}&numVersao={versao}"
            f"&numProtocolo={protocolo}&descTipo=IPE&CodigoInstituicao=1"
        ) if seq else ""

        fato = {
            "cod_cvm":      cod_norm,
            "nome":         cols[1].strip(),
            "categoria":    cols[2].strip(),
            "data_ref":     re.sub(r"<[^>]+>", "", cols[5]).strip(),
            "data_entrega": dt_entrega,
            "assunto":      cols[11].strip() if len(cols) > 11 else "",
            "link":         link,
        }

        ticker = _COD_CVM_TICKER.get(cod_norm)
        if ticker:
            fato["ticker"] = ticker
            portfolio_fatos.append(fato)
        else:
            outros_fatos.append(fato)

    print(f"  Portfólio: {len(portfolio_fatos)}  |  Outras empresas: {len(outros_fatos)}")
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
    def __init__(self, data_ref: str):
        super().__init__()
        self._data = data_ref
        self.set_auto_page_break(auto=True, margin=25)
        self.add_font("Arial", "",  r"C:\Windows\Fonts\arial.ttf")
        self.add_font("Arial", "B", r"C:\Windows\Fonts\arialbd.ttf")

    def header(self):
        self.set_fill_color(26, 54, 93)
        self.rect(0, 0, 210, 16, "F")
        self.set_font("Arial", "B", 9)
        self.set_text_color(255, 255, 255)
        self.set_xy(10, 3)
        self.cell(95, 10, "QUARTZ FIA — Fatos Relevantes")
        self.cell(95, 10, self._data, align="R")
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


def gerar_pdf(fatos_portfolio: list[dict], fatos_outros: list[dict], data_ref: date) -> Path:
    pdf = Relatorio(data_ref.strftime("%d/%m/%Y"))
    pdf.alias_nb_pages()
    pdf.add_page()

    # SEÇÃO 1 — Portfólio
    pdf.secao_header(
        "Seção 1 — Portfólio Quartz FIA",
        f"{len(fatos_portfolio)} fato(s) relevante(s) das empresas em carteira"
    )

    if not fatos_portfolio:
        pdf.set_font("Arial", "", 9)
        pdf.set_text_color(160, 160, 160)
        pdf.cell(0, 8, "Nenhum fato relevante publicado nesta data pelas empresas do portfólio.", align="C")
        pdf.ln(6)
    else:
        # Banner
        tickers = ", ".join(sorted(set(f.get("ticker", "?") for f in fatos_portfolio)))
        pdf.set_fill_color(235, 248, 255)
        pdf.set_draw_color(49, 130, 206)
        pdf.set_font("Arial", "B", 9)
        pdf.set_text_color(26, 54, 93)
        y = pdf.get_y()
        pdf.rect(10, y, 190, 10, "FD")
        pdf.set_xy(14, y + 1)
        pdf.cell(0, 8, f"{len(fatos_portfolio)} fato(s)  |  {tickers}")
        pdf.ln(14)

        for fato in fatos_portfolio:
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
            pdf.cell(0, 4, f"Assunto: {fato.get('assunto', 'N/D')}  |  Entrega: {fato.get('data_entrega', '')}",
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

    # SEÇÃO 2 — Outros
    pdf.add_page()
    pdf.secao_header(
        "Seção 2 — Demais Empresas da B3",
        f"{len(fatos_outros)} fato(s) relevante(s) de outras companhias"
    )

    if not fatos_outros:
        pdf.set_font("Arial", "", 9)
        pdf.set_text_color(160, 160, 160)
        pdf.cell(0, 8, "Nenhum fato relevante de outras empresas nesta data.", align="C")
    else:
        pdf.set_fill_color(235, 248, 255)
        pdf.set_draw_color(49, 130, 206)
        pdf.set_font("Arial", "B", 9)
        pdf.set_text_color(26, 54, 93)
        y = pdf.get_y()
        pdf.rect(10, y, 190, 10, "FD")
        pdf.set_xy(14, y + 1)
        pdf.cell(0, 8, f"{len(fatos_outros)} fato(s)  |  ordenado por nome")
        pdf.ln(14)

        for fato in sorted(fatos_outros, key=lambda x: x["nome"]):
            if pdf.get_y() > 260:
                pdf.add_page()

            pdf.set_font("Arial", "B", 9)
            pdf.set_text_color(26, 54, 93)
            pdf.cell(0, 5, fato["nome"], new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            pdf.set_font("Arial", "", 7)
            pdf.set_text_color(100, 100, 100)
            pdf.cell(0, 4, f"Assunto: {fato.get('assunto', 'N/D')}  |  Entrega: {fato.get('data_entrega', '')}  |  CVM: {fato.get('cod_cvm', '')}",
                     new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            if fato.get("link"):
                pdf.set_text_color(0, 0, 200)
                pdf.multi_cell(0, 4, fato["link"], link=fato["link"])
                pdf.set_text_color(100, 100, 100)

            pdf.set_draw_color(200, 200, 200)
            pdf.line(10, pdf.get_y() + 1, 200, pdf.get_y() + 1)
            pdf.ln(5)

    from datetime import datetime
    base = PASTA_SAIDA / f"quartz_fatos_{data_ref.strftime('%Y%m%d')}.pdf"
    arquivo = base
    try:
        pdf.output(str(arquivo))
    except PermissionError:
        # Arquivo aberto em outro programa — grava com sufixo de hora
        arquivo = PASTA_SAIDA / f"quartz_fatos_{data_ref.strftime('%Y%m%d')}_{datetime.now().strftime('%H%M')}.pdf"
        pdf.output(str(arquivo))
    return arquivo

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="QUARTZ FIA — Fatos Relevantes (tempo real)")
    parser.add_argument("--data", type=str, help="Data AAAA-MM-DD (padrão: ontem)")
    parser.add_argument("--hoje", action="store_true", help="Buscar fatos de hoje")
    args = parser.parse_args()

    if args.hoje:
        data_alvo = date.today()
    elif args.data:
        data_alvo = date.fromisoformat(args.data)
    else:
        data_alvo = date.today() - timedelta(days=1)

    # 1. Consultar CVM em tempo real
    raw_portfolio, raw_outros = consultar_rad(data_alvo)

    # 2. Extrair texto dos PDFs do portfólio
    for fato in raw_portfolio:
        ticker = fato.get("ticker", "?")
        print(f"Extraindo texto: {ticker} — {fato['nome']}")
        try:
            fato["texto"] = extrair_texto(fato.get("link", ""))
        except Exception as e:
            print(f"  Erro: {e}")
            fato["texto"] = "Não foi possível extrair o conteúdo deste fato relevante."

    # 3. Gerar PDF
    arquivo = gerar_pdf(raw_portfolio, raw_outros, data_alvo)
    print(f"\nPDF gerado: {arquivo.resolve()}")


if __name__ == "__main__":
    main()
