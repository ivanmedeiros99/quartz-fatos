#!/usr/bin/env python3
"""
QUARTZ FIA — Resumo de Fatos Relevantes (CVM)

Ao rodar, o script:
  1. Baixa o CSV de documentos da CVM
  2. Separa fatos relevantes em: portfólio Quartz x demais empresas da B3
  3. Baixa cada PDF das empresas do portfólio e extrai o texto
  4. Salva um PDF formatado na pasta ./relatorios

Uso:
  python quartz-sem-api.py                    # fatos de ontem
  python quartz-sem-api.py --data 2026-04-08  # data específica

Dependências:
  pip install requests pdfplumber fpdf2
"""

import sys, io, csv, zipfile, argparse
from pathlib import Path
from datetime import date, timedelta

try:
    import requests, pdfplumber
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos
except ImportError as e:
    print(f"Dependência faltando: {e}\nInstale com: pip install requests pdfplumber fpdf2")
    sys.exit(1)

PASTA_SAIDA = Path(__file__).parent / "relatorios"
PASTA_SAIDA.mkdir(exist_ok=True)

CVM_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"

# ---------------------------------------------------------------------------
# Portfólio QUARTZ FIA — ticker: CNPJ
# ---------------------------------------------------------------------------
PORTFOLIO = {
    "ASAI3":  "03.560.312/0001-24",
    "AXIA6":  "00.001.180/0001-26",
    "CEAB3":  "45.242.914/0001-05",
    "CSAN3":  "50.746.577/0001-15",
    "EQTL3":  "03.220.438/0001-73",
    "GGPS3":  "09.229.201/0001-30",
    "HAPV3":  "63.554.067/0001-98",
    "INTB3":  "82.901.000/0001-27",
    "ITUB4":  "60.872.504/0001-23",
    "MULT3":  "07.816.890/0001-53",
    "ORVR3":  "11.421.994/0001-36",
    "RADL3":  "61.585.865/0001-51",
    "RENT3":  "16.670.085/0001-55",
    "SMFT3":  "31.613.412/0001-72",
}

CNPJS = set(PORTFOLIO.values())
CNPJ_TICKER = {v: k for k, v in PORTFOLIO.items()}

# ---------------------------------------------------------------------------
# 1. Baixar CSV da CVM — retorna (fatos_portfolio, fatos_outros)
# ---------------------------------------------------------------------------
def buscar_fatos(data_alvo: date) -> tuple[list[dict], list[dict]]:
    print(f"Baixando dados da CVM ({data_alvo.year})...")
    resp = requests.get(CVM_URL.format(year=data_alvo.year), timeout=180)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        nome_csv = [n for n in z.namelist() if n.endswith(".csv")][0]
        with z.open(nome_csv) as f:
            rows = list(csv.DictReader(io.StringIO(f.read().decode("latin-1")), delimiter=";"))

    data_str = data_alvo.strftime("%Y-%m-%d")
    portfolio, outros = [], []
    for r in rows:
        if (r.get("Categoria") or "").strip() != "Fato Relevante":
            continue
        if (r.get("Data_Entrega") or "").strip() != data_str:
            continue
        cnpj = (r.get("CNPJ_Companhia") or "").strip()
        if cnpj in CNPJS:
            r["Ticker"] = CNPJ_TICKER[cnpj]
            portfolio.append(r)
        else:
            outros.append(r)

    print(f"  {len(portfolio)} fato(s) do portfólio  |  {len(outros)} de outras empresas da B3")
    return portfolio, outros

# ---------------------------------------------------------------------------
# 2. Extrair texto de um PDF
# ---------------------------------------------------------------------------
def extrair_texto(url: str) -> str:
    resp = requests.get(url, timeout=90)
    resp.raise_for_status()
    paginas = []
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for p in pdf.pages:
            t = p.extract_text()
            if t:
                paginas.append(t)
    return "\n\n".join(paginas)

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
        self.cell(0, 8, f"Fonte: Dados Abertos CVM  |  Pág. {self.page_no()}/{{nb}}", align="C")

    def secao_header(self, titulo: str, subtitulo: str = ""):
        """Faixa colorida de título de seção."""
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


def _banner_resumo(pdf: Relatorio, n: int, tickers_ou_label: str):
    """Caixa azul-clara com contagem de fatos."""
    pdf.set_fill_color(235, 248, 255)
    pdf.set_draw_color(49, 130, 206)
    pdf.set_font("Arial", "B", 9)
    pdf.set_text_color(26, 54, 93)
    y = pdf.get_y()
    pdf.rect(10, y, 190, 10, "FD")
    pdf.set_xy(14, y + 1)
    pdf.cell(0, 8, f"{n} fato(s) relevante(s)  |  {tickers_ou_label}")
    pdf.ln(14)


def _bloco_portfolio(pdf: Relatorio, fato: dict):
    """Renderiza um fato com texto completo (seção 1)."""
    if pdf.get_y() > 230:
        pdf.add_page()

    pdf.set_font("Arial", "B", 11)
    pdf.set_text_color(26, 54, 93)
    pdf.cell(0, 7, f"{fato['ticker']} — {fato['nome']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_draw_color(49, 130, 206)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(2)

    pdf.set_font("Arial", "", 7)
    pdf.set_text_color(130, 130, 130)
    pdf.cell(0, 4, f"Assunto: {fato['assunto']}  |  Entrega: {fato['data']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 4, f"PDF: {fato['link']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(2)

    pdf.set_font("Arial", "", 8)
    pdf.set_text_color(45, 55, 72)
    pdf.multi_cell(0, 4, fato["texto"])
    pdf.ln(5)


def _bloco_outros(pdf: Relatorio, fato: dict):
    """Renderiza metadados de um fato de empresa fora do portfólio (seção 2)."""
    if pdf.get_y() > 260:
        pdf.add_page()

    pdf.set_font("Arial", "B", 9)
    pdf.set_text_color(26, 54, 93)
    pdf.cell(0, 5, fato["nome"], new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_font("Arial", "", 7)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 4, f"Assunto: {fato['assunto']}  |  Entrega: {fato['data']}  |  CNPJ: {fato['cnpj']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 4, f"PDF: {fato['link']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.set_draw_color(200, 200, 200)
    pdf.line(10, pdf.get_y() + 1, 200, pdf.get_y() + 1)
    pdf.ln(5)


def gerar_pdf(fatos_portfolio: list[dict], fatos_outros: list[dict], data_ref: date) -> Path:
    pdf = Relatorio(data_ref.strftime("%d/%m/%Y"))
    pdf.alias_nb_pages()
    pdf.add_page()

    # ------------------------------------------------------------------
    # SEÇÃO 1 — Portfólio Quartz FIA
    # ------------------------------------------------------------------
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
        tickers = ", ".join(sorted(set(f["ticker"] for f in fatos_portfolio)))
        _banner_resumo(pdf, len(fatos_portfolio), tickers)
        for fato in fatos_portfolio:
            _bloco_portfolio(pdf, fato)

    # ------------------------------------------------------------------
    # SEÇÃO 2 — Demais empresas da B3
    # ------------------------------------------------------------------
    pdf.add_page()
    pdf.secao_header(
        "Seção 2 — Demais Empresas da B3",
        f"{len(fatos_outros)} fato(s) relevante(s) de outras companhias abertas"
    )

    if not fatos_outros:
        pdf.set_font("Arial", "", 9)
        pdf.set_text_color(160, 160, 160)
        pdf.cell(0, 8, "Nenhum fato relevante publicado nesta data por outras empresas.", align="C")
    else:
        _banner_resumo(pdf, len(fatos_outros), "ordenado por nome da companhia")
        for fato in sorted(fatos_outros, key=lambda x: x["nome"]):
            _bloco_outros(pdf, fato)

    arquivo = PASTA_SAIDA / f"quartz_fatos_{data_ref.strftime('%Y%m%d')}.pdf"
    pdf.output(str(arquivo))
    return arquivo

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="QUARTZ FIA — Fatos Relevantes")
    parser.add_argument("--data", type=str, help="Data AAAA-MM-DD (padrão: ontem)")
    args = parser.parse_args()

    data_alvo = date.fromisoformat(args.data) if args.data else date.today() - timedelta(days=1)

    # 1. Buscar e separar
    raw_portfolio, raw_outros = buscar_fatos(data_alvo)

    # 2. Portfólio: extrair texto de cada PDF
    fatos_portfolio = []
    for r in raw_portfolio:
        ticker = r["Ticker"]
        nome   = (r.get("Nome_Companhia") or "").strip()
        link   = (r.get("Link_Download")  or "").strip()
        assunto = (r.get("Assunto")       or "N/D").strip()
        dt     = (r.get("Data_Entrega")   or "").strip()

        print(f"Extraindo texto: {ticker} — {nome}")
        try:
            texto = extrair_texto(link)
        except Exception as e:
            print(f"  Erro ao baixar PDF: {e}")
            texto = "Não foi possível extrair o conteúdo deste fato relevante."

        fatos_portfolio.append({"ticker": ticker, "nome": nome, "assunto": assunto,
                                 "data": dt, "link": link, "texto": texto})

    # 3. Outros: apenas metadados (sem download de PDF)
    fatos_outros = []
    for r in raw_outros:
        fatos_outros.append({
            "nome":    (r.get("Nome_Companhia") or "").strip(),
            "cnpj":    (r.get("CNPJ_Companhia") or "").strip(),
            "assunto": (r.get("Assunto")        or "N/D").strip(),
            "data":    (r.get("Data_Entrega")   or "").strip(),
            "link":    (r.get("Link_Download")  or "").strip(),
        })

    # 4. Gerar PDF
    arquivo = gerar_pdf(fatos_portfolio, fatos_outros, data_alvo)
    print(f"\nPDF gerado: {arquivo.resolve()}")


if __name__ == "__main__":
    main()
