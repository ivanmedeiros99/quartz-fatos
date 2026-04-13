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
  pip install selenium pdfplumber fpdf2
  (+ Chrome e ChromeDriver instalados)

Instalação do ChromeDriver:
  - Windows: baixe em https://googlechromelabs.github.io/chrome-for-testing/
  - Mac:     brew install chromedriver
  - Linux:   sudo apt install chromium-chromedriver
"""

import sys, io, re, time, argparse
from pathlib import Path
from datetime import date, timedelta

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    import pdfplumber, requests
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos
except ImportError as e:
    print(f"Dependência faltando: {e}")
    print("Instale com: pip install selenium pdfplumber fpdf2")
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

# ---------------------------------------------------------------------------
# 1. Consultar RAD/CVM via Selenium
# ---------------------------------------------------------------------------
def criar_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    return webdriver.Chrome(options=opts)


def consultar_rad(data_alvo: date) -> tuple[list[dict], list[dict]]:
    """
    Acessa o RAD da CVM, filtra por 'Fato Relevante' no dia,
    e retorna (fatos_portfolio, fatos_outros).
    """
    driver = criar_driver()
    portfolio_fatos = []
    outros_fatos = []

    try:
        print(f"Acessando RAD/CVM...")
        driver.get(RAD_URL)
        wait = WebDriverWait(driver, 30)

        # Esperar o formulário carregar
        wait.until(EC.presence_of_element_located((By.ID, "cboCategorias")))
        time.sleep(2)

        # Selecionar categoria: Fato Relevante
        print("Configurando filtros...")
        cat_select = Select(driver.find_element(By.ID, "cboCategorias"))
        cat_select.select_by_visible_text("Fato Relevante")
        time.sleep(1)

        # Selecionar período: "No período"
        radio_periodo = driver.find_element(By.ID, "rdPeriodo")
        radio_periodo.click()
        time.sleep(1)

        # Preencher data De e Até (mesmo dia)
        data_fmt = data_alvo.strftime("%d/%m/%Y")
        campo_de = driver.find_element(By.ID, "txtDataIni")
        campo_de.clear()
        campo_de.send_keys(data_fmt)

        campo_ate = driver.find_element(By.ID, "txtDataFim")
        campo_ate.clear()
        campo_ate.send_keys(data_fmt)

        # Clicar em Consultar
        print(f"Consultando fatos relevantes de {data_fmt}...")
        btn = driver.find_element(By.ID, "btnConsulta")
        btn.click()

        # Esperar resultados carregarem
        time.sleep(5)
        wait.until(EC.presence_of_element_located((By.ID, "grdDocumentos")))
        time.sleep(3)

        # Extrair linhas da tabela de resultados
        tabela = driver.find_element(By.ID, "grdDocumentos")
        linhas = tabela.find_elements(By.TAG_NAME, "tr")

        print(f"  {len(linhas) - 1} resultado(s) encontrado(s) na CVM")

        for linha in linhas[1:]:  # pula header
            colunas = linha.find_elements(By.TAG_NAME, "td")
            if len(colunas) < 7:
                continue

            try:
                cod_cvm = colunas[0].text.strip()
                empresa = colunas[1].text.strip()
                categoria = colunas[2].text.strip()
                dt_ref = colunas[5].text.strip()
                dt_entrega = colunas[6].text.strip()
                status = colunas[7].text.strip() if len(colunas) > 7 else ""
                assunto = colunas[-1].text.strip() if len(colunas) > 10 else ""

                # Tentar obter link do documento
                link = ""
                try:
                    link_el = colunas[10].find_element(By.TAG_NAME, "a") if len(colunas) > 10 else None
                    if link_el:
                        link = link_el.get_attribute("href") or ""
                except:
                    # Buscar qualquer link na linha
                    try:
                        link_el = linha.find_element(By.CSS_SELECTOR, "a[href*='frmDownloadDocumento'], a[href*='frmExibirArquivoIPE']")
                        link = link_el.get_attribute("href") or ""
                    except:
                        pass

                fato = {
                    "cod_cvm": cod_cvm,
                    "nome": empresa,
                    "categoria": categoria,
                    "data_ref": dt_ref,
                    "data_entrega": dt_entrega,
                    "status": status,
                    "assunto": assunto,
                    "link": link,
                }

                # Classificar: portfolio vs outros
                # Verificar pelo código CVM
                codigos_portfolio = {v["cvm"] for v in PORTFOLIO.values()}
                if cod_cvm in codigos_portfolio:
                    ticker = next(k for k, v in PORTFOLIO.items() if v["cvm"] == cod_cvm)
                    fato["ticker"] = ticker
                    portfolio_fatos.append(fato)
                else:
                    outros_fatos.append(fato)

            except Exception as e:
                print(f"  Erro ao processar linha: {e}")
                continue

    finally:
        driver.quit()

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
                pdf.cell(0, 4, f"PDF: {fato['link']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
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

            pdf.set_draw_color(200, 200, 200)
            pdf.line(10, pdf.get_y() + 1, 200, pdf.get_y() + 1)
            pdf.ln(5)

    arquivo = PASTA_SAIDA / f"quartz_fatos_{data_ref.strftime('%Y%m%d')}.pdf"
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
