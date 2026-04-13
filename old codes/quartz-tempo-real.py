#!/usr/bin/env python3
"""
QUARTZ FIA — Fatos Relevantes em Tempo Real (CVM RAD/ENET)

Consulta o sistema RAD da CVM em tempo real e exibe novos Fatos Relevantes
conforme são publicados, destacando as empresas do portfólio.

Uso:
  python quartz-tempo-real.py                    # polling a cada 60s, data de hoje
  python quartz-tempo-real.py --intervalo 30     # polling a cada 30s
  python quartz-tempo-real.py --data 2026-04-09  # data específica (retroativo)
  python quartz-tempo-real.py --sem-texto        # não extrai texto dos PDFs

Dependências:
  pip install requests pdfplumber
"""

import sys, io, re, time, argparse
from datetime import date

try:
    import requests, pdfplumber
except ImportError as e:
    print(f"Dependência faltando: {e}\nInstale com: pip install requests pdfplumber")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Portfólio QUARTZ FIA — ticker: (CNPJ, código CVM 6 dígitos)
# ---------------------------------------------------------------------------
PORTFOLIO = {
    "ASAI3": ("03.560.312/0001-24", "025372"),  # SENDAS DISTRIBUIDORA S.A.
    "AXIA6": ("00.001.180/0001-26", "002437"),  # CENTRAIS ELET BRAS S.A. - ELETROBRAS
    "CEAB3": ("45.242.914/0001-05", "024848"),  # C&A MODAS S.A.
    "CSAN3": ("50.746.577/0001-15", "019836"),  # COSAN S.A.
    "EQTL3": ("03.220.438/0001-73", "020010"),  # EQUATORIAL S.A.
    "GGPS3": ("09.229.201/0001-30", "025712"),  # GPS PARTICIPAÇÕES E EMPREENDIMENTOS S.A.
    "HAPV3": ("63.554.067/0001-98", "024392"),  # HAPVIDA PARTICIPAÇÕES E INVESTIMENTOS S.A.
    "INTB3": ("82.901.000/0001-27", "025453"),  # INTELBRAS S.A.
    "ITUB4": ("60.872.504/0001-23", "019348"),  # ITAU UNIBANCO HOLDING S.A.
    "MULT3": ("07.816.890/0001-53", "020982"),  # MULTIPLAN - EMPREEND IMOBILIARIOS S.A.
    "ORVR3": ("11.421.994/0001-36", "025550"),  # ORIZON VALORIZAÇÃO DE RESÍDUOS S.A.
    "RADL3": ("61.585.865/0001-51", "005258"),  # RAIA DROGASIL S.A.
    "RENT3": ("16.670.085/0001-55", "019739"),  # LOCALIZA RENT A CAR S.A.
    "SMFT3": ("31.613.412/0001-72", "024260"),  # SMARTFIT ESCOLA DE GINÁSTICA E DANÇA S.A.
}

# Código CVM 6 dígitos → ticker
COD_CVM_TICKER = {cod: ticker for ticker, (_, cod) in PORTFOLIO.items()}

BASE_URL = "https://www.rad.cvm.gov.br/ENET/"

# ---------------------------------------------------------------------------
# Cores ANSI
# ---------------------------------------------------------------------------
VERDE   = "\033[92m"
AMARELO = "\033[93m"
AZUL    = "\033[94m"
CINZA   = "\033[90m"
BOLD    = "\033[1m"
RESET   = "\033[0m"

# ---------------------------------------------------------------------------
# Consulta ao RAD/ENET
# ---------------------------------------------------------------------------
def listar_documentos(data_alvo: date, session: requests.Session) -> list[dict]:
    """
    Chama frmConsultaExternaCVM.aspx/ListarDocumentos para Fatos Relevantes
    (categoria IPE_4_-1_-1) na data informada. Retorna lista de dicts.
    """
    dt_str = data_alvo.strftime("%d/%m/%Y")
    payload = (
        f"{{ dataDe: '{dt_str}', dataAte: '{dt_str}', empresa: '', "
        f"setorAtividade: '-1', categoriaEmissor: '-1', situacaoEmissor: '-1', "
        f"tipoParticipante: '-1', dataReferencia: '', categoria: 'IPE_4_-1_-1', "
        f"periodo: '2', horaIni: '', horaFim: '', palavraChave: '', "
        f"ultimaDtRef: 'false', tipoEmpresa: '0', token: '', versaoCaptcha: '' }}"
    )
    resp = session.post(
        BASE_URL + "frmConsultaExternaCVM.aspx/ListarDocumentos",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=30,
    )
    resp.raise_for_status()
    inner = resp.json()["d"]

    if inner.get("SolicitarCaptcha") == "S":
        raise RuntimeError("CVM solicitou CAPTCHA — tente novamente em alguns instantes.")
    if inner.get("temErro"):
        raise RuntimeError(inner.get("msgErro", "Erro desconhecido na API da CVM."))

    linhas = [l for l in inner["dados"].split("&*") if l]
    docs = []
    for linha in linhas:
        cols = linha.split("$&")
        if len(cols) < 11:
            continue

        # col[0] ex: "02008-7" → código CVM normalizado para 6 dígitos: "002008"
        cod_norm = cols[0].strip().split("-")[0].zfill(6)

        # Data/hora de entrega: "<spanOrder>20260410</spanOrder> 10/04/2026 08:59"
        dt_match = re.search(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2})", cols[6])
        dt_entrega = dt_match.group(1) if dt_match else ""

        # Parâmetros de download: OpenDownloadDocumentos('seq','versao','protocolo','IPE')
        dl_match = re.search(r"OpenDownloadDocumentos\('(\d+)','(\d+)','(\d+)'", cols[10])
        seq       = dl_match.group(1) if dl_match else ""
        versao    = dl_match.group(2) if dl_match else ""
        protocolo = dl_match.group(3) if dl_match else ""

        docs.append({
            "id":        seq or f"{cod_norm}_{dt_entrega}",  # chave única
            "cod_cvm":   cod_norm,
            "empresa":   cols[1].strip(),
            "categoria": cols[2].strip(),
            "tipo":      cols[3].strip(),
            "especie":   cols[11].strip() if len(cols) > 11 else "",
            "dt_entrega": dt_entrega,
            "assunto":   cols[12].strip() if len(cols) > 12 else "",
            "seq":       seq,
            "versao":    versao,
            "protocolo": protocolo,
        })
    return docs


def url_download(doc: dict) -> str:
    return (
        f"{BASE_URL}frmDownloadDocumento.aspx?"
        f"Tela=ext&numSequencia={doc['seq']}&numVersao={doc['versao']}"
        f"&numProtocolo={doc['protocolo']}&descTipo=IPE&CodigoInstituicao=1"
    )


def extrair_texto_pdf(url: str, session: requests.Session) -> str:
    resp = session.get(url, timeout=90)
    resp.raise_for_status()
    paginas = []
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for p in pdf.pages:
            t = p.extract_text()
            if t:
                paginas.append(t)
    return "\n\n".join(paginas)


# ---------------------------------------------------------------------------
# Exibição
# ---------------------------------------------------------------------------
def imprimir_header(data_alvo: date, intervalo: int):
    print()
    print(f"{BOLD}{'='*72}{RESET}")
    print(f"{BOLD}  QUARTZ FIA — Fatos Relevantes em Tempo Real  |  RAD/CVM{RESET}")
    print(f"{BOLD}{'='*72}{RESET}")
    print(f"  Data    : {data_alvo.strftime('%d/%m/%Y')}")
    print(f"  Polling : a cada {intervalo}s")
    print(f"  Carteira: {', '.join(sorted(PORTFOLIO.keys()))}")
    print(f"  Pressione Ctrl+C para encerrar.")
    print(f"{BOLD}{'-'*72}{RESET}\n")


def imprimir_documento(doc: dict, ticker: str | None, texto: str | None):
    if ticker:
        print(f"\n{BOLD}{VERDE}{'*'*72}{RESET}")
        print(f"{BOLD}{VERDE}  ★  PORTFÓLIO  |  {ticker}  —  {doc['empresa']}{RESET}")
        print(f"{BOLD}{VERDE}{'*'*72}{RESET}")
    else:
        print(f"\n{AZUL}{'-'*72}{RESET}")
        print(f"{BOLD}  {doc['empresa']}{RESET}")

    print(f"  {CINZA}Categoria :{RESET} {doc['categoria']}")
    if doc['tipo'] and doc['tipo'] != doc['categoria']:
        print(f"  {CINZA}Tipo      :{RESET} {doc['tipo']}")
    if doc['assunto']:
        print(f"  {CINZA}Assunto   :{RESET} {doc['assunto']}")
    print(f"  {CINZA}Publicado :{RESET} {doc['dt_entrega']}")
    if doc['seq']:
        print(f"  {CINZA}Download  :{RESET} {url_download(doc)}")

    if texto:
        print(f"\n  {CINZA}{'. . . '*14}{RESET}")
        trecho = texto[:2000].strip()
        # Indent each line
        for linha in trecho.splitlines():
            print(f"  {linha}")
        if len(texto) > 2000:
            print(f"\n  {CINZA}[... {len(texto)-2000} caracteres omitidos ...]{RESET}")


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="QUARTZ FIA — Fatos Relevantes em Tempo Real")
    parser.add_argument("--data",      type=str, help="Data AAAA-MM-DD (padrão: hoje)")
    parser.add_argument("--intervalo", type=int, default=60, help="Intervalo de polling em segundos (padrão: 60)")
    parser.add_argument("--sem-texto", action="store_true", help="Não extrai texto dos PDFs do portfólio")
    args = parser.parse_args()

    data_alvo = date.fromisoformat(args.data) if args.data else date.today()

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

    vistos: set[str] = set()
    ciclo = 0

    imprimir_header(data_alvo, args.intervalo)

    try:
        while True:
            ciclo += 1
            agora = time.strftime("%H:%M:%S")

            try:
                docs = listar_documentos(data_alvo, session)
                novos = [d for d in docs if d["id"] not in vistos]

                if novos:
                    print()  # limpa a linha de status
                    for doc in sorted(novos, key=lambda d: d["dt_entrega"]):
                        vistos.add(doc["id"])
                        ticker = COD_CVM_TICKER.get(doc["cod_cvm"])

                        texto = None
                        if ticker and not args.sem_texto and doc["seq"]:
                            try:
                                texto = extrair_texto_pdf(url_download(doc), session)
                            except Exception as e:
                                print(f"  {AMARELO}Aviso: não foi possível extrair PDF: {e}{RESET}")

                        imprimir_documento(doc, ticker, texto)

                    print(f"\n{CINZA}  [{agora}] +{len(novos)} novo(s) | Total acumulado: {len(vistos)} fatos | Ciclo #{ciclo}{RESET}")
                else:
                    print(
                        f"\r{CINZA}  [{agora}] Aguardando novos fatos... "
                        f"({len(vistos)} acumulados | ciclo #{ciclo}){RESET}   ",
                        end="", flush=True
                    )

            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"\n{AMARELO}  [{agora}] Erro na consulta: {e}{RESET}")

            time.sleep(args.intervalo)

    except KeyboardInterrupt:
        print(f"\n\n{BOLD}  Encerrando. Total de fatos processados: {len(vistos)}{RESET}\n")


if __name__ == "__main__":
    main()
