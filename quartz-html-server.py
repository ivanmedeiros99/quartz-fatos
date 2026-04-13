#!/usr/bin/env python3
"""
QUARTZ FIA — Servidor Web Local para Relatórios

Importa a lógica de busca do seu script existente (quartz-documentos-cvm.py),
gera páginas HTML responsivas e serve na rede local.

Acesse pelo celular: http://<IP_DO_COMPUTADOR>:8000

Uso:
  python quartz_servidor.py                    # ontem
  python quartz_servidor.py --data 2026-04-09  # data específica
  python quartz_servidor.py --hoje             # hoje
  python quartz_servidor.py --porta 9000       # porta customizada

Dependências (mesmas do script principal):
  pip install requests pdfplumber fpdf2
"""

import sys, io, re, copy, socket, argparse, threading, webbrowser
from pathlib import Path
from datetime import date, datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler

try:
    import requests, pdfplumber
except ImportError as e:
    print(f"Dependência faltando: {e}")
    print("Instale com: pip install requests pdfplumber")
    sys.exit(1)

PASTA_SAIDA = Path(__file__).parent / "relatorios"
PASTA_SAIDA.mkdir(exist_ok=True)

RAD_URL = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"

# ---------------------------------------------------------------------------
# Portfólio QUARTZ FIA — matching por nome (razão social parcial)
# ---------------------------------------------------------------------------
PORTFOLIO = {
    "ASAI3":  {"nomes": ["SENDAS DISTRIBUIDORA", "ASSAI", "ASSAÍ"]},
    "AXIA6":  {"nomes": ["CENTRAIS ELET", "ELETROBRAS", "ELETROBRÁS", "AXIA ENERGIA"]},
    "CEAB3":  {"nomes": ["C&A MODAS", "CEA MODAS", "C&amp;A MODAS"]},
    "CSAN3":  {"nomes": ["COSAN"]},
    "EQTL3":  {"nomes": ["EQUATORIAL S", "EQUATORIAL ENERGIA"]},
    "GGPS3":  {"nomes": ["GPS PARTICIPAC"]},
    "HAPV3":  {"nomes": ["HAPVIDA"]},
    "INTB3":  {"nomes": ["INTELBRAS"]},
    "ITUB4":  {"nomes": ["ITAU UNIBANCO", "ITAÚ UNIBANCO"]},
    "MULT3":  {"nomes": ["MULTIPLAN"]},
    "ORVR3":  {"nomes": ["ORIZON"]},
    "RADL3":  {"nomes": ["RAIA DROGASIL", "RAIADROGASIL"]},
    "RENT3":  {"nomes": ["LOCALIZA"]},
    "SMFT3":  {"nomes": ["SMARTFIT", "SMART FIT"]},
}

TIPOS_DOCUMENTO = {
    "fatos_relevantes": {
        "filtro_categoria": ["Fato Relevante"],
        "label": "Fatos Relevantes",
        "icone": "🔴",
        "sempre_gerar": True,
    },
    "comunicados": {
        "filtro_categoria": ["Comunicado ao Mercado"],
        "excluir_palavras": ["Apresentações a analistas", "Apresentacoes a analistas"],
        "label": "Comunicados ao Mercado",
        "icone": "🔵",
        "sempre_gerar": True,
    },
    "resultados": {
        "filtro_categoria": ["Dados Econômico-Financeiros", "Dados Economico-Financeiros",
                             "Comunicado ao Mercado"],
        "manter_palavras": ["Press", "press", "Release", "release", "Resultado",
                            "resultado", "Apresentaç", "apresentaç", "Apresentac",
                            "apresentac", "Earnings", "earnings", "Guidance",
                            "guidance", "Projeç", "projeç", "Projecoes", "projecoes"],
        "label": "Release de Resultados",
        "icone": "🟢",
        "sempre_gerar": False,
    },
}


def _identificar_ticker(nome_empresa: str) -> str | None:
    nome_upper = nome_empresa.upper()
    for ticker, info in PORTFOLIO.items():
        for fragmento in info["nomes"]:
            if fragmento in nome_upper:
                return ticker
    return None


# ---------------------------------------------------------------------------
# Consulta RAD/CVM
# ---------------------------------------------------------------------------
_CACHE: dict[str, list[dict]] = {}


def _buscar_todos_ipe(data_alvo: date) -> list[dict]:
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
        raise RuntimeError("CVM solicitou CAPTCHA — tente novamente.")
    if inner.get("temErro"):
        raise RuntimeError(inner.get("msgErro", "Erro na API da CVM."))

    dados = inner.get("dados", "")
    if not dados:
        return []

    fatos = []
    for linha in [l for l in dados.split("&*") if l]:
        cols = linha.split("$&")
        if len(cols) < 7:
            continue

        dt_m = re.search(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2})", cols[6])
        dl_m = re.search(r"OpenDownloadDocumentos\('(\d+)','(\d+)','(\d+)'", cols[10] if len(cols) > 10 else "")

        link = ""
        if dl_m:
            link = (
                f"https://www.rad.cvm.gov.br/ENET/frmDownloadDocumento.aspx?"
                f"Tela=ext&numSequencia={dl_m.group(1)}&numVersao={dl_m.group(2)}"
                f"&numProtocolo={dl_m.group(3)}&descTipo=IPE&CodigoInstituicao=1"
            )

        fatos.append({
            "nome":         cols[1].strip(),
            "categoria":    cols[2].strip() if len(cols) > 2 else "",
            "tipo_doc":     cols[3].strip() if len(cols) > 3 else "",
            "especie":      cols[4].strip() if len(cols) > 4 else "",
            "data_ref":     re.sub(r"<[^>]+>", "", cols[5]).strip(),
            "data_entrega": dt_m.group(1) if dt_m else "",
            "assunto":      re.sub(r"<[^>]+>", "", cols[11]).strip() if len(cols) > 11 else "",
            "link":         link,
        })

    return fatos


def _texto_buscavel(f):
    return f"{f.get('categoria','')} {f.get('tipo_doc','')} {f.get('especie','')} {f.get('assunto','')}"


def consultar_tipo(data_alvo, tipo_config):
    chave = data_alvo.isoformat()
    if chave not in _CACHE:
        print("  Buscando documentos na CVM...")
        _CACHE[chave] = _buscar_todos_ipe(data_alvo)
        print(f"  {len(_CACHE[chave])} documentos encontrados")

    todos = copy.deepcopy(_CACHE[chave])

    cats = tipo_config.get("filtro_categoria", [])
    if cats:
        todos = [f for f in todos if any(c in f.get("categoria", "") for c in cats)]

    manter = tipo_config.get("manter_palavras")
    if manter:
        todos = [f for f in todos if any(p in _texto_buscavel(f) for p in manter)]

    excluir = tipo_config.get("excluir_palavras")
    if excluir:
        todos = [f for f in todos if not any(p in _texto_buscavel(f) for p in excluir)]

    portfolio, outros = [], []
    for f in todos:
        ticker = _identificar_ticker(f["nome"])
        if ticker:
            f["ticker"] = ticker
            portfolio.append(f)
        else:
            outros.append(f)

    return portfolio, outros


# ---------------------------------------------------------------------------
# Gerar HTML responsivo
# ---------------------------------------------------------------------------
def _esc(text: str) -> str:
    """Escapa HTML."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _card_html(fato: dict, show_ticker: bool = True) -> str:
    ticker = fato.get("ticker", "")
    nome = _esc(fato["nome"])
    assunto = _esc(fato.get("assunto", "N/D"))
    entrega = _esc(fato.get("data_entrega", ""))
    tipo_doc = _esc(fato.get("tipo_doc", ""))
    link = fato.get("link", "")

    ticker_html = f'<span class="ticker">{_esc(ticker)}</span>' if show_ticker and ticker else ""
    tipo_html = f' <span class="tag">{tipo_doc}</span>' if tipo_doc else ""
    link_html = f'<a href="{_esc(link)}" target="_blank" class="pdf-link">Ver PDF original</a>' if link else ""

    return f"""
    <div class="card">
      <div class="card-header">{ticker_html} {nome}{tipo_html}</div>
      <div class="card-meta">{assunto} &middot; {entrega}</div>
      {link_html}
    </div>"""


def gerar_html(data_alvo: date, resultados: dict) -> str:
    data_fmt = data_alvo.strftime("%d/%m/%Y")
    hora = datetime.now().strftime("%H:%M")

    # Tabs
    tabs_html = ""
    panels_html = ""
    primeiro = True

    for chave, dados in resultados.items():
        tipo = TIPOS_DOCUMENTO[chave]
        portfolio = dados["portfolio"]
        outros = dados["outros"]
        total = len(portfolio) + len(outros)
        active = "active" if primeiro else ""

        tabs_html += f'<button class="tab {active}" onclick="showTab(\'{chave}\')">{tipo["icone"]} {tipo["label"]} <span class="badge">{total}</span></button>\n'

        panel = f'<div class="panel {active}" id="panel-{chave}">\n'

        # Seção portfólio
        panel += '<h2 class="section-title">Portfólio Quartz FIA</h2>\n'
        if portfolio:
            for f in portfolio:
                panel += _card_html(f, show_ticker=True)
        else:
            panel += '<p class="empty">Nenhum registro do portfólio nesta data.</p>\n'

        # Seção outros
        panel += f'<h2 class="section-title">Demais Empresas da B3 ({len(outros)})</h2>\n'
        if outros:
            for f in sorted(outros, key=lambda x: x["nome"]):
                panel += _card_html(f, show_ticker=False)
        else:
            panel += '<p class="empty">Nenhum registro de outras empresas.</p>\n'

        panel += '</div>\n'
        panels_html += panel
        primeiro = False

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QUARTZ FIA — {data_fmt}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
         background: #f0f2f5; color: #1a202c; -webkit-font-smoothing: antialiased; }}

  .header {{ background: linear-gradient(135deg, #1a365d, #2b6cb0);
             color: white; padding: 20px 16px; position: sticky; top: 0; z-index: 10; }}
  .header h1 {{ font-size: 18px; font-weight: 600; }}
  .header .meta {{ font-size: 12px; opacity: 0.8; margin-top: 4px; }}

  .tabs {{ display: flex; gap: 0; overflow-x: auto; background: #fff;
           border-bottom: 2px solid #e2e8f0; position: sticky; top: 68px; z-index: 9;
           -webkit-overflow-scrolling: touch; }}
  .tab {{ flex: 1; min-width: 0; padding: 12px 8px; border: none; background: none;
          font-size: 13px; font-weight: 500; color: #718096; cursor: pointer;
          border-bottom: 3px solid transparent; white-space: nowrap;
          transition: all 0.2s; }}
  .tab.active {{ color: #2b6cb0; border-bottom-color: #2b6cb0; background: #ebf8ff; }}
  .badge {{ display: inline-block; background: #e2e8f0; color: #4a5568;
            border-radius: 10px; padding: 1px 7px; font-size: 11px; margin-left: 4px; }}
  .tab.active .badge {{ background: #bee3f8; color: #2b6cb0; }}

  .panel {{ display: none; padding: 12px; }}
  .panel.active {{ display: block; }}

  .section-title {{ font-size: 14px; font-weight: 600; color: #2d3748;
                    padding: 12px 4px 8px; border-bottom: 1px solid #e2e8f0;
                    margin-bottom: 8px; }}

  .card {{ background: #fff; border-radius: 10px; padding: 14px 16px;
           margin-bottom: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
           border-left: 3px solid #e2e8f0; }}
  .card:has(.ticker) {{ border-left-color: #3182ce; }}
  .card-header {{ font-size: 14px; font-weight: 600; color: #1a365d; line-height: 1.4; }}
  .ticker {{ display: inline-block; background: #ebf8ff; color: #2b6cb0;
             font-size: 11px; font-weight: 700; padding: 2px 6px; border-radius: 4px;
             margin-right: 6px; vertical-align: middle; }}
  .tag {{ display: inline-block; background: #f7fafc; color: #718096;
          font-size: 10px; padding: 1px 5px; border-radius: 3px;
          border: 1px solid #e2e8f0; margin-left: 6px; vertical-align: middle; }}
  .card-meta {{ font-size: 12px; color: #a0aec0; margin-top: 4px; }}
  .pdf-link {{ display: inline-block; margin-top: 8px; font-size: 12px;
               color: #3182ce; text-decoration: none; font-weight: 500; }}
  .pdf-link:active {{ color: #2c5282; }}

  .empty {{ color: #a0aec0; font-size: 13px; text-align: center; padding: 24px; }}

  .footer {{ text-align: center; padding: 16px; font-size: 11px; color: #a0aec0; }}

  @media (min-width: 768px) {{
    body {{ max-width: 720px; margin: 0 auto; }}
    .tab {{ font-size: 14px; padding: 14px 16px; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>QUARTZ FIA</h1>
  <div class="meta">{data_fmt} &middot; Atualizado às {hora}</div>
</div>

<div class="tabs">
  {tabs_html}
</div>

{panels_html}

<div class="footer">
  Fonte: RAD/CVM (tempo real) &middot; Gerado às {hora}
</div>

<script>
function showTab(id) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelector(`[onclick*="${{id}}"]`).classList.add('active');
  document.getElementById('panel-' + id).classList.add('active');
}}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Servidor HTTP local
# ---------------------------------------------------------------------------
def _get_local_ip() -> str:
    """Descobre o IP local da máquina na rede Wi-Fi."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class QuartzHandler(SimpleHTTPRequestHandler):
    """Serve apenas a pasta de relatórios."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PASTA_SAIDA), **kwargs)

    def log_message(self, format, *args):
        pass  # silenciar logs do servidor


def iniciar_servidor(porta: int):
    ip = _get_local_ip()
    server = HTTPServer(("0.0.0.0", porta), QuartzHandler)

    print(f"\n{'='*60}")
    print(f"  Servidor ativo!")
    print(f"  No computador:  http://localhost:{porta}/index.html")
    print(f"  No celular:     http://{ip}:{porta}/index.html")
    print(f"{'='*60}")
    print(f"  Pressione Ctrl+C para encerrar.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor encerrado.")
        server.server_close()


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="QUARTZ FIA — Relatórios via navegador")
    parser.add_argument("--data", type=str, help="Data AAAA-MM-DD (padrão: ontem)")
    parser.add_argument("--hoje", action="store_true", help="Buscar registros de hoje")
    parser.add_argument("--porta", type=int, default=8000, help="Porta do servidor (padrão: 8000)")
    args = parser.parse_args()

    if args.hoje:
        data_alvo = date.today()
    elif args.data:
        data_alvo = date.fromisoformat(args.data)
    else:
        data_alvo = date.today() - timedelta(days=1)

    print(f"Data alvo: {data_alvo.strftime('%d/%m/%Y')}")
    print("=" * 60)

    # 1. Consultar CVM e montar resultados
    resultados = {}
    for chave, tipo in TIPOS_DOCUMENTO.items():
        print(f"\n[{tipo['icone']}] {tipo['label']}")
        try:
            portfolio, outros = consultar_tipo(data_alvo, tipo)
        except Exception as e:
            print(f"  ERRO: {e}")
            portfolio, outros = [], []

        total = len(portfolio) + len(outros)
        print(f"  Portfólio: {len(portfolio)}  |  Outras: {len(outros)}")

        if not tipo["sempre_gerar"] and total == 0:
            print(f"  Nenhum registro — seção omitida.")
            continue

        resultados[chave] = {"portfolio": portfolio, "outros": outros}

    # 2. Gerar HTML
    html = gerar_html(data_alvo, resultados)
    arquivo = PASTA_SAIDA / "index.html"
    arquivo.write_text(html, encoding="utf-8")
    print(f"\nHTML gerado: {arquivo.resolve()}")

    # 3. Abrir no navegador local e iniciar servidor
    webbrowser.open(f"http://localhost:{args.porta}/index.html")
    iniciar_servidor(args.porta)


if __name__ == "__main__":
    main()
