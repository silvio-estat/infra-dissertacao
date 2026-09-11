#!/usr/bin/env python3
"""
comum.py — o que todos os geradores de scripts/geradores/ compartilham.

Ha um gerador por fonte (gerar_c2a.py, gerar_c2b.py, gerar_relper.py,
gerar_fogos.py, gerar_intel.py, gerar_voz.py) e este modulo por baixo de
todos: seeds, operacao, geometria, datas, escrita de arquivo + sidecar,
"escaneamento" de pagina e registro dos defeitos propositais.

PRINCIPIO
  Toda saida nasce de uma lista de FATOS (cenario.py), a "verdade" interna do
  gerador. Ela existe para que fontes diferentes falem do MESMO fato — o
  avistamento que aparece no radio, no relato do C2_B e no informe — e NAO e
  usada para medir acerto da extracao: a IA aponta, o estado-maior confere
  (decisao de projeto, 2026-09-10).

SIDECAR
  Todo binario (xlsx, pdf, jpg, wav) sai em par com um .json de mesmo nome:
  a proveniencia (operacao, quem remeteu, quando). O sidecar entra em
  RECEPCAO_BRUTA; o binario, em ARQUIVO. Ver canonico/modelo_canonico.yaml.

SIGILO
  Fontes por codigo generico (C2_A, C2_B). Unidades, lugares e pessoas sao
  inventados; so a operacao (Perseu 2024) e publica.
"""

import csv
import hashlib
import json
import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import yaml
from PIL import Image, ImageDraw, ImageFilter, ImageFont

from gerar_seeds import AZIMUTE, KM_LAT, KM_LON, LAT0, LON0, _latlon

RAIZ = Path(__file__).resolve().parents[2]
DIR_CANONICO = RAIZ / "canonico"
DIR_SEEDS = DIR_CANONICO / "seeds"
SAIDA_PADRAO = RAIZ / "dados_sinteticos"

FUSO = timezone(timedelta(hours=-3))          # horario de Brasilia, sem horario de verao em 2024
UTC = timezone.utc
MESES_DTG = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"]
_NS = uuid.UUID("6f1c2a4e-0000-4000-8000-0000000000dc")   # namespace fixo: ids reproduziveis


# =============================================================================
# Seeds
# =============================================================================
def _ler_csv(caminho: Path) -> list[dict]:
    with open(caminho, encoding="utf-8") as fh:
        linhas = [l for l in fh if not l.startswith("#")]
    return list(csv.DictReader(linhas))


@dataclass
class Operacao:
    cod: str
    nome: str
    tipo: str
    inicio: datetime
    fim: datetime
    area_wkt: str
    unidade_resp: str

    # O C2_A identifica a operacao pela chave natural (nome, ano) — e o que ele
    # exporta. O codigo canonico e a mesma coisa formatada: NOME_ANO.
    @property
    def nome_c2a(self) -> str:
        return self.cod.rsplit("_", 1)[0].capitalize()

    @property
    def ano(self) -> str:
        return self.cod.rsplit("_", 1)[1]

    @property
    def dias(self) -> list[date]:
        """Dias locais da operacao, do primeiro ao ultimo, inclusive."""
        d0 = self.inicio.astimezone(FUSO).date()
        d1 = self.fim.astimezone(FUSO).date()
        return [d0 + timedelta(days=i) for i in range((d1 - d0).days + 1)]


def carregar_operacao() -> Operacao:
    linha = _ler_csv(DIR_SEEDS / "operacoes.csv")[0]
    return Operacao(
        cod=linha["OPERACAO_COD"], nome=linha["OPERACAO_NOME"], tipo=linha["OPERACAO_TIPO_COD"],
        inicio=datetime.fromisoformat(linha["INICIO_DATA"].replace("Z", "+00:00")),
        fim=datetime.fromisoformat(linha["FIM_DATA"].replace("Z", "+00:00")),
        area_wkt=linha["AREA_WKT"], unidade_resp=linha["UNIDADE_RESP_COD"])


def carregar_dominios() -> dict:
    with open(DIR_CANONICO / "modelo_canonico.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)["dominios"]


# =============================================================================
# Geometria
# =============================================================================
def axial(lat: float, lon: float) -> tuple[float, float]:
    """Inverso de _latlon: (lat, lon) -> (km ao longo do eixo, km perpendicular)."""
    az = math.radians(AZIMUTE)
    norte = (lat - LAT0) / KM_LAT
    leste = (lon - LON0) / KM_LON
    return norte * math.cos(az) + leste * math.sin(az), norte * math.sin(az) - leste * math.cos(az)


def deslocar(lat: float, lon: float, norte_km: float, leste_km: float) -> tuple[float, float]:
    return lat + norte_km * KM_LAT, lon + leste_km * KM_LON


def jitter(lat: float, lon: float, raio_km: float, rng: random.Random) -> tuple[float, float]:
    """Ponto aleatorio dentro de um circulo de `raio_km` — 'perto de' um lugar."""
    r = raio_km * math.sqrt(rng.random())
    t = rng.uniform(0, 2 * math.pi)
    return deslocar(lat, lon, r * math.cos(t), r * math.sin(t))


def ponto_de_wkt(wkt: str) -> tuple[float, float]:
    """'POINT(lon lat)' -> (lat, lon)."""
    lon, lat = wkt.strip()[6:-1].split()
    return float(lat), float(lon)


def wkt_ponto(lat: float, lon: float) -> str:
    return f"POINT({lon:.6f} {lat:.6f})"


def dms(lat: float, lon: float) -> str:
    """Grau-minuto-segundo por extenso, como o C2_B mostra na tela."""
    def parte(v, pos, neg):
        h = pos if v >= 0 else neg
        v = abs(v)
        g = int(v)
        m = int((v - g) * 60)
        s = ((v - g) * 60 - m) * 60
        return f"{g}° {m:02d}' {s:04.1f}\" {h}"
    return f"{parte(lat, 'N', 'S')}, {parte(lon, 'E', 'W')}"


def utm(lat: float, lon: float) -> tuple[float, float, int]:
    """WGS84 -> UTM (E, N, zona). Formulas classicas; erro submetrico, suficiente aqui."""
    a, f = 6378137.0, 1 / 298.257223563
    e2 = f * (2 - f)
    k0 = 0.9996
    zona = int((lon + 180) // 6) + 1
    lon0 = math.radians((zona - 1) * 6 - 180 + 3)
    phi, lam = math.radians(lat), math.radians(lon)
    n = a / math.sqrt(1 - e2 * math.sin(phi) ** 2)
    t = math.tan(phi) ** 2
    ep2 = e2 / (1 - e2)
    c = ep2 * math.cos(phi) ** 2
    a_ = math.cos(phi) * (lam - lon0)
    m = a * ((1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256) * phi
             - (3 * e2 / 8 + 3 * e2 ** 2 / 32 + 45 * e2 ** 3 / 1024) * math.sin(2 * phi)
             + (15 * e2 ** 2 / 256 + 45 * e2 ** 3 / 1024) * math.sin(4 * phi)
             - (35 * e2 ** 3 / 3072) * math.sin(6 * phi))
    e = k0 * n * (a_ + (1 - t + c) * a_ ** 3 / 6
                  + (5 - 18 * t + t ** 2 + 72 * c - 58 * ep2) * a_ ** 5 / 120) + 500000
    nn = k0 * (m + n * math.tan(phi) * (a_ ** 2 / 2 + (5 - t + 9 * c + 4 * c ** 2) * a_ ** 4 / 24
                                        + (61 - 58 * t + t ** 2 + 600 * c - 330 * ep2) * a_ ** 6 / 720))
    if lat < 0:
        nn += 10_000_000
    return e, nn, zona


def decametrica(lat: float, lon: float) -> str:
    """Coordenada metrica militar 'EEEEE-NNNNN': E e N em decametros, ultimos 5 digitos.
    E a forma do Anexo I ('03250-10550'). A transformacao decametrica_para_ponto faz o inverso
    ancorada na zona UTM da area de operacoes."""
    e, n, _ = utm(lat, lon)
    return f"{int(e // 10) % 100000:05d}-{int(n // 10) % 100000:05d}"


def geojson_ponto(lat: float, lon: float) -> dict:
    return {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]}


# =============================================================================
# Datas
# =============================================================================
def local(dia: date, hora: int, minuto: int = 0, segundo: int = 0) -> datetime:
    """Instante local (Brasilia) -> datetime com fuso, em UTC."""
    return datetime(dia.year, dia.month, dia.day, hora, minuto, segundo, tzinfo=FUSO).astimezone(UTC)


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def br(dt: datetime) -> str:
    return dt.astimezone(FUSO).strftime("%d/%m/%Y %H:%M:%S")


def dtg(dt: datetime) -> str:
    """Grupo data-hora militar: '271430 NOV 24' (dia, hora, minuto, mes, ano) em hora local."""
    d = dt.astimezone(FUSO)
    return f"{d.day:02d}{d.hour:02d}{d.minute:02d} {MESES_DTG[d.month - 1]} {d.year % 100:02d}"


def hora_curta(dt: datetime) -> str:
    return dt.astimezone(FUSO).strftime("%H%M")


# =============================================================================
# Identificadores e arquivos
# =============================================================================
def id_det(*partes) -> str:
    """Identificador deterministico (UUID v5) a partir das partes. Mesma semente, mesmos ids."""
    return str(uuid.uuid5(_NS, "|".join(str(p) for p in partes)))


def sha256_arquivo(caminho: Path) -> str:
    return hashlib.sha256(caminho.read_bytes()).hexdigest()


def escrever_json(caminho: Path, obj) -> Path:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return caminho


def escrever_sidecar(binario: Path, dados: dict) -> Path:
    """O .json de proveniencia ao lado do binario: mesmo nome, extensao .json."""
    return escrever_json(binario.with_suffix(".json"), dados)


# =============================================================================
# Pagina "escaneada": desenha um formulario com Pillow e degrada como scanner
# =============================================================================
A4 = (1654, 2339)   # A4 a 200 dpi

_FONTES = {
    "sans": ["/usr/share/fonts/liberation-sans-fonts/LiberationSans-Regular.ttf",
             "/usr/share/fonts/liberation/LiberationSans-Regular.ttf",
             "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
             "/usr/share/fonts/dejavu-sans-fonts/DejaVuSans.ttf",
             "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"],
    "sans_b": ["/usr/share/fonts/liberation-sans-fonts/LiberationSans-Bold.ttf",
               "/usr/share/fonts/liberation/LiberationSans-Bold.ttf",
               "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
               "/usr/share/fonts/dejavu-sans-fonts/DejaVuSans-Bold.ttf",
               "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"],
    "mono": ["/usr/share/fonts/liberation-mono-fonts/LiberationMono-Regular.ttf",
             "/usr/share/fonts/liberation/LiberationMono-Regular.ttf",
             "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
             "/usr/share/fonts/dejavu-sans-mono-fonts/DejaVuSansMono.ttf",
             "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"],
}


def fonte(tamanho: int, negrito: bool = False, mono: bool = False) -> ImageFont.FreeTypeFont:
    chave = "mono" if mono else ("sans_b" if negrito else "sans")
    for caminho in _FONTES[chave]:
        if Path(caminho).exists():
            return ImageFont.truetype(caminho, tamanho)
    return ImageFont.load_default()


def pagina() -> tuple[Image.Image, ImageDraw.ImageDraw]:
    img = Image.new("RGB", A4, "white")
    return img, ImageDraw.Draw(img)


def texto_centrado(draw, y, txt, f, largura=A4[0], x0=0, fill="black"):
    w = draw.textlength(txt, font=f)
    draw.text((x0 + (largura - w) / 2, y), txt, font=f, fill=fill)


def desenhar_tabela(draw, x0, y0, larguras, cabecalho, linhas, f_cab, f_txt,
                    alt_cab=46, alt_lin=44, margem=8):
    """Tabela simples com grade. Celula de texto numa linha; o que nao couber e cortado."""
    x1 = x0 + sum(larguras)
    y = y0
    # cabecalho
    draw.rectangle([x0, y, x1, y + alt_cab], outline="black", width=2)
    x = x0
    for w, txt in zip(larguras, cabecalho):
        draw.line([x, y, x, y + alt_cab], fill="black", width=2)
        draw.text((x + margem, y + (alt_cab - f_cab.size) / 2), str(txt), font=f_cab, fill="black")
        x += w
    y += alt_cab
    for linha in linhas:
        draw.rectangle([x0, y, x1, y + alt_lin], outline="black", width=1)
        x = x0
        for w, txt in zip(larguras, linha):
            draw.line([x, y, x, y + alt_lin], fill="black", width=1)
            txt = "" if txt is None else str(txt)
            while txt and draw.textlength(txt, font=f_txt) > w - 2 * margem:
                txt = txt[:-1]
            draw.text((x + margem, y + (alt_lin - f_txt.size) / 2), txt, font=f_txt, fill="black")
            x += w
        y += alt_lin
    return y


def assinar(draw, x, y, rng: random.Random, largura=260):
    """Rabisco de assinatura: uma linha quebrada aleatoria, para o OCR ter o que ignorar."""
    pts, px, py = [], x, y
    for _ in range(rng.randint(14, 24)):
        px += largura / 18 + rng.uniform(-6, 6)
        py = y + rng.uniform(-22, 22)
        pts.append((px, py))
    draw.line(pts, fill=(20, 20, 60), width=3)


def escanear(img: Image.Image, rng: random.Random, np_rng: np.random.Generator) -> Image.Image:
    """Degrada a pagina como um scanner de campo: leve inclinacao, ruido, fundo cinza, desfoque."""
    g = img.convert("L").rotate(rng.uniform(-1.4, 1.4), resample=Image.BICUBIC, fillcolor=255)
    arr = np.asarray(g).astype(np.float32)
    arr = arr * rng.uniform(0.86, 0.98) + rng.uniform(0, 18)              # contraste e fundo
    arr += np_rng.normal(0, rng.uniform(5, 12), arr.shape)                 # ruido do sensor
    g = Image.fromarray(np.clip(arr, 0, 255).astype(np.uint8))
    return g.filter(ImageFilter.GaussianBlur(rng.uniform(0.35, 0.8)))


def salvar_pdf(paginas: list[Image.Image], caminho: Path) -> Path:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    paginas[0].save(caminho, "PDF", resolution=200.0, save_all=True, append_images=paginas[1:])
    return caminho


# =============================================================================
# Contexto: tudo que um gerador precisa, num objeto so
# =============================================================================
@dataclass
class Contexto:
    seed: int = 2024
    saida: Path = SAIDA_PADRAO
    escala: float = 1.0          # multiplica os volumes; 0.1 para um teste rapido

    def __post_init__(self):
        self.saida = Path(self.saida)
        self.rng = random.Random(self.seed)
        self.np_rng = np.random.default_rng(self.seed)
        self.operacao = carregar_operacao()
        self.unidades = _ler_csv(DIR_SEEDS / "unidades.csv")
        self.gazetteer = _ler_csv(DIR_SEEDS / "gazetteer.csv")
        self.dominios = carregar_dominios()
        self.landing = self.saida / "landing" / self.operacao.cod.lower()
        self.verdade = self.saida / "verdade"
        self._por_cod = {u["UNIDADE_COD"]: u for u in self.unidades}
        for g in self.gazetteer:
            g["lat"], g["lon"] = ponto_de_wkt(g["GEOMETRIA_WKT"])
            g["km"], g["perp"] = axial(g["lat"], g["lon"])
        self.setores = self._setores()
        self.fatos: list = []
        self.defeitos: list[dict] = []
        self.resumo: dict = {}

    # --- unidades ---------------------------------------------------------
    def unidade(self, cod: str) -> dict:
        return self._por_cod[cod]

    def sigla(self, cod: str) -> str:
        return self._por_cod[cod]["UNIDADE_SIGLA"]

    def por_escalao(self, escalao: str) -> list[dict]:
        return [u for u in self.unidades if u["ESCALAO_COD"] == escalao]

    def filhas(self, cod: str) -> list[dict]:
        return [u for u in self.unidades if u["SUPERIOR_COD"] == cod]

    def om_de(self, cod: str) -> str:
        """Sobe a hierarquia ate a Organizacao Militar (OM) da unidade."""
        u = self._por_cod[cod]
        while u["ESCALAO_COD"] not in ("OM", "BDA"):
            u = self._por_cod[u["SUPERIOR_COD"]]
        return u["UNIDADE_COD"]

    def _setores(self) -> dict[str, tuple[float, float]]:
        """Cada OM recebe uma faixa de km ao longo do eixo: e o 'setor' dela na operacao."""
        oms = self.por_escalao("OM")
        largura = 110 / len(oms)
        return {om["UNIDADE_COD"]: (5 + i * largura, 5 + (i + 1) * largura) for i, om in enumerate(oms)}

    # --- lugares ----------------------------------------------------------
    def locais(self, om_cod: str | None = None, tipos: tuple[str, ...] | None = None) -> list[dict]:
        ls = self.gazetteer
        if tipos:
            ls = [g for g in ls if g["LOCAL_TIPO_COD"] in tipos]
        if om_cod:
            ini, fim = self.setores[om_cod]
            ls = [g for g in ls if ini <= g["km"] <= fim and abs(g["perp"]) <= 12]
        return ls

    # --- saida ------------------------------------------------------------
    def pasta(self, *partes: str) -> Path:
        p = self.landing.joinpath(*partes)
        p.mkdir(parents=True, exist_ok=True)
        return p

    def registrar_defeito(self, fonte: str, arquivo: Path | str, tipo: str, descricao: str):
        """Defeito PROPOSITAL, plantado para o pipeline ter o que detectar. Fica na verdade/."""
        self.defeitos.append({"fonte": fonte, "arquivo": str(arquivo), "tipo": tipo, "descricao": descricao})

    def n(self, base: int) -> int:
        """Volume escalado, nunca abaixo de 1."""
        return max(1, round(base * self.escala))
