"""
Gerador de Dados Sintéticos — infra-dissertacao
Gera lotes de dados GPS, SITREP e Sensor (drone) e envia para MinIO 'landing/'.

Cada batalhão tem subunidades com pools fixos de veículos e drones,
garantindo diversidade suficiente para o MERGE INTO no Silver.

Uso:
    python scripts/gerar_dados.py                          # 10 lotes × 200 registros
    python scripts/gerar_dados.py --lotes 20 --registros 500
    python scripts/gerar_dados.py --tipo gps --lotes 5
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timezone, timedelta
from io import BytesIO


from faker import Faker
from minio import Minio

fake = Faker("pt_BR")

BATALHOES = ["1BPE", "2BPE", "3BPE", "4BPE", "5BPE", "1BIB", "2BIB"]

SUBUNIDADES = {
    "1BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
    "2BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
    "3BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
    "4BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
    "5BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
    "1BIB": ["Cia Cmdo", "1a Cia Fuz Bld", "2a Cia Fuz Bld", "3a Cia Fuz Bld", "Cia Ap"],
    "2BIB": ["Cia Cmdo", "1a Cia Fuz Bld", "2a Cia Fuz Bld", "3a Cia Fuz Bld", "Cia Ap"],
}

LAT_BASE, LON_BASE = -15.77, -47.92
LAT_RANGE, LON_RANGE = 2.0, 3.0

AREAS_COBERTURA = ["NORTE", "SUL", "LESTE", "OESTE", "CENTRO"]


def _gerar_pool_veiculos():
    """25 veículos por subunidade → ~750 únicos no total."""
    pool = {}
    for bat in BATALHOES:
        veiculos = []
        for idx, sub in enumerate(SUBUNIDADES[bat]):
            for seq in range(1, 26):
                veiculos.append({
                    "id": f"VTR-{bat}-{idx + 1:02d}-{seq:02d}",
                    "sub": sub,
                })
        pool[bat] = veiculos
    return pool


def _gerar_pool_drones():
    """15 drones por batalhão → 105 únicos no total."""
    pool = {}
    for bat in BATALHOES:
        drones = []
        for seq in range(1, 16):
            drones.append({
                "id": f"DRN-{bat}-{seq:02d}",
                "area": AREAS_COBERTURA[seq % len(AREAS_COBERTURA)],
            })
        pool[bat] = drones
    return pool


POOL_VEICULOS = _gerar_pool_veiculos()
POOL_DRONES = _gerar_pool_drones()


def ts_recente(minutos_atras: int = 480) -> str:
    """Timestamp entre agora e N minutos atrás (padrão 8h = janela operacional)."""
    delta = timedelta(minutes=random.randint(0, minutos_atras))
    return (datetime.now(timezone.utc) - delta).isoformat()


def gerar_gps(n: int) -> list[dict]:
    registros = []
    for _ in range(n):
        bat = random.choice(BATALHOES)
        veiculo = random.choice(POOL_VEICULOS[bat])
        lat = LAT_BASE + random.uniform(-LAT_RANGE, LAT_RANGE)
        lon = LON_BASE + random.uniform(-LON_RANGE, LON_RANGE)
        registros.append({
            "batalhao_origem":   bat,
            "tipo_dado":         "gps",
            "timestamp_geracao": ts_recente(),
            "id_veiculo":        veiculo["id"],
            "subunidade":        veiculo["sub"],
            "latitude":          round(lat, 6),
            "longitude":         round(lon, 6),
            "altitude_m":        round(random.uniform(800, 1200), 1),
            "velocidade_kmh":    round(random.uniform(0, 120), 1),
            "direcao_graus":     random.randint(0, 359),
            "precisao_m":        round(random.uniform(3, 50), 1),
        })
    return registros




def gerar_sensor(n: int) -> list[dict]:
    """Gera dados de drones/sensores de reconhecimento aéreo."""
    status_opcoes = ["ativo", "retornando", "em_espera", "manutencao"]
    registros = []
    for _ in range(n):
        bat = random.choice(BATALHOES)
        drone = random.choice(POOL_DRONES[bat])
        lat = LAT_BASE + random.uniform(-LAT_RANGE, LAT_RANGE)
        lon = LON_BASE + random.uniform(-LON_RANGE, LON_RANGE)
        registros.append({
            "batalhao_origem":   bat,
            "tipo_dado":         "sensor",
            "timestamp_geracao": ts_recente(120),
            "id_sensor":         drone["id"],
            "drone_id":          drone["id"],
            "area_cobertura":    drone["area"],
            "latitude_centro":   round(lat, 6),
            "longitude_centro":  round(lon, 6),
            "raio_km":           round(random.uniform(1.0, 15.0), 1),
            "altitude_voo":      round(random.uniform(50, 500), 0),
            "bateria_pct":       random.randint(10, 100),
            "status_missao":     random.choice(status_opcoes),
        })
    return registros


def gerar_relt_intel(n: int) -> list[dict]:
    """Inteligência — Relatório de Inteligência (avistamento inimigo)."""
    tipos_ameaca = ["TROPA_PE", "BLINDADO", "ARTILHARIA", "FRANCO_ATIRADOR", "DRONE_INIMIGO", "VEICULO_SUSPEITO"]
    confiabilidades = ["A1", "A2", "B1", "B2", "C2", "C3"]
    fontes = ["OBSERVACAO_DIRETA", "CAPTURADO", "AGENTE", "SENSOR_DRONE", "RELATO_CIVIL"]
    registros = []
    for _ in range(n):
        bat = random.choice(BATALHOES)
        sub = random.choice(SUBUNIDADES[bat])
        lat = LAT_BASE + random.uniform(-LAT_RANGE, LAT_RANGE)
        lon = LON_BASE + random.uniform(-LON_RANGE, LON_RANGE)
        registros.append({
            "batalhao_origem":    bat,
            "tipo_dado":          "relt_intel",
            "timestamp_geracao":  ts_recente(240),
            "id_relatorio":       str(uuid.uuid4()),
            "subunidade":         sub,
            "tipo_ameaca":        random.choice(tipos_ameaca),
            "coordenada_lat":     round(lat, 6),
            "coordenada_lon":     round(lon, 6),
            "efetivo_estimado":   random.randint(5, 150),
            "confiabilidade":     random.choice(confiabilidades),
            "fonte_info":         random.choice(fontes),
            "descricao":          fake.sentence(nb_words=10),
        })
    return registros


def gerar_paf(n: int) -> list[dict]:
    """Fogos — Pedido de Apoio de Fogo."""
    tipos_missao = ["SUPORTE_IMEDIATO", "SUPORTE_GERAL", "CONTRABATERIA", "SUPRESSAO"]
    tipos_alvo = ["PESSOAL_DESCOBERTO", "VEICULO", "POSICAO_DEFENSIVA", "MATERIAL", "AREA_SUSPEITA"]
    tipos_municao = ["EXPLOSIVO", "FUMACA", "ILUMINACAO"]
    prioridades = ["URGENTE", "PRIORITARIO", "ROTINA"]
    status_opcoes = ["SOLICITADO", "APROVADO", "EXECUTADO", "CANCELADO"]
    registros = []
    for _ in range(n):
        bat = random.choice(BATALHOES)
        sub = random.choice(SUBUNIDADES[bat])
        lat = LAT_BASE + random.uniform(-LAT_RANGE, LAT_RANGE)
        lon = LON_BASE + random.uniform(-LON_RANGE, LON_RANGE)
        registros.append({
            "batalhao_origem":     bat,
            "tipo_dado":           "paf",
            "timestamp_geracao":   ts_recente(360),
            "id_paf":              str(uuid.uuid4()),
            "subunidade":          sub,
            "tipo_missao":         random.choice(tipos_missao),
            "coordenada_alvo_lat": round(lat, 6),
            "coordenada_alvo_lon": round(lon, 6),
            "tipo_alvo":           random.choice(tipos_alvo),
            "tipo_municao":        random.choice(tipos_municao),
            "prioridade":          random.choice(prioridades),
            "status_execucao":     random.choice(status_opcoes),
        })
    return registros


def gerar_obstaculo(n: int) -> list[dict]:
    """Manobra — Obstáculo identificado (complementa GPS com dados de transitabilidade)."""
    tipos = ["MINA", "BARREIRA_FISICA", "INUNDACAO", "DESTRUICAO_PONTE", "ENTULHO", "AREA_CONTAMINADA"]
    transitabilidades = ["INTRANSITAVEL", "RESTRITO", "TRANSITAVEL"]
    registros = []
    for _ in range(n):
        bat = random.choice(BATALHOES)
        sub = random.choice(SUBUNIDADES[bat])
        lat = LAT_BASE + random.uniform(-LAT_RANGE, LAT_RANGE)
        lon = LON_BASE + random.uniform(-LON_RANGE, LON_RANGE)
        registros.append({
            "batalhao_origem":       bat,
            "tipo_dado":             "obstaculo",
            "timestamp_geracao":     ts_recente(720),
            "id_obstaculo":          str(uuid.uuid4()),
            "subunidade":            sub,
            "tipo_obstaculo":        random.choice(tipos),
            "coordenada_lat":        round(lat, 6),
            "coordenada_lon":        round(lon, 6),
            "transitabilidade":      random.choice(transitabilidades),
            "coberto_fogo":          random.choice([True, False]),
            "largura_m":             round(random.uniform(10, 200), 1),
            "confirmado_engenharia": random.random() > 0.4,
        })
    return registros


def gerar_seg_area(n: int) -> list[dict]:
    """Proteção — Ocorrência de segurança de área."""
    tipos_ocorrencia = ["INFILTRACAO", "ATAQUE_SNIPER", "IED", "EMBOSCADA", "ATIVIDADE_SUSPEITA", "VIOLACAO_PERIMETRO"]
    niveis_ameaca = ["BAIXO", "MEDIO", "ALTO", "CRITICO"]
    status_opcoes = ["EM_ANDAMENTO", "RESOLVIDO", "PENDENTE"]
    registros = []
    for _ in range(n):
        bat = random.choice(BATALHOES)
        sub = random.choice(SUBUNIDADES[bat])
        lat = LAT_BASE + random.uniform(-LAT_RANGE, LAT_RANGE)
        lon = LON_BASE + random.uniform(-LON_RANGE, LON_RANGE)
        registros.append({
            "batalhao_origem":           bat,
            "tipo_dado":                 "seg_area",
            "timestamp_geracao":         ts_recente(240),
            "id_ocorrencia":             str(uuid.uuid4()),
            "subunidade":                sub,
            "tipo_ocorrencia":           random.choice(tipos_ocorrencia),
            "coordenada_lat":            round(lat, 6),
            "coordenada_lon":            round(lon, 6),
            "efetivo_proprio_envolvido": random.randint(1, 30),
            "baixas_proprias":           random.choices([0, 0, 0, 1, 2, 3], k=1)[0],
            "baixas_inimigas":           random.choices([0, 0, 0, 1, 2, 3], k=1)[0],
            "nivel_ameaca":              random.choice(niveis_ameaca),
            "status_resolucao":          random.choice(status_opcoes),
        })
    return registros


TIPOS_VIATURA = ["VBTP", "VTR_CARGA", "VTR_CMDO", "AMBULANCIA", "VTR_MANT"]


def gerar_pessoal(n: int) -> list[dict]:
    """Logística (S1) — Relatório de efetivo por subunidade."""
    situacoes = ["OPERACIONAL", "DEGRADADO", "INOPERANTE", "RESERVA"]
    necessidades_s1 = ["PESSOAL_REFORCADO", "EVACUACAO_MEDICA", "NENHUMA"]
    necessidades_logistica = [
        "MUNICAO", "COMBUSTIVEL", "RACOES", "MATERIAL SAUDE",
        "PECAS REPOSICAO", "AGUA", "BATERIAS", "NENHUMA",
    ]
    registros = []
    for _ in range(n):
        bat = random.choice(BATALHOES)
        sub = random.choice(SUBUNIDADES[bat])
        efetivo_organico = 120
        baixas_combate     = random.choices([0, 0, 0, 1, 2, 3, 5], k=1)[0]
        baixas_nao_combate = random.choices([0, 0, 1, 2, 3], k=1)[0]
        evacuados          = random.choices([0, 0, 0, 1, 2], k=1)[0]
        efetivo_presente   = max(20, efetivo_organico
                                 - baixas_combate - baixas_nao_combate
                                 - evacuados - random.randint(0, 10))
        registros.append({
            "batalhao_origem":         bat,
            "tipo_dado":               "pessoal",
            "timestamp_geracao":       ts_recente(480),
            "id_relatorio":            str(uuid.uuid4()),
            "subunidade":              sub,
            "situacao_operacional":    random.choice(situacoes),
            "efetivo_organico":        efetivo_organico,
            "efetivo_presente":        efetivo_presente,
            "baixas_combate":          baixas_combate,
            "baixas_nao_combate":      baixas_nao_combate,
            "evacuados":               evacuados,
            "necessidade_prioritaria": random.choice(necessidades_s1),
            "necessidade_logistica":   random.choice(necessidades_logistica),
        })
    return registros


def gerar_material(n: int) -> list[dict]:
    """S4 — Estado do material: por viatura individual (granularidade máxima)."""
    status_opcoes = ["OPERACIONAL", "MANUTENCAO", "BAIXADO_TECNICO", "BAIXADO_COMBATE"]
    status_pesos  = [65, 20, 10, 5]
    registros = []
    for _ in range(n):
        bat     = random.choice(BATALHOES)
        veiculo = random.choice(POOL_VEICULOS[bat])
        status  = random.choices(status_opcoes, weights=status_pesos, k=1)[0]
        fuel = (
            random.randint(5,  30) if status in ("BAIXADO_TECNICO", "BAIXADO_COMBATE")
            else random.randint(25, 100)
        )
        km_rodados           = random.randint(0, 20000)
        proxima_manutencao   = ((km_rodados // 5000) + 1) * 5000
        registros.append({
            "batalhao_origem":        bat,
            "tipo_dado":              "material",
            "timestamp_geracao":      ts_recente(60),
            "id_viatura":             veiculo["id"],
            "subunidade":             veiculo["sub"],
            "tipo_viatura":           TIPOS_VIATURA[hash(veiculo["id"]) % len(TIPOS_VIATURA)],
            "status_viatura":         status,
            "nivel_combustivel_pct":  fuel,
            "km_rodados":             km_rodados,
            "proxima_manutencao_km":  proxima_manutencao,
        })
    return registros


GERADORES = {
    "gps":        gerar_gps,
    "sensor":     gerar_sensor,
    "relt_intel": gerar_relt_intel,
    "paf":        gerar_paf,
    "obstaculo":  gerar_obstaculo,
    "seg_area":   gerar_seg_area,
    "pessoal":    gerar_pessoal,
    "material":   gerar_material,
}


def upload_lote(client: Minio, tipo: str, registros: list[dict], lote: int):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    nome = f"{tipo}/lote_{ts}_{lote:03d}.json"
    dados = json.dumps(registros, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        "landing",
        nome,
        BytesIO(dados),
        length=len(dados),
        content_type="application/json",
    )
    print(f"  -> landing/{nome} ({len(registros)} registros, {len(dados)} bytes)")


def main():
    parser = argparse.ArgumentParser(description="Gerador de dados sinteticos para infra-dissertacao")
    parser.add_argument("--tipo", choices=list(GERADORES.keys()) + ["todos"],
                        default="todos", help="Tipo de dado a gerar")
    parser.add_argument("--lotes", type=int, default=10, help="Numero de lotes por tipo")
    parser.add_argument("--registros", type=int, default=200, help="Registros por lote")
    parser.add_argument("--host", default="localhost:9000")
    parser.add_argument("--user", default="minio_admin")
    parser.add_argument("--password", default="minio_pass_2026")
    args = parser.parse_args()

    client = Minio(args.host, access_key=args.user, secret_key=args.password, secure=False)
    tipos = list(GERADORES.keys()) if args.tipo == "todos" else [args.tipo]

    print(f"\nGerando {args.lotes} lote(s) x {args.registros} registros para: {tipos}\n")
    for tipo in tipos:
        gerador = GERADORES[tipo]
        for i in range(1, args.lotes + 1):
            registros = gerador(args.registros)
            upload_lote(client, tipo, registros, i)

    total = len(tipos) * args.lotes * args.registros
    print(f"\nTotal enviado: {total} registros -> MinIO landing/")
    print("Agora dispare a DAG 'dag_ingestao_bronze' no Airflow.\n")


if __name__ == "__main__":
    main()
