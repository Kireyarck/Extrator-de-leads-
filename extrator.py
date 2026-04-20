"""
EXTRATOR V3 - VERSAO ENRIQUECIDA
================================

Fluxo principal:
1. Pergunta a origem da lista:
   1 -> Google
   2 -> Receita Federal (dados publicos via consulta de CNPJ)
   3 -> Os 2
2. Mantem o restante da logica de operacao:
   - lote a partir dos arquivos existentes
   - cidade especifica
3. Salva sempre o mesmo schema final de 18 colunas.
"""

from __future__ import annotations

import os
import platform
import re
import time
import unicodedata
from typing import Dict, List, Optional, Sequence
from urllib.parse import quote_plus

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


COLUNAS_CSV = [
    "Nome da Empresa",
    "Número de Telefone",
    "E-mail",
    "Site",
    "Endereço",
    "Avaliação",
    "Número de Reviews",
    "Categoria",
    "Status",
    "Horário de Funcionamento",
    "Faixa de Preço",
    "Número de Fotos",
    "Link Google Maps",
    "CNPJ",
    "Razão Social",
    "Nome Fantasia",
    "Situação Cadastral",
    "CNAE Principal",
]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

CONSULTAS_CNPJ_HOME = "https://www.consultascnpj.com/"
TIMEOUT_PADRAO = 30
MAX_LINKS_RECEITA = 20
PAUSA_ENTRE_CIDADES = 5

TERMOS_SAUDE_RECEITA = [
    "clinica",
    "saude",
    "odonto",
    "odontologia",
    "dental",
    "fisioterapia",
    "consultorio",
    "hospital",
    "laboratorio",
    "neuro",
]

TOKENS_SAUDE_RECEITA = [
    "clinic",
    "saude",
    "medic",
    "hospital",
    "laborator",
    "odont",
    "odonto",
    "dental",
    "fisio",
    "fisioter",
    "consult",
    "psicolog",
    "fono",
    "neuro",
    "terapi",
    "obstetr",
    "especialidade",
]

GATILHOS_SAUDE_RECEITA = {
    "clinica",
    "saude",
    "odonto",
    "odontologia",
    "dental",
    "fisioterapia",
    "consultorio",
    "hospital",
    "laboratorio",
    "neuro",
    "medicina",
    "medico",
    "psicologia",
}

ESTADOS_FALLBACK = [
    {"nome": "Acre", "sigla": "AC"},
    {"nome": "Alagoas", "sigla": "AL"},
    {"nome": "Amapá", "sigla": "AP"},
    {"nome": "Amazonas", "sigla": "AM"},
    {"nome": "Bahia", "sigla": "BA"},
    {"nome": "Ceará", "sigla": "CE"},
    {"nome": "Distrito Federal", "sigla": "DF"},
    {"nome": "Espírito Santo", "sigla": "ES"},
    {"nome": "Goiás", "sigla": "GO"},
    {"nome": "Maranhão", "sigla": "MA"},
    {"nome": "Mato Grosso", "sigla": "MT"},
    {"nome": "Mato Grosso do Sul", "sigla": "MS"},
    {"nome": "Minas Gerais", "sigla": "MG"},
    {"nome": "Pará", "sigla": "PA"},
    {"nome": "Paraíba", "sigla": "PB"},
    {"nome": "Paraná", "sigla": "PR"},
    {"nome": "Pernambuco", "sigla": "PE"},
    {"nome": "Piauí", "sigla": "PI"},
    {"nome": "Rio de Janeiro", "sigla": "RJ"},
    {"nome": "Rio Grande do Norte", "sigla": "RN"},
    {"nome": "Rio Grande do Sul", "sigla": "RS"},
    {"nome": "Rondônia", "sigla": "RO"},
    {"nome": "Roraima", "sigla": "RR"},
    {"nome": "Santa Catarina", "sigla": "SC"},
    {"nome": "São Paulo", "sigla": "SP"},
    {"nome": "Sergipe", "sigla": "SE"},
    {"nome": "Tocantins", "sigla": "TO"},
]

CIDADES_FALLBACK = {
    "AC": ["Rio Branco", "Cruzeiro do Sul", "Sena Madureira", "Tarauacá", "Feijó"],
    "AL": ["Maceió", "Arapiraca", "Palmeira dos Índios", "Rio Largo", "Penedo"],
    "AP": ["Macapá", "Santana", "Laranjal do Jari", "Oiapoque", "Mazagão"],
    "AM": ["Manaus", "Parintins", "Itacoatiara", "Manacapuru", "Coari"],
    "BA": ["Salvador", "Feira de Santana", "Vitória da Conquista", "Camaçari", "Juazeiro"],
    "CE": ["Fortaleza", "Caucaia", "Juazeiro do Norte", "Maracanaú", "Sobral"],
    "DF": ["Brasília", "Taguatinga", "Ceilândia", "Samambaia", "Planaltina"],
    "ES": ["Vitória", "Vila Velha", "Cariacica", "Serra", "Cachoeiro de Itapemirim"],
    "GO": ["Goiânia", "Aparecida de Goiânia", "Anápolis", "Rio Verde", "Luziânia"],
    "MA": ["São Luís", "Imperatriz", "São José de Ribamar", "Timon", "Caxias"],
    "MT": ["Cuiabá", "Várzea Grande", "Rondonópolis", "Sinop", "Tangará da Serra"],
    "MS": ["Campo Grande", "Dourados", "Três Lagoas", "Corumbá", "Ponta Porã"],
    "MG": ["Belo Horizonte", "Uberlândia", "Contagem", "Juiz de Fora", "Betim"],
    "PA": ["Belém", "Ananindeua", "Santarém", "Marabá", "Castanhal"],
    "PB": ["João Pessoa", "Campina Grande", "Santa Rita", "Patos", "Bayeux"],
    "PR": ["Curitiba", "Londrina", "Maringá", "Ponta Grossa", "Cascavel"],
    "PE": ["Recife", "Jaboatão dos Guararapes", "Olinda", "Caruaru", "Petrolina"],
    "PI": ["Teresina", "Parnaíba", "Picos", "Piripiri", "Floriano"],
    "RJ": ["Rio de Janeiro", "São Gonçalo", "Duque de Caxias", "Nova Iguaçu", "Niterói"],
    "RN": ["Natal", "Mossoró", "Parnamirim", "São Gonçalo do Amarante", "Macaíba"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas", "Canoas", "Santa Maria"],
    "RO": ["Porto Velho", "Ji-Paraná", "Ariquemes", "Vilhena", "Cacoal"],
    "RR": ["Boa Vista", "Rorainópolis", "Caracaraí", "Alto Alegre", "Mucajaí"],
    "SC": ["Florianópolis", "Joinville", "Blumenau", "São José", "Criciúma"],
    "SP": ["São Paulo", "Guarulhos", "Campinas", "São Bernardo do Campo", "Santo André"],
    "SE": [
        "Aracaju",
        "Nossa Senhora do Socorro",
        "Lagarto",
        "Itabaiana",
        "Estância",
        "Propriá",
        "Tobias Barreto",
        "Simão Dias",
        "Capela",
        "Gararu",
    ],
    "TO": ["Palmas", "Araguaína", "Gurupi", "Porto Nacional", "Paraíso do Tocantins"],
}


def criar_sessao_http() -> requests.Session:
    sessao = requests.Session()
    sessao.headers.update(REQUEST_HEADERS)
    return sessao


def normalizar_texto(valor: str) -> str:
    if not valor:
        return ""
    texto = unicodedata.normalize("NFKD", valor)
    texto = "".join(char for char in texto if not unicodedata.combining(char))
    texto = texto.lower()
    texto = re.sub(r"[^a-z0-9\s]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def normalizar_rotulo(rotulo: str) -> str:
    return normalizar_texto(rotulo).replace(" ", "")


def apenas_digitos(valor: str) -> str:
    return re.sub(r"\D", "", valor or "")


def deduplicar_preservando_ordem(valores: Sequence[str]) -> List[str]:
    itens = []
    vistos = set()
    for valor in valores:
        valor_limpo = (valor or "").strip()
        if not valor_limpo or valor_limpo in vistos:
            continue
        vistos.add(valor_limpo)
        itens.append(valor_limpo)
    return itens


def formatar_cnpj(cnpj: str) -> str:
    digitos = apenas_digitos(cnpj)
    if len(digitos) != 14:
        return digitos
    return (
        f"{digitos[0:2]}.{digitos[2:5]}.{digitos[5:8]}/"
        f"{digitos[8:12]}-{digitos[12:14]}"
    )


def montar_endereco(logradouro: str, numero: str, complemento: str, bairro: str, cidade: str, uf: str, cep: str) -> str:
    partes = []
    primeira_linha = " ".join(parte for parte in [logradouro, numero] if parte).strip()
    if primeira_linha:
        partes.append(primeira_linha)
    if complemento:
        partes.append(complemento)
    if bairro:
        partes.append(bairro)
    cidade_uf = " - ".join(parte for parte in [cidade, uf] if parte).strip()
    if cidade_uf:
        partes.append(cidade_uf)
    if cep:
        partes.append(cep)
    return ", ".join(parte for parte in partes if parte)


def criar_linha_vazia() -> Dict[str, str]:
    return {coluna: "" for coluna in COLUNAS_CSV}


def normalizar_linha_saida(linha: Dict[str, object]) -> Dict[str, str]:
    base = criar_linha_vazia()
    for coluna in COLUNAS_CSV:
        valor = linha.get(coluna, "") if linha else ""
        base[coluna] = "" if valor is None else str(valor).strip()
    return base


def combinar_linhas(base: Dict[str, str], complemento: Dict[str, str]) -> Dict[str, str]:
    resultado = normalizar_linha_saida(base)
    for coluna in COLUNAS_CSV:
        if not resultado[coluna] and complemento.get(coluna):
            resultado[coluna] = complemento[coluna]
    return resultado


def get_estados() -> List[Dict[str, str]]:
    sessao = criar_sessao_http()
    try:
        response = sessao.get("https://brasilapi.com.br/api/ibge/uf/v1", timeout=TIMEOUT_PADRAO)
        response.raise_for_status()
        if response.text.strip().startswith("["):
            data = response.json()
            estados = [{"nome": item["nome"], "sigla": item["sigla"]} for item in data]
            return sorted(estados, key=lambda item: item["nome"])
    except Exception as exc:
        print(f"Brasil API falhou: {exc}")

    print("Tentando API do IBGE...")
    for tentativa in range(3):
        try:
            response = sessao.get(
                "https://servicodados.ibge.gov.br/api/v1/localidades/estados?orderBy=nome",
                timeout=TIMEOUT_PADRAO,
            )
            response.raise_for_status()
            if response.text.strip().startswith("["):
                data = response.json()
                return [{"nome": item["nome"], "sigla": item["sigla"]} for item in data]
        except Exception:
            if tentativa < 2:
                time.sleep(2)
    print("Usando lista de estados fallback (offline).")
    return ESTADOS_FALLBACK


def get_cidades(estado_sigla: str) -> List[str]:
    sessao = criar_sessao_http()
    sigla = (estado_sigla or "").upper().strip()
    try:
        response = sessao.get(
            f"https://brasilapi.com.br/api/ibge/municipios/v1/{sigla}",
            timeout=TIMEOUT_PADRAO,
        )
        response.raise_for_status()
        if response.text.strip().startswith("["):
            data = response.json()
            return [cidade["nome"] for cidade in data]
    except Exception as exc:
        print(f"Brasil API falhou para cidades: {exc}")

    print(f"Tentando IBGE para cidades de {sigla}...")
    for tentativa in range(3):
        try:
            response = sessao.get(
                f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{sigla}/municipios",
                timeout=TIMEOUT_PADRAO,
            )
            response.raise_for_status()
            if response.text.strip().startswith("["):
                data = response.json()
                return [cidade["nome"] for cidade in data]
        except Exception:
            if tentativa < 2:
                time.sleep(2)
    print(f"Usando lista de cidades fallback para {sigla}.")
    return CIDADES_FALLBACK.get(sigla, [])


def print_in_grid(options_list: Sequence[object], num_columns: int = 4) -> None:
    max_len = 0
    formatted_options: List[str] = []
    for i, item in enumerate(options_list):
        if isinstance(item, dict):
            display_name = f"{item['nome']} ({item['sigla']})"
        else:
            display_name = str(item)
        formatted_item = f"{i + 1}. {display_name}"
        formatted_options.append(formatted_item)
        max_len = max(max_len, len(formatted_item))

    for i, formatted_item in enumerate(formatted_options):
        print(f"{formatted_item:<{max_len + 4}}", end="")
        if (i + 1) % num_columns == 0 or (i + 1) == len(formatted_options):
            print()


def selecionar_ou_digitar(prompt_principal: str, lista_opcoes: Sequence[object], tipo_opcao: str = "estado"):
    while True:
        print(f"\n--- {prompt_principal} ---")
        print_in_grid(lista_opcoes)
        mapa_opcoes: Dict[str, object] = {}
        if tipo_opcao == "estado":
            for estado in lista_opcoes:
                mapa_opcoes[normalizar_texto(estado["nome"])] = estado
                mapa_opcoes[normalizar_texto(estado["sigla"])] = estado
        else:
            for cidade in lista_opcoes:
                mapa_opcoes[normalizar_texto(str(cidade))] = cidade

        user_input = input("\nDigite o NUMERO da opcao desejada ou o nome completo: ").strip()
        try:
            indice = int(user_input)
            if 1 <= indice <= len(lista_opcoes):
                return lista_opcoes[indice - 1]
            print("ERRO: Numero fora do intervalo da lista. Tente novamente.")
        except ValueError:
            opcao = mapa_opcoes.get(normalizar_texto(user_input))
            if opcao:
                return opcao
            print(f"ERRO: Opcao '{user_input}' nao encontrada. Verifique a digitacao.")
        time.sleep(1)


def selecionar_origem_lista() -> str:
    while True:
        print("\n--- ORIGEM DA LISTA ---")
        print("1. Google")
        print("2. Receita Federal")
        print("3. Os 2")
        origem = input("\nEscolha uma opcao (1, 2 ou 3): ").strip()
        if origem in {"1", "2", "3"}:
            return origem
        print("Opcao invalida. Tente novamente.")


def resolver_estado_por_nome_ou_sigla(estado_digitado: str, estados: Sequence[Dict[str, str]]) -> Optional[Dict[str, str]]:
    chave = normalizar_texto(estado_digitado)
    for estado in estados:
        if chave in {normalizar_texto(estado["nome"]), normalizar_texto(estado["sigla"])}:
            return estado
    return None


def sanitize_filename(name: str) -> str:
    texto = normalizar_texto(name).replace(" ", "_")
    return texto or "arquivo"


def remover_sufixo_contador(nome_arquivo: str) -> str:
    nome_base, _ = os.path.splitext(nome_arquivo)
    return re.sub(r"_\(\d+\)$", "", nome_base)


def obter_rotulo_fonte_saida(origem: str) -> str:
    return {
        "1": "google",
        "2": "receita",
        "3": "google_receita",
    }.get(origem, "desconhecida")


def get_unique_filename(filename: str) -> str:
    if not os.path.exists(filename):
        return filename
    name_part, extension = os.path.splitext(filename)
    counter = 1
    while True:
        candidate = f"{name_part}_({counter}){extension}"
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def extrair_cidade_do_nome_arquivo(nome_arquivo: str) -> str:
    nome = remover_sufixo_contador(nome_arquivo)
    nome = nome.replace("leads_", "")
    for prefixo in ("google_receita_", "google_", "receita_"):
        if nome.startswith(prefixo):
            nome = nome[len(prefixo):]
            break
    partes = nome.split("_")
    if len(partes) <= 1:
        return nome.replace("_", " ").title().strip()
    cidade = "_".join(partes[1:])
    return cidade.replace("_", " ").title().strip()


def resolver_cidade_pelo_nome_arquivo(nome_arquivo: str, cidades_do_estado: Sequence[str]) -> str:
    stem = nome_arquivo.replace("leads_", "").replace(".csv", "")
    stem_normalizado = normalizar_texto(stem).replace(" ", "_")
    melhor_cidade = ""
    melhor_tamanho = -1

    for cidade in cidades_do_estado:
        cidade_normalizada = normalizar_texto(cidade).replace(" ", "_")
        if stem_normalizado.endswith(cidade_normalizada) and len(cidade_normalizada) > melhor_tamanho:
            melhor_cidade = cidade
            melhor_tamanho = len(cidade_normalizada)

    return melhor_cidade or extrair_cidade_do_nome_arquivo(nome_arquivo)


def carregar_cidades_dos_arquivos_existentes(diretorio: str = ".") -> List[Dict[str, str]]:
    arquivos_csv = []
    for arquivo in os.listdir(diretorio):
        if arquivo.startswith("leads_") and arquivo.endswith(".csv"):
            arquivos_csv.append(
                {
                    "arquivo": arquivo,
                    "cidade": extrair_cidade_do_nome_arquivo(arquivo),
                    "caminho": os.path.join(diretorio, arquivo),
                }
            )
    return sorted(arquivos_csv, key=lambda item: item["cidade"])


def encontrar_csv_google_local(nicho: str, cidade: str, diretorio: str = ".") -> Optional[str]:
    nicho_slug = sanitize_filename(nicho)
    cidade_slug = sanitize_filename(cidade)
    bases_prioritarias = [
        (f"leads_google_{nicho_slug}_{cidade_slug}", 0),
        (f"leads_google_receita_{nicho_slug}_{cidade_slug}", 1),
        (f"leads_{nicho_slug}_{cidade_slug}", 2),
    ]

    candidatos = []
    for arquivo in os.listdir(diretorio):
        if not arquivo.startswith("leads_") or not arquivo.endswith(".csv"):
            continue

        stem = remover_sufixo_contador(arquivo)
        for base_esperada, prioridade in bases_prioritarias:
            if stem != base_esperada:
                continue

            caminho = os.path.join(diretorio, arquivo)
            try:
                df = pd.read_csv(caminho, encoding="utf-8-sig")
            except Exception:
                continue

            if "Nome da Empresa" not in df.columns or "Link Google Maps" not in df.columns:
                continue

            links_google = df["Link Google Maps"].fillna("").astype(str).str.strip()
            quantidade_links = int((links_google != "").sum())
            if quantidade_links <= 0:
                continue

            candidatos.append(
                {
                    "caminho": caminho,
                    "arquivo": arquivo,
                    "prioridade": prioridade,
                    "quantidade_links": quantidade_links,
                    "mtime": os.path.getmtime(caminho),
                }
            )
            break

    if not candidatos:
        return None

    candidatos.sort(
        key=lambda item: (
            item["prioridade"],
            -item["quantidade_links"],
            -item["mtime"],
        )
    )
    return candidatos[0]["caminho"]


def carregar_nomes_do_csv_google_local(nicho: str, cidade: str, diretorio: str = ".") -> List[str]:
    caminho_csv = encontrar_csv_google_local(nicho, cidade, diretorio=diretorio)
    if not caminho_csv:
        return []

    try:
        df = pd.read_csv(caminho_csv, encoding="utf-8-sig")
    except Exception as exc:
        print(f"Falha ao ler CSV Google local para apoio da Receita: {exc}")
        return []

    nomes = deduplicar_preservando_ordem(
        [
            str(nome).strip()
            for nome in df.get("Nome da Empresa", [])
            if str(nome).strip() and str(nome).strip().lower() != "nan"
        ]
    )
    if nomes:
        print(f"Usando {len(nomes)} nome(s) de apoio do CSV Google local: {os.path.basename(caminho_csv)}")
    return nomes


def criar_driver_chrome(headless: bool = False) -> webdriver.Chrome:
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")
    options.add_argument("--lang=pt-BR")
    options.add_experimental_option("prefs", {"intl.accept_languages": "pt-BR,pt,en-US,en"})
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    if headless or "microsoft" in platform.release().lower() or os.environ.get("WSL_DISTRO_NAME"):
        options.add_argument("--headless=new")
    os.environ["WDM_LOG_LEVEL"] = "0"
    return webdriver.Chrome(service=service, options=options)


def resumir_excecao(exc: Exception) -> str:
    linhas = [linha.strip() for linha in str(exc).splitlines() if linha.strip() and linha.strip() != "Stacktrace:"]
    if linhas:
        if linhas[0].lower() == "message:" and len(linhas) > 1:
            return linhas[1]
        return linhas[0]
    return exc.__class__.__name__


def obter_contexto_driver(driver: webdriver.Chrome) -> str:
    url = ""
    titulo = ""
    try:
        url = driver.current_url
    except Exception:
        pass
    try:
        titulo = driver.title
    except Exception:
        pass

    partes = []
    if url:
        partes.append(f"URL: {url}")
    if titulo:
        partes.append(f"Titulo: {titulo}")
    return " | ".join(partes) if partes else "URL e titulo indisponiveis"


def clicar_elemento_com_fallback(driver: webdriver.Chrome, elemento) -> bool:
    try:
        elemento.click()
        return True
    except (ElementClickInterceptedException, WebDriverException):
        try:
            driver.execute_script("arguments[0].click();", elemento)
            return True
        except WebDriverException:
            return False


def clicar_consentimento_google_no_contexto(driver: webdriver.Chrome) -> bool:
    rotulos_alvo = {
        "aceitar",
        "aceitar tudo",
        "aceito",
        "concordo",
        "eu concordo",
        "accept",
        "accept all",
        "i agree",
    }

    candidatos = driver.find_elements(By.XPATH, "//button|//*[@role='button']|//input[@type='button' or @type='submit']")
    for elemento in candidatos[:60]:
        try:
            rotulo = " ".join(
                parte
                for parte in [
                    elemento.text or "",
                    elemento.get_attribute("aria-label") or "",
                    elemento.get_attribute("value") or "",
                ]
                if parte
            )
        except WebDriverException:
            continue

        rotulo_normalizado = normalizar_texto(rotulo)
        if not rotulo_normalizado:
            continue
        if rotulo_normalizado in rotulos_alvo or any(
            rotulo_normalizado.startswith(f"{rotulo} ") for rotulo in rotulos_alvo
        ):
            if clicar_elemento_com_fallback(driver, elemento):
                return True
    return False


def tentar_aceitar_consentimento_google(driver: webdriver.Chrome) -> bool:
    for _ in range(3):
        try:
            driver.switch_to.default_content()
        except Exception:
            pass

        if clicar_consentimento_google_no_contexto(driver):
            time.sleep(2)
            return True

        for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
            try:
                driver.switch_to.default_content()
                driver.switch_to.frame(iframe)
                if clicar_consentimento_google_no_contexto(driver):
                    time.sleep(2)
                    driver.switch_to.default_content()
                    return True
            except WebDriverException:
                continue
            finally:
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass
        time.sleep(1)
    return False


def identificar_estado_google_maps(driver: webdriver.Chrome) -> Optional[str]:
    try:
        if driver.find_elements(By.XPATH, '//a[contains(@class, "hfpxzc")]'):
            return "feed"
        if driver.find_elements(By.XPATH, '//div[@role="feed"]'):
            return "feed"
        if driver.find_elements(By.XPATH, "//h1"):
            return "detail"
        corpo = normalizar_texto(driver.find_element(By.TAG_NAME, "body").text)
    except Exception:
        return None

    gatilhos_sem_resultado = [
        "nenhum resultado",
        "nao encontramos resultados",
        "nao retornou nenhum resultado",
        "no results found",
        "did not match any results",
    ]
    if any(gatilho in corpo for gatilho in gatilhos_sem_resultado):
        return "no_results"
    return None


def aguardar_estado_google_maps(driver: webdriver.Chrome, timeout: int = 25) -> Optional[str]:
    try:
        return WebDriverWait(driver, timeout).until(lambda navegador: identificar_estado_google_maps(navegador))
    except TimeoutException:
        tentar_aceitar_consentimento_google(driver)
        return identificar_estado_google_maps(driver)


def abrir_google_maps_por_url_direta(driver: webdriver.Chrome, query: str) -> Optional[str]:
    driver.get(f"https://www.google.com/maps/search/?api=1&query={quote_plus(query)}")
    time.sleep(2)
    tentar_aceitar_consentimento_google(driver)
    return aguardar_estado_google_maps(driver, timeout=25)


def pesquisar_google_maps_via_caixa(driver: webdriver.Chrome, query: str) -> Optional[str]:
    driver.get("https://www.google.com/maps")
    time.sleep(2)
    tentar_aceitar_consentimento_google(driver)

    try:
        search_box = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "searchboxinput")))
    except TimeoutException:
        return aguardar_estado_google_maps(driver, timeout=10)

    search_box.click()
    search_box.send_keys(Keys.CONTROL, "a")
    search_box.send_keys(Keys.DELETE)
    search_box.send_keys(query)
    search_box.send_keys(Keys.ENTER)
    time.sleep(2)
    tentar_aceitar_consentimento_google(driver)
    return aguardar_estado_google_maps(driver, timeout=25)


def abrir_resultados_google_maps(driver: webdriver.Chrome, query: str) -> Optional[str]:
    estado = abrir_google_maps_por_url_direta(driver, query)
    if estado in {"feed", "detail", "no_results"}:
        return estado
    return pesquisar_google_maps_via_caixa(driver, query)


def iniciar_busca_google_maps(query: str, headless: bool = False) -> tuple[Optional[webdriver.Chrome], Optional[str]]:
    ultimo_erro = ""

    for tentativa in range(2):
        try:
            driver = criar_driver_chrome(headless=headless)
        except Exception as exc:
            print(f"Erro ao iniciar o Chrome: {exc}")
            print("Verifique se o Google Chrome esta instalado e atualizado.")
            return None, None

        try:
            estado = abrir_resultados_google_maps(driver, query)
            if estado in {"feed", "detail", "no_results"}:
                return driver, estado
            ultimo_erro = (
                f"Google Maps nao estabilizou a busca na tentativa {tentativa + 1}/2. "
                f"{obter_contexto_driver(driver)}"
            )
        except Exception as exc:
            ultimo_erro = (
                f"Falha ao abrir o Google Maps na tentativa {tentativa + 1}/2 "
                f"({exc.__class__.__name__}: {resumir_excecao(exc)}). "
                f"{obter_contexto_driver(driver)}"
            )

        try:
            driver.quit()
        except Exception:
            pass

        if tentativa == 0:
            print("Falha na navegacao inicial do Google Maps. Reiniciando o navegador e tentando novamente...")
            time.sleep(2)

    if ultimo_erro:
        print(ultimo_erro)
    return None, None


def encontrar_painel_resultados_google_maps(driver: webdriver.Chrome):
    seletores = [
        (By.XPATH, '//div[contains(@aria-label, "Resultados")]/..//div[@role="feed"]'),
        (By.XPATH, '//div[contains(@aria-label, "Results")]/..//div[@role="feed"]'),
        (By.XPATH, '//div[@role="feed"]'),
    ]
    for by, seletor in seletores:
        for elemento in driver.find_elements(by, seletor):
            try:
                if elemento.is_displayed():
                    return elemento
            except WebDriverException:
                continue
    return None


def rolar_painel_resultados_google_maps(driver: webdriver.Chrome, scrollable_div) -> None:
    last_height = 0
    while True:
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_div)
        time.sleep(3)
        if driver.find_elements(By.CLASS_NAME, "HlvSq"):
            break
        new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
        if new_height == last_height:
            break
        last_height = new_height


def coletar_empresas_resultados_google_maps(driver: webdriver.Chrome) -> List[Dict[str, str]]:
    empresas = []
    vistos = set()
    for result in driver.find_elements(By.XPATH, '//a[contains(@class, "hfpxzc")]'):
        try:
            nome = (result.get_attribute("aria-label") or "").strip()
            url = (result.get_attribute("href") or "").strip()
        except StaleElementReferenceException:
            continue

        if not nome or not url or url in vistos:
            continue
        vistos.add(url)
        empresas.append({"nome": nome, "url": url})
    return empresas


def construir_empresa_do_detalhe_google_maps(driver: webdriver.Chrome) -> Optional[Dict[str, str]]:
    try:
        nome = driver.find_element(By.XPATH, "//h1").text.strip()
        url = driver.current_url
    except Exception:
        return None

    if not nome or not url:
        return None
    return {"nome": nome, "url": url}


def extrair_email_do_site(website_url: str, driver: webdriver.Chrome) -> str:
    if not website_url or website_url == "Não informado":
        return ""
    if not website_url.startswith(("http://", "https://")):
        website_url = "https://" + website_url

    janela_original = driver.current_window_handle
    try:
        driver.execute_script(f"window.open('{website_url}', '_blank');")
        time.sleep(3)
        driver.switch_to.window(driver.window_handles[-1])
        body_text = driver.find_element(By.TAG_NAME, "body").text
        emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", body_text)
        return emails[0].strip() if emails else ""
    except Exception:
        return ""
    finally:
        try:
            if len(driver.window_handles) > 1:
                driver.close()
            driver.switch_to.window(janela_original)
        except Exception:
            pass


def scrape_and_filter_maps(estado_nome: str, cidade: str, nicho_pesquisado: str) -> List[Dict[str, str]]:
    """Realiza o scraping do Google Maps e retorna linhas no schema final."""
    leads: List[Dict[str, str]] = []
    query = f"{nicho_pesquisado} em {cidade}, {estado_nome}"

    print(f"\n--- Iniciando a busca no Google por: '{query}' ---")
    print("Isso pode levar alguns minutos. Aguarde...")

    driver, estado_inicial = iniciar_busca_google_maps(query, headless=False)
    if not driver:
        return []

    try:
        if estado_inicial == "no_results":
            print("Nenhum resultado utilizavel foi encontrado no Google Maps para essa busca.")
            try:
                driver.quit()
            except Exception:
                pass
            return []

        time.sleep(2)
        scrollable_div = encontrar_painel_resultados_google_maps(driver)
        if scrollable_div is not None:
            rolar_painel_resultados_google_maps(driver, scrollable_div)

        empresas_para_visitar = coletar_empresas_resultados_google_maps(driver)
        if not empresas_para_visitar and identificar_estado_google_maps(driver) == "detail":
            empresa_detalhe = construir_empresa_do_detalhe_google_maps(driver)
            if empresa_detalhe:
                empresas_para_visitar = [empresa_detalhe]

        if not empresas_para_visitar:
            estado_atual = identificar_estado_google_maps(driver)
            if estado_atual == "no_results":
                print("Nenhuma empresa encontrada no Google Maps para essa busca.")
            else:
                print(f"Nao foi possivel localizar resultados utilizaveis no Google Maps. {obter_contexto_driver(driver)}")
            try:
                driver.quit()
            except Exception:
                pass
            return []
    except Exception as exc:
        print(
            f"Falha ao preparar os resultados do Google Maps "
            f"({exc.__class__.__name__}: {resumir_excecao(exc)}). {obter_contexto_driver(driver)}"
        )
        try:
            driver.quit()
        except Exception:
            pass
        return []

    print(f"Coleta inicial concluida. {len(empresas_para_visitar)} empresas encontradas.")

    for indice, empresa in enumerate(empresas_para_visitar, start=1):
        empresa_nome = empresa["nome"]
        empresa_url = empresa["url"]
        print(f"\nAnalisando ({indice}/{len(empresas_para_visitar)}): {empresa_nome}")

        try:
            driver.get(empresa_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h1")))
            time.sleep(2)
        except Exception:
            print("-> Erro ao carregar a pagina da empresa. Pulando.")
            continue

        telefone = ""
        try:
            telefone_element = driver.find_element(By.XPATH, '//button[contains(@data-item-id, "phone:tel:")]')
            telefone = telefone_element.get_attribute("aria-label").replace("Telefone: ", "").strip()
        except NoSuchElementException:
            pass

        if not telefone:
            print("-> Descartado: nao possui numero de telefone.")
            continue

        website_url = ""
        try:
            website_element = driver.find_element(By.XPATH, '//a[@data-item-id="authority"]')
            website_url = website_element.get_attribute("href")
        except NoSuchElementException:
            pass

        endereco = ""
        try:
            endereco_element = driver.find_element(By.XPATH, '//button[@data-item-id="address"]')
            endereco = endereco_element.get_attribute("aria-label").replace("Endereço: ", "").strip()
        except NoSuchElementException:
            pass

        avaliacao = ""
        try:
            rating_element = driver.find_element(
                By.XPATH,
                '//div[contains(@aria-label, "estrelas") or contains(@aria-label, "stars")]',
            )
            aria_label = rating_element.get_attribute("aria-label")
            match = re.search(r"([\d,\.]+)", aria_label or "")
            if match:
                avaliacao = match.group(1)
        except NoSuchElementException:
            pass

        num_reviews = ""
        try:
            reviews_element = driver.find_element(
                By.XPATH,
                '//span[contains(text(), "avaliações") or contains(text(), "review")]',
            )
            match = re.search(r"([\d\.]+(?:\s*[KkMm])?)", reviews_element.text)
            if match:
                num_reviews = match.group(1).strip()
        except NoSuchElementException:
            pass

        categoria = ""
        try:
            categoria_element = driver.find_element(
                By.XPATH,
                '//div[contains(@class, "DkEaL") or contains(@class, "llr4J")]',
            )
            categoria = categoria_element.text.strip()
        except NoSuchElementException:
            pass

        status = ""
        horario_funcionamento = ""
        try:
            hours_button = driver.find_element(By.XPATH, '//button[@data-item-id="hours"]')
            hours_text = (hours_button.get_attribute("aria-label") or hours_button.text or "").strip()
            hours_text_lower = hours_text.lower()
            if "aberto" in hours_text_lower or "open" in hours_text_lower:
                status = "Aberto"
            elif "fechado" in hours_text_lower or "closed" in hours_text_lower:
                status = "Fechado"
            horario_funcionamento = hours_text
        except NoSuchElementException:
            pass

        faixa_preco = ""
        try:
            price_element = driver.find_element(
                By.XPATH,
                '//span[contains(@class, "price") or contains(@class, "F7nice")]',
            )
            faixa_preco = price_element.text.strip()
        except NoSuchElementException:
            pass

        num_fotos = ""
        try:
            photos_button = driver.find_element(By.XPATH, '//button[contains(@data-item-id, "photos")]')
            photos_text = photos_button.get_attribute("aria-label") or photos_button.text
            match = re.search(r"([\d\.]+(?:\s*[KkMm])?)", photos_text or "")
            if match:
                num_fotos = match.group(1).strip()
        except NoSuchElementException:
            pass

        email = ""
        if website_url:
            print("   -> Extraindo e-mail do site...")
            email = extrair_email_do_site(website_url, driver)

        leads.append(
            normalizar_linha_saida(
                {
                    "Nome da Empresa": empresa_nome,
                    "Número de Telefone": telefone,
                    "E-mail": email,
                    "Site": website_url,
                    "Endereço": endereco,
                    "Avaliação": avaliacao,
                    "Número de Reviews": num_reviews,
                    "Categoria": categoria,
                    "Status": status,
                    "Horário de Funcionamento": horario_funcionamento,
                    "Faixa de Preço": faixa_preco,
                    "Número de Fotos": num_fotos,
                    "Link Google Maps": empresa_url,
                }
            )
        )

    driver.quit()
    print(f"\n--- Extração Google finalizada. {len(leads)} empresas com telefone encontradas. ---")
    return leads


def decode_cloudflare_email(hex_string: str) -> str:
    try:
        key = int(hex_string[:2], 16)
        chars = [chr(int(hex_string[index:index + 2], 16) ^ key) for index in range(2, len(hex_string), 2)]
        return "".join(chars)
    except Exception:
        return ""


def extrair_localidade_de_texto(texto: str) -> tuple[str, str]:
    if not texto:
        return "", ""

    texto_compacto = re.sub(r"\s+", " ", texto).strip()
    padroes = [
        r"localizada em\s+([A-Za-zÀ-ÿ\s]+?)\s*[,|-]\s*([A-Z]{2})(?=[,\s]|$)",
        r"\bem\s+([A-Za-zÀ-ÿ\s]+?)\s*-\s*([A-Z]{2})(?=[,\s]|$)",
        r"\bem\s+([A-Za-zÀ-ÿ\s]+?)\s*,\s*([A-Z]{2})(?=[,\s]|$)",
    ]

    for padrao in padroes:
        match = re.search(padrao, texto_compacto, flags=re.IGNORECASE)
        if match:
            cidade = match.group(1).strip(" ,.-")
            uf = match.group(2).upper().strip()
            return cidade, uf
    return "", ""


def snippet_diverge_da_localidade(texto: str, cidade_esperada: str, uf_esperada: str) -> bool:
    cidade_encontrada, uf_encontrada = extrair_localidade_de_texto(texto)
    if not cidade_encontrada or not uf_encontrada:
        return False

    cidade_normalizada = normalizar_texto(cidade_encontrada)
    uf_normalizada = normalizar_texto(uf_encontrada)
    return cidade_normalizada != normalizar_texto(cidade_esperada) or uf_normalizada != normalizar_texto(uf_esperada)


def construir_estrategia_busca_receita(nicho: str) -> Dict[str, List[str]]:
    nicho_normalizado = normalizar_texto(nicho)
    tokens_base = [token for token in nicho_normalizado.split() if len(token) >= 3]

    if any(token in GATILHOS_SAUDE_RECEITA for token in tokens_base):
        buscas = deduplicar_preservando_ordem([nicho_normalizado, *TERMOS_SAUDE_RECEITA])
        tokens = deduplicar_preservando_ordem([*tokens_base, *TOKENS_SAUDE_RECEITA])
        return {"buscas": buscas, "tokens": tokens}

    buscas = deduplicar_preservando_ordem([nicho_normalizado])
    return {"buscas": buscas, "tokens": tokens_base}


def detalhe_compativel_com_nicho_receita(detalhe: Dict[str, str], tokens_nicho: Sequence[str]) -> bool:
    tokens_validos = [token for token in tokens_nicho if len(token) >= 3]
    if not tokens_validos:
        return True

    campos = " ".join(
        [
            detalhe.get("Nome Fantasia", ""),
            detalhe.get("Razão Social", ""),
            detalhe.get("Categoria", ""),
            detalhe.get("CNAE Principal", ""),
        ]
    )
    campos_normalizados = normalizar_texto(campos)
    return any(token in campos_normalizados for token in tokens_validos)


def extrair_valor_por_rotulo(rotulos: Dict[str, str], *labels: str) -> str:
    for label in labels:
        valor = rotulos.get(normalizar_rotulo(label))
        if valor:
            return valor.strip()
    return ""


def parsear_cnae_principal(soup: BeautifulSoup) -> Dict[str, str]:
    for heading in soup.find_all(["h1", "h2", "h3"]):
        if normalizar_texto(heading.get_text(" ", strip=True)) == "atividade principal":
            ul = heading.find_next("ul")
            if not ul:
                return {"codigo": "", "descricao": "", "valor": ""}
            li = ul.find("li")
            if not li:
                return {"codigo": "", "descricao": "", "valor": ""}
            fortes = [strong.get_text(" ", strip=True) for strong in li.find_all("strong")]
            if len(fortes) >= 2:
                codigo = re.sub(r"\s+", "", fortes[0])
                descricao = fortes[1].strip()
                valor = f"{codigo} - {descricao}".strip(" -")
                return {"codigo": codigo, "descricao": descricao, "valor": valor}
            texto = li.get_text(" ", strip=True)
            return {"codigo": "", "descricao": texto, "valor": texto}
    return {"codigo": "", "descricao": "", "valor": ""}


def parsear_detalhe_consultascnpj(html: str, url: str) -> Optional[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rotulos: Dict[str, str] = {}

    for item in soup.find_all("li"):
        texto = " ".join(item.stripped_strings)
        if ":" not in texto:
            continue
        label, valor = texto.split(":", 1)
        chave = normalizar_rotulo(label)
        if chave and chave not in rotulos:
            rotulos[chave] = valor.strip()

    cnpj_url = extrair_cnpj_do_link(url)
    cf_email = soup.select_one("span.__cf_email__")
    email = decode_cloudflare_email(cf_email.get("data-cfemail", "")) if cf_email else ""
    cnae_principal = parsear_cnae_principal(soup)

    logradouro = extrair_valor_por_rotulo(rotulos, "logradouro")
    numero = extrair_valor_por_rotulo(rotulos, "numero")
    complemento = extrair_valor_por_rotulo(rotulos, "complemento")
    bairro = extrair_valor_por_rotulo(rotulos, "bairro")
    cidade = extrair_valor_por_rotulo(rotulos, "município", "municipio")
    uf = extrair_valor_por_rotulo(rotulos, "uf")
    cep = extrair_valor_por_rotulo(rotulos, "cep")

    if not cidade or not uf:
        meta_description = ""
        meta_tag = soup.find("meta", attrs={"name": "description"})
        if meta_tag:
            meta_description = meta_tag.get("content", "")
        cidade_texto, uf_texto = extrair_localidade_de_texto(meta_description)
        if not cidade_texto or not uf_texto:
            cidade_texto, uf_texto = extrair_localidade_de_texto(soup.get_text(" ", strip=True))
        cidade = cidade or cidade_texto
        uf = uf or uf_texto

    return {
        "CNPJ": formatar_cnpj(extrair_valor_por_rotulo(rotulos, "número do cnpj", "numero do cnpj") or cnpj_url),
        "Razão Social": extrair_valor_por_rotulo(rotulos, "razão social", "razao social"),
        "Nome Fantasia": extrair_valor_por_rotulo(rotulos, "nome fantasia"),
        "Situação Cadastral": extrair_valor_por_rotulo(rotulos, "situação", "situacao"),
        "CNAE Principal": cnae_principal["valor"],
        "Categoria": cnae_principal["descricao"],
        "Número de Telefone": extrair_valor_por_rotulo(rotulos, "telefone"),
        "E-mail": email or extrair_valor_por_rotulo(rotulos, "e-mail", "email"),
        "Endereço": montar_endereco(logradouro, numero, complemento, bairro, cidade, uf, cep),
        "_cidade_receita": cidade,
        "_uf_receita": uf,
        "_url_fonte_receita": url,
    }


def extrair_cnpj_do_link(url: str) -> str:
    match = re.search(r"/(\d{14})(?:/)?$", url or "")
    return match.group(1) if match else ""


def buscar_links_receita_por_nome_cidade(
    query: str,
    cidade_esperada: str,
    uf_esperada: str,
    limite: int = MAX_LINKS_RECEITA,
    driver: Optional[webdriver.Chrome] = None,
) -> List[str]:
    print(f"\nBuscando candidatos da Receita para: {query}")
    driver_local = driver
    driver_criado_no_fluxo = False
    if driver_local is None:
        try:
            driver_local = criar_driver_chrome(headless=True)
            driver_criado_no_fluxo = True
        except Exception as exc:
            print(f"Erro ao iniciar o navegador para busca da Receita: {exc}")
            return []

    urls: List[str] = []
    vistos = set()

    try:
        driver_local.get(CONSULTAS_CNPJ_HOME)
        caixa = WebDriverWait(driver_local, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input.gsc-input")))
        caixa.clear()
        caixa.send_keys(query)
        caixa.send_keys(Keys.ENTER)

        WebDriverWait(driver_local, 20).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "a.gs-title[href]") or d.find_elements(By.CSS_SELECTOR, ".gs-no-results-result")
        )
        time.sleep(2)

        resultados = driver_local.find_elements(By.CSS_SELECTOR, ".gsc-webResult.gsc-result, .gs-webResult.gs-result")
        for resultado in resultados:
            texto_resultado = resultado.text.strip()
            if snippet_diverge_da_localidade(texto_resultado, cidade_esperada, uf_esperada):
                print("Descartado no snippet da busca por cidade/UF divergentes.")
                continue

            href = ""
            for link in resultado.find_elements(By.CSS_SELECTOR, "a.gs-title[href]"):
                href_candidato = (link.get_attribute("href") or "").strip()
                if not href_candidato:
                    continue
                if "consultascnpj.com" not in href_candidato:
                    continue
                if "/sitemap" in href_candidato:
                    continue
                if not extrair_cnpj_do_link(href_candidato):
                    continue
                href = href_candidato
                if href:
                    break

            if not href or href in vistos:
                continue
            vistos.add(href)
            urls.append(href)
            if len(urls) >= limite:
                break
    except Exception as exc:
        print(f"Falha ao buscar resultados da Receita: {exc}")
    finally:
        if driver_criado_no_fluxo and driver_local is not None:
            driver_local.quit()

    print(f"{len(urls)} candidato(s) encontrados na busca publica.")
    return urls


def montar_linha_receita(detalhe: Dict[str, str]) -> Dict[str, str]:
    nome_fantasia = (detalhe.get("Nome Fantasia") or "").strip()
    fantasia_normalizada = normalizar_texto(nome_fantasia)
    if "possui" in fantasia_normalizada and (
        fantasia_normalizada.startswith("nao")
        or fantasia_normalizada.startswith("no ")
        or fantasia_normalizada.startswith("na o")
    ):
        nome_fantasia = ""
    nome_empresa = nome_fantasia or detalhe.get("Razão Social") or ""
    categoria = detalhe.get("Categoria", "")
    return normalizar_linha_saida(
        {
            "Nome da Empresa": nome_empresa,
            "Número de Telefone": detalhe.get("Número de Telefone", ""),
            "E-mail": detalhe.get("E-mail", ""),
            "Site": "",
            "Endereço": detalhe.get("Endereço", ""),
            "Avaliação": "",
            "Número de Reviews": "",
            "Categoria": categoria,
            "Status": "",
            "Horário de Funcionamento": "",
            "Faixa de Preço": "",
            "Número de Fotos": "",
            "Link Google Maps": "",
            "CNPJ": detalhe.get("CNPJ", ""),
            "Razão Social": detalhe.get("Razão Social", ""),
            "Nome Fantasia": detalhe.get("Nome Fantasia", ""),
            "Situação Cadastral": detalhe.get("Situação Cadastral", ""),
            "CNAE Principal": detalhe.get("CNAE Principal", ""),
        }
    )


def buscar_empresas_receita(estado_sigla: str, cidade: str, nicho: str) -> List[Dict[str, str]]:
    estrategia = construir_estrategia_busca_receita(nicho)
    termos_busca = estrategia["buscas"]
    tokens_nicho = estrategia["tokens"]
    nomes_apoio_google = carregar_nomes_do_csv_google_local(nicho, cidade)

    if len(termos_busca) > 1:
        print(f"Nicho expandido para a Receita: {', '.join(termos_busca)}")

    links: List[str] = []
    vistos_links = set()
    try:
        driver_busca = criar_driver_chrome(headless=True)
    except Exception as exc:
        print(f"Erro ao iniciar o navegador para busca da Receita: {exc}")
        return []

    try:
        for termo_busca in termos_busca:
            query = f'{termo_busca} "{cidade}" {estado_sigla}'
            for link in buscar_links_receita_por_nome_cidade(query, cidade, estado_sigla, driver=driver_busca):
                if link in vistos_links:
                    continue
                vistos_links.add(link)
                links.append(link)

        for nome_empresa in nomes_apoio_google:
            query = f'"{nome_empresa}" "{cidade}" {estado_sigla}'
            for link in buscar_links_receita_por_nome_cidade(query, cidade, estado_sigla, driver=driver_busca):
                if link in vistos_links:
                    continue
                vistos_links.add(link)
                links.append(link)
    finally:
        driver_busca.quit()

    if not links:
        print("Nenhum candidato valido encontrado na busca publica da Receita.")
        return []

    sessao = criar_sessao_http()
    linhas: List[Dict[str, str]] = []
    vistos_cnpj = set()
    cidade_chave = normalizar_texto(cidade)
    uf_chave = normalizar_texto(estado_sigla)

    for indice, url in enumerate(links, start=1):
        try:
            print(f"Consultando detalhe Receita ({indice}/{len(links)}): {url}")
            response = sessao.get(url, timeout=TIMEOUT_PADRAO)
            response.raise_for_status()
            detalhe = parsear_detalhe_consultascnpj(response.text, url)
            if not detalhe:
                continue
        except Exception as exc:
            print(f"Falha ao consultar detalhe da Receita: {exc}")
            continue

        cidade_receita = normalizar_texto(detalhe.get("_cidade_receita", ""))
        uf_receita = normalizar_texto(detalhe.get("_uf_receita", ""))
        situacao_receita = normalizar_texto(detalhe.get("Situação Cadastral", ""))
        cnpj = apenas_digitos(detalhe.get("CNPJ", ""))

        if not cidade_receita or not uf_receita:
            print("Descartado: localidade da Receita nao foi confirmada.")
            continue
        if cidade_receita != cidade_chave:
            print("Descartado: cidade divergente na Receita.")
            continue
        if uf_receita != uf_chave:
            print("Descartado: UF divergente na Receita.")
            continue
        if situacao_receita != "ativa":
            print(f"Descartado: situação cadastral '{detalhe.get('Situação Cadastral', '')}'.")
            continue
        if not detalhe_compativel_com_nicho_receita(detalhe, tokens_nicho):
            print("Descartado: resultado da Receita nao parece compatível com o nicho.")
            continue
        if cnpj and cnpj in vistos_cnpj:
            continue

        if cnpj:
            vistos_cnpj.add(cnpj)
        linhas.append(montar_linha_receita(detalhe))

    print(f"--- Extração Receita finalizada. {len(linhas)} empresa(s) encontradas. ---")
    if not linhas:
        print("Nenhum registro ATIVO com cidade/UF confirmados passou na validação da Receita.")
    return linhas


def chaves_nome_linha(linha: Dict[str, str]) -> set[str]:
    chaves = set()
    for campo in ("Nome da Empresa", "Razão Social", "Nome Fantasia"):
        valor = linha.get(campo, "")
        chave = normalizar_texto(valor)
        if chave:
            chaves.add(chave)
    return chaves


def mesclar_resultados_por_nome_cidade(
    google_rows: Sequence[Dict[str, str]],
    receita_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, str]]:
    resultado = [normalizar_linha_saida(row) for row in google_rows]

    for indice_google, linha_google in enumerate(resultado):
        nomes_google = chaves_nome_linha(linha_google)
        if not nomes_google:
            continue
        for linha_receita in receita_rows:
            if nomes_google.intersection(chaves_nome_linha(linha_receita)):
                resultado[indice_google] = combinar_linhas(linha_google, linha_receita)
                break

    return resultado


def executar_busca(origem: str, estado_nome: str, estado_sigla: str, cidade: str, nicho: str) -> List[Dict[str, str]]:
    if origem == "1":
        return scrape_and_filter_maps(estado_nome, cidade, nicho)
    if origem == "2":
        return buscar_empresas_receita(estado_sigla, cidade, nicho)
    dados_google = scrape_and_filter_maps(estado_nome, cidade, nicho)
    if not dados_google:
        return []
    dados_receita = buscar_empresas_receita(estado_sigla, cidade, nicho)
    return mesclar_resultados_por_nome_cidade(dados_google, dados_receita)


def salvar_csv(dados: Sequence[Dict[str, str]], origem: str, nicho: str, cidade: str) -> Optional[str]:
    if not dados:
        return None
    df = pd.DataFrame([normalizar_linha_saida(linha) for linha in dados], columns=COLUNAS_CSV)
    fonte = obter_rotulo_fonte_saida(origem)
    base_nome_arquivo = f"leads_{fonte}_{sanitize_filename(nicho)}_{sanitize_filename(cidade)}.csv"
    nome_arquivo = get_unique_filename(base_nome_arquivo)
    df.to_csv(nome_arquivo, index=False, encoding="utf-8-sig")
    return nome_arquivo


def executar_modo_lote(origem: str) -> None:
    print("\n--- Buscando arquivos CSV existentes... ---")
    arquivos = carregar_cidades_dos_arquivos_existentes()
    if not arquivos:
        print("Nenhum arquivo CSV encontrado no diretorio atual.")
        return

    print(f"\n{len(arquivos)} arquivo(s) encontrado(s):")
    for indice, arquivo in enumerate(arquivos, start=1):
        print(f"   {indice}. {arquivo['cidade']} ({arquivo['arquivo']})")

    nicho = input("\nDigite o nicho para busca (ex: clinica, dentista, farmacia): ").strip()
    while not nicho:
        print("O nicho nao pode ser vazio.")
        nicho = input("Digite o nicho para busca: ").strip()

    estados = get_estados()
    estado = None
    while not estado:
        estado_digitado = input("Digite o ESTADO (nome ou sigla, ex: Sergipe ou SE): ").strip()
        estado = resolver_estado_por_nome_ou_sigla(estado_digitado, estados)
        if not estado:
            print("Estado nao reconhecido. Tente novamente.")

    cidades_do_estado = get_cidades(estado["sigla"])
    for arquivo in arquivos:
        arquivo["cidade_resolvida"] = resolver_cidade_pelo_nome_arquivo(arquivo["arquivo"], cidades_do_estado)

    print("\n--- Configuracao da Extração em Lote ---")
    print(f"Origem: {origem}")
    print(f"Nicho: {nicho}")
    print(f"Estado: {estado['nome']} ({estado['sigla']})")
    print(f"Cidades: {len(arquivos)}")
    print("-" * 60)

    confirmar = input("\nConfirmar extração para TODAS as cidades? (s/n): ").strip().lower()
    if confirmar != "s":
        print("Operacao cancelada.")
        return

    for indice, arquivo in enumerate(arquivos, start=1):
        cidade_processada = arquivo.get("cidade_resolvida") or arquivo["cidade"]
        print(f"\n{'=' * 60}")
        print(f"PROCESSANDO {indice}/{len(arquivos)}: {cidade_processada}")
        print(f"{'=' * 60}")

        dados = executar_busca(origem, estado["nome"], estado["sigla"], cidade_processada, nicho)
        nome_arquivo = salvar_csv(dados, origem, nicho, cidade_processada)
        if nome_arquivo:
            print(f"\nArquivo salvo: '{nome_arquivo}' ({len(dados)} leads)")
        else:
            print(f"\nNenhum lead encontrado em {cidade_processada}")

        if indice < len(arquivos):
            print(f"\nAguardando {PAUSA_ENTRE_CIDADES} segundos antes da proxima cidade...")
            time.sleep(PAUSA_ENTRE_CIDADES)

    print("\n" + "=" * 60)
    print("EXTRAÇÃO EM LOTE CONCLUIDA!")
    print("=" * 60)


def executar_modo_individual(origem: str) -> None:
    nicho = input("\nPasso 1 de 3: Digite o nome do nicho: ").strip()
    while not nicho:
        print("O nicho nao pode ser vazio.")
        nicho = input("Passo 1 de 3: Digite o nome do nicho: ").strip()

    estados = get_estados()
    estado_selecionado = selecionar_ou_digitar("Selecao de Estado", estados, tipo_opcao="estado")
    print(f"\nBuscando cidades para {estado_selecionado['nome']}...")
    cidades = get_cidades(estado_selecionado["sigla"])
    if not cidades:
        print("Nenhuma cidade encontrada para o estado selecionado.")
        return

    cidade_selecionada = selecionar_ou_digitar("Selecao de Cidade", cidades, tipo_opcao="cidade")

    print("\n--- Revisao ---")
    print(f"Origem: {origem}")
    print(f"Nicho: {nicho}")
    print(f"Estado: {estado_selecionado['nome']}")
    print(f"Cidade: {cidade_selecionada}")
    print("-" * 15)

    if input("Voce confirma a busca com esses dados? (s/n): ").strip().lower() != "s":
        print("\nBusca cancelada pelo usuario.")
        return

    dados = executar_busca(
        origem,
        estado_selecionado["nome"],
        estado_selecionado["sigla"],
        cidade_selecionada,
        nicho,
    )
    nome_arquivo = salvar_csv(dados, origem, nicho, cidade_selecionada)
    if nome_arquivo:
        print(f"\nSucesso! Os dados foram salvos no arquivo '{nome_arquivo}'.")
    else:
        print("\nNenhum dado foi encontrado ou salvo.")


def main() -> None:
    print("=" * 70)
    print("Scraper de Leads - Versao Enriquecida")
    print("=" * 70)
    print("\nEsta versao salva sempre 18 colunas:")
    for coluna in COLUNAS_CSV:
        print(f"  - {coluna}")

    origem = selecionar_origem_lista()

    print("\n--- MODO DE OPERACAO ---")
    print("1. Extrair de TODAS as cidades dos arquivos existentes")
    print("2. Extrair de uma cidade especifica (nova busca)")

    modo = input("\nEscolha uma opcao (1 ou 2): ").strip()
    if modo == "1":
        executar_modo_lote(origem)
    elif modo == "2":
        executar_modo_individual(origem)
    else:
        print("Opcao invalida. Execute o script novamente.")
        return

    print("\n" + "=" * 50)
    if input("Deseja realizar uma nova busca? (s/n): ").strip().lower() == "s":
        print("\n" + "=" * 50 + "\n")
        main()
    else:
        print("\nPrograma finalizado. Ate a proxima!")


if __name__ == "__main__":
    main()
