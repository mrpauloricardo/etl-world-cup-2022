import pandas as pd

# --- Etapa de carregamento e extração de dados (Extract) ---

# Leitura do arquivo .csv
df_copa_2022 = pd.read_csv("world_cup_2022.csv")

# Obter as informações das colunas e os tipos de dados
print(df_copa_2022.info())

# Filtragem de colunas relevantes para o projeto
colunas_selecionadas = [
    'date',
    'hour',
    'category',
    'team1',
    'team2',
    'number of goals team1',
    'number of goals team2',
    'possession team1',
    'possession team2',
    'total attempts team1',
    'total attempts team2',
    'on target attempts team1',
    'on target attempts team2',
    'yellow cards team1',
    'yellow cards team2',
    'red cards team1',
    'red cards team2',
    'fouls against team1',
    'fouls against team2',
    'passes team1',
    'passes team2',
    'crosses team1',
    'crosses team2'
]


# Novo DataFrame com as colunas selecionadas
df_copa_2022_filtrado = df_copa_2022[colunas_selecionadas]

print(df_copa_2022_filtrado)

#  --- Etapa de transformação dos dados (Transform) ---

# Checagem de valores nulos ou inexistentes
valores_ausentes = df_copa_2022_filtrado.isnull().sum()

# Substituição dos valores nulos por 0
df_copa_2022_transformado = df_copa_2022_filtrado.fillna(0)

# Conversão de data e hora
df_copa_2022_transformado['date'] = pd.to_datetime(df_copa_2022_transformado['date'], format='%d %b %Y')
df_copa_2022_transformado['hour'] = pd.to_datetime(df_copa_2022_transformado['hour'], format='%H : %M').dt.time

# Criação de nova coluna com a média de gols por partida
df_copa_2022_transformado['total goals per match'] = (df_copa_2022_transformado['number of goals team1'] + df_copa_2022_transformado['number of goals team2'])

# Agregação de dados e cálculo das médias por categoria
agregacao = df_copa_2022_transformado.groupby('category')[[
    'number of goals team1',
    'number of goals team2',
    'total attempts team1',
    'total attempts team2',
    'on target attempts team1',
    'on target attempts team2',
    'yellow cards team1',
    'yellow cards team2',
    'red cards team1',
    'red cards team2',
    'fouls against team1',
    'fouls against team2',
    'passes team1',
    'passes team2',
    'crosses team1',
    'crosses team2'
]].mean()

# Função para registrar o resultado da partida
def get_match_result(row):
    return f"{row['team1']} {row['number of goals team1']} x {row['number of goals team2']} {row['team2']}"

# Criação de nova coluna 'match result' com base nos gols
df_copa_2022_transformado['match result'] = df_copa_2022_transformado.apply(get_match_result, axis=1)

print(df_copa_2022_transformado)

# --- Etapa de carregamento dos dados (Load) ---

# Criando novo csv com as transformações
df_copa_2022_transformado.to_csv("copa_transformado.csv", index=False)