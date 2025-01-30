# Importar librerías
import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3


# Obtención de datos
url = "https://l2h237eh53.execute-api.us-east-1.amazonaws.com/dev/precios"
params = {"start_date":"2024-03-15", "end_date":"2024-04-14"}

try:
    response = requests.get(url, params)
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Error: codigo {response.status_code}")
except requests.exceptions.RequestException as e:
    print(f"Error de conexion: {e}")


# Procesamiento de los Datos
data_flat = data.get("data")

df = pd.DataFrame(data_flat)

df = df.reset_index()
df = df.rename(columns={'index':'hora'})
df_long = pd.melt(df, id_vars=['hora'], var_name='fecha', value_name='precio')


# Tratamiento de Datos Faltantes
horas_faltantes = df_long[df_long['precio'].isna()]

#print(horas_faltantes[['hora', 'fecha']].drop_duplicates())

df_long['precio'] = df_long['precio'].ffill()

df_long['precio'] = df_long['precio'].fillna(df_long['precio'].rolling(window=7, min_periods=1, center=True).mean())

#print(df_long.isna().sum())
#print(df_long)


# Cálculos de Promedios
df_promedio = df_long.groupby('fecha')['precio'].mean().reset_index()
df_promedio = df_promedio.rename(columns={'precio': 'promedio_diario'})
#print(df_promedio.head())

df_promedio['promedio_7d'] = df_promedio['promedio_diario'].rolling(window=7, min_periods=1).mean()
#print(df_promedio.head())


# Visualización
plt.figure(figsize=(10, 6))
plt.plot(df_promedio['fecha'], df_promedio['promedio_diario'], label='Promedio Diario', color='blue', linestyle='-', marker='o')
plt.plot(df_promedio['fecha'], df_promedio['promedio_7d'], label='Promedio Móvil 7d', color='red', linestyle='--')
plt.title('Comparación entre Promedio Diario y Promedio Móvil de 7 Días', fontsize=14)
plt.xlabel('Fecha', fontsize=12)
plt.ylabel('Precio', fontsize=12)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig('image.png')
plt.show()

#Almacenamiento de Resultados
conn = sqlite3.connect("precios.db")

conn.execute('''
    CREATE TABLE IF NOT EXISTS resultados_diarios (
        fecha TEXT PRIMARY KEY,
        precio_promedio REAL,
        precio_7d REAL
    )
''')

df_promedio[['fecha', 'promedio_diario', 'promedio_7d']].to_sql('resultados_diarios', conn, if_exists='replace', index=False)

print("Datos guardados en la base de datos SQLite con éxito.")

query = "SELECT * FROM resultados_diarios"
df_cargado = pd.read_sql(query, conn)
print(df_cargado)

conn.close()
