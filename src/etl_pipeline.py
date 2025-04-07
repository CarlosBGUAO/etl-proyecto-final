# etl_pipeline.py con Prefect

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
from datetime import datetime
import os
from prefect import flow, task

# Configuración de visualización
@task
def configurar_entorno():
    plt.style.use('ggplot')
    sns.set(style="whitegrid")
    pd.set_option('display.max_columns', None)

# Función para cargar datos desde CSV
@task
def cargar_datos():
    print("Cargando datos...")
    potencial = pd.read_csv('data/datosFNB.csv', sep=';', encoding='ISO-8859-1')
    ventas = pd.read_csv('data/HistoricoVentas.csv', encoding='utf-8')
    resumen = pd.read_csv('data/DatosResueltos.csv', sep=';', encoding='cp1252')
    return potencial, ventas, resumen

# Función para limpiar y transformar datos potenciales
@task
def limpiar_potencial(df):
    df = df.copy()
    df['nombreDepartamento'] = df['nombreDepartamento'].str.upper()
    df['municipio'] = df['municipio'].str.upper()
    df['Cupo'] = pd.to_numeric(df['Cupo'], errors='coerce')
    df['estrato'] = pd.to_numeric(df['estrato'], errors='coerce')
    return df

# Función para limpiar y transformar datos de ventas
@task
def limpiar_ventas(df):
    df = df.copy()
    df['fechaVenta'] = pd.to_datetime(df['fechaVenta'], errors='coerce')
    df['año_venta'] = df['fechaVenta'].dt.year
    df['mes_venta'] = df['fechaVenta'].dt.month
    df['nombreDepartamento'] = df['nombreDepartamento'].str.upper()
    df['municipio'] = df['municipio'].str.upper()
    df['valorTotal'] = pd.to_numeric(df['valorTotal'], errors='coerce')
    df = df[df['estadoVenta'] == 'Entregado']
    return df

# Análisis por distribuidora
@task
def analisis_distribuidora(potencial, ventas):
    pot = potencial.groupby('Distribuidora').agg(
        clientes_potenciales=('Codigo', 'count'),
        cupo_total=('Cupo', 'sum')
    ).reset_index()

    ven = ventas.groupby('Canal').agg(
        clientes_con_credito=('documentoUsuario', 'nunique'),
        ventas_totales=('valorTotal', 'sum'),
        ticket_promedio=('valorTotal', 'mean')
    ).reset_index().rename(columns={'Canal': 'Distribuidora'})

    df = pd.merge(pot, ven, on='Distribuidora', how='left')
    df['tasa_penetracion'] = df['clientes_con_credito'] / df['clientes_potenciales'] * 100
    return df

# Análisis por municipio
@task
def analisis_municipio(potencial, ventas):
    pot = potencial.groupby(['Distribuidora', 'nombreDepartamento', 'municipio']).agg(
        clientes_potenciales=('Codigo', 'count'),
        cupo_total=('Cupo', 'sum')
    ).reset_index()

    ven = ventas.groupby(['Canal', 'nombreDepartamento', 'municipio']).agg(
        clientes_con_credito=('documentoUsuario', 'nunique'),
        ventas_totales=('valorTotal', 'sum'),
        ticket_promedio=('valorTotal', 'mean'),
        plazo_promedio=('cuotas', 'mean')
    ).reset_index().rename(columns={'Canal': 'Distribuidora'})

    df = pd.merge(pot, ven, on=['Distribuidora', 'nombreDepartamento', 'municipio'], how='left')
    df['tasa_penetracion'] = df['clientes_con_credito'] / df['clientes_potenciales'] * 100
    return df

# Visualizaciones
@task
def generar_visualizaciones(pen_dist, pen_mun):
    os.makedirs('visualizaciones', exist_ok=True)

    plt.figure(figsize=(10, 6))
    sns.barplot(x='Distribuidora', y='tasa_penetracion', data=pen_dist)
    plt.title('Tasa de Penetración por Distribuidora')
    plt.xlabel('Distribuidora')
    plt.ylabel('Penetración (%)')
    plt.savefig('visualizaciones/penetracion_distribuidora.png')
    plt.close()

    top_mun = pen_mun.sort_values('tasa_penetracion', ascending=False).head(10)
    plt.figure(figsize=(12, 8))
    sns.barplot(x='municipio', y='tasa_penetracion', data=top_mun)
    plt.title('Top 10 Municipios por Tasa de Penetración')
    plt.xlabel('Municipio')
    plt.ylabel('Penetración (%)')
    plt.xticks(rotation=45)
    plt.savefig('visualizaciones/top_municipios.png')
    plt.close()

    oportunidad = pen_mun[(pen_mun['clientes_potenciales'] > pen_mun['clientes_potenciales'].quantile(0.75)) &
                          (pen_mun['tasa_penetracion'] < pen_mun['tasa_penetracion'].median())]
    top_oportunidad = oportunidad.sort_values('clientes_potenciales', ascending=False).head(10)
    plt.figure(figsize=(12, 8))
    sns.barplot(x='municipio', y='clientes_potenciales', data=top_oportunidad)
    plt.title('Top 10 Municipios con Mayor Oportunidad')
    plt.xlabel('Municipio')
    plt.ylabel('Clientes Potenciales')
    plt.xticks(rotation=45)
    plt.savefig('visualizaciones/oportunidad_municipios.png')
    plt.close()

# Guardar en SQLite
@task
def guardar_sqlite(potencial, ventas, resumen, pen_dist, pen_mun):
    os.makedirs('output', exist_ok=True)
    conn = sqlite3.connect('output/analisis_creditos.db')
    potencial.to_sql('potencial', conn, if_exists='replace', index=False)
    ventas.to_sql('ventas', conn, if_exists='replace', index=False)
    resumen.to_sql('resumen_municipio', conn, if_exists='replace', index=False)
    pen_dist.to_sql('penetracion_distribuidora', conn, if_exists='replace', index=False)
    pen_mun.to_sql('penetracion_municipio', conn, if_exists='replace', index=False)
    conn.close()

# Pipeline completo con Prefect
@flow(name="ETL Pipeline - Penetración Créditos")
def etl_pipeline():
    configurar_entorno()
    datos_pot, datos_ventas, resumen = cargar_datos()
    datos_pot_clean = limpiar_potencial(datos_pot)
    datos_ventas_clean = limpiar_ventas(datos_ventas)
    penetracion_dist = analisis_distribuidora(datos_pot_clean, datos_ventas_clean)
    penetracion_mun = analisis_municipio(datos_pot_clean, datos_ventas_clean)
    generar_visualizaciones(penetracion_dist, penetracion_mun)
    guardar_sqlite(datos_pot_clean, datos_ventas_clean, resumen, penetracion_dist, penetracion_mun)
    print("Pipeline ETL ejecutado exitosamente.")

# Ejecutar pipeline si se corre el script
if __name__ == "__main__":
    etl_pipeline()
