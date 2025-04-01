import pandas as pd
import json
from glob import glob
import os

# === CARGA DE ARCHIVOS JSON DE COSTOS TURÍSTICOS POR CONTINENTE ===


def cargar_costos_turisticos():
    ruta = "./data/MONGO/"
    archivos_json = glob(os.path.join(ruta, "costos_turisticos_*.json"))
    lista_paises = []

    for archivo in archivos_json:
        with open(archivo, encoding="utf-8") as f:
            datos = json.load(f)
            lista_paises.extend(datos)

    df_costos = pd.json_normalize(lista_paises, sep="_")
    return df_costos


# === CARGA DE DATOS ADICIONALES ===

def cargar_datos_adicionales():
    ruta_mongo = "./data/MONGO/"
    ruta_sql = "./data/SQL/"

    df_bigmac = pd.read_json(os.path.join(ruta_mongo, "paises_mundo_big_mac.json"))

    df_poblacion = pd.read_csv(os.path.join(ruta_sql, "pais_poblacion.csv"))
    df_poblacion.rename(
        columns={"pais": "país", "poblacion": "poblacion_total"}, inplace=True
    )

    df_envejecimiento = pd.read_csv(os.path.join(ruta_sql, "pais_envejecimiento.csv"))
    df_envejecimiento.rename(
        columns={
            "nombre_pais": "país",
            "tasa_de_envejecimiento": "porcentaje_envejecimiento",
        },
        inplace=True,
    )

    return df_bigmac, df_poblacion, df_envejecimiento


# === UNIFICACIÓN DE DATOS ===


def integrar_datos(df_costos, df_bigmac, df_poblacion, df_envejecimiento):
    for df in [df_costos, df_bigmac, df_poblacion, df_envejecimiento]:
        df["país"] = df["país"].str.strip()

    cols_a_eliminar = [
        col
        for col in df_poblacion.columns
        if col in df_costos.columns and col != "país"
    ]
    df_poblacion = df_poblacion.drop(columns=cols_a_eliminar)

    cols_a_eliminar = [
        col
        for col in df_envejecimiento.columns
        if col in df_costos.columns and col != "país"
    ]
    df_envejecimiento = df_envejecimiento.drop(columns=cols_a_eliminar)

    df_merged = (
        df_costos.merge(df_bigmac, on="país", how="left")
        .merge(df_poblacion, on="país", how="left")
        .merge(df_envejecimiento, on="país", how="left")
    )

    return df_merged


# === LIMPIEZA Y TRANSFORMACIÓN FINAL ===


def limpiar_datos(df):
    df = df.dropna(
        subset=["precio_big_mac_usd", "poblacion_total", "porcentaje_envejecimiento"]
    )

    df["costo_total_diario_promedio"] = (
        df["costos_diarios_estimados_en_dólares_hospedaje_precio_promedio_usd"]
        + df["costos_diarios_estimados_en_dólares_comida_precio_promedio_usd"]
        + df["costos_diarios_estimados_en_dólares_transporte_precio_promedio_usd"]
        + df["costos_diarios_estimados_en_dólares_entretenimiento_precio_promedio_usd"]
    )

    df.columns = df.columns.str.replace(" ", "_").str.lower()
    return df


# === EJECUCIÓN GENERAL ===


def main():
    print("Cargando datos...")
    df_costos = cargar_costos_turisticos()
    df_bigmac, df_poblacion, df_envejecimiento = cargar_datos_adicionales()

    print("Integrando fuentes...")
    df_completo = integrar_datos(df_costos, df_bigmac, df_poblacion, df_envejecimiento)

    print("Limpiando y transformando datos...")
    df_final = limpiar_datos(df_completo)

    print("Vista previa de los datos integrados:")
    print(df_final.head())

    print("verificar que no hayan nulls")
    print(df_final.isnull().sum())

    #rellenar los nulls con nana
    df_final.fillna("NaN", inplace=True)
    print("verificar que no hayan nulls después de la relleno con NaN")
    print(df_final.isnull().sum())

    #verificar los datos suplicados
    print("verificar que no hayan duplicados")
    print(df_final.duplicated().sum())

    os.makedirs("Lab07-DB2/data/NewData", exist_ok=True)

    output_path = "Lab07-DB2/data/NewData/datos_integrados.csv"
    print(f"Guardando en archivo CSV: {output_path}")
    df_final.to_csv(output_path, index=False)

main()