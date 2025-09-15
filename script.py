import pandas as pd
import boto3
import os

BUCKET = os.getenv("BUCKET_NAME", "mi-bucket-ejemplo2025")
FILE_NAME = "filtered_data.csv"
SOURCE_FILE = "datos.csv"

def main():
    try:
        print("🔍 Verificando existencia del archivo de entrada...")
        if not os.path.exists(SOURCE_FILE):
            print(f"❌ ERROR: El archivo '{SOURCE_FILE}' no existe.")
            exit(1)

        print("📥 Leyendo datos desde 'datos.csv'...")
        df = pd.read_csv(SOURCE_FILE)

        print("🔎 Filtrando datos por HTTPS=True y Category='Data'...")
        filtered = df[(df["HTTPS"] == True) & (df["Category"] == "Data")]

        print(f"💾 Guardando datos filtrados en '{FILE_NAME}'...")
        filtered.to_csv(FILE_NAME, index=False)

        print(f"🚀 Conectando con S3 y subiendo '{FILE_NAME}' al bucket '{BUCKET}'...")
        s3 = boto3.client("s3")
        s3.upload_file(FILE_NAME, BUCKET, FILE_NAME)

        print(f"✅ Archivo '{FILE_NAME}' subido correctamente a S3.")
        print("📌 Proceso completado con éxito.")

    except Exception as e:
        print(f"❗ ERROR durante la ejecución: {e}")
        exit(1)

if __name__ == "__main__":
    main()
