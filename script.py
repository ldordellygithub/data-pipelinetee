import pandas as pd
import boto3
import os

BUCKET = os.getenv("BUCKET_NAME", "mi-bucket-ejemplo")
FILE_NAME = "filtered_data.csv"

# Simulación de ingesta local
df = pd.read_csv("datos.csv")

# Transformación
filtered = df[(df["HTTPS"] == True) & (df["Category"] == "Data")]
filtered.to_csv(FILE_NAME, index=False)

# Subida a S3
s3 = boto3.client("s3")
s3.upload_file(FILE_NAME, BUCKET, FILE_NAME)
print(f"Archivo {FILE_NAME} subido a S3 correctamente.")
print("arcihvo  actualizado  y listo")