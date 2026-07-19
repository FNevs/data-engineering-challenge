# Imagem base oficial do Airflow. A versao e fixada para garantir builds
# reproduziveis: `latest` quebraria o projeto a cada release upstream.
FROM apache/airflow:2.9.3-python3.11

USER airflow

COPY requirements.txt /tmp/requirements.txt

# --no-cache-dir mantem a imagem enxuta; a constraint da propria imagem evita
# que o pip promova upgrades incompativeis das dependencias do Airflow.
RUN pip install --no-cache-dir -r /tmp/requirements.txt
