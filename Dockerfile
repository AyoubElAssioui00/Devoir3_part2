# Utilisation de l'image officielle Airflow
FROM apache/airflow:2.7.0-python3.9

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers du projet dans l'image Docker
COPY ./dags /opt/airflow/dags
COPY ./requirements.txt /app/requirements.txt

# Installer les dépendances Python nécessaires
RUN pip install --no-cache-dir -r /app/requirements.txt

# Créer les répertoires pour les datasets et les résultats
RUN mkdir -p /app/data/raw_data /app/results/output_plots

# Exposer les ports pour Airflow
EXPOSE 8080

# Commande par défaut pour Airflow
CMD ["airflow", "standalone"]
