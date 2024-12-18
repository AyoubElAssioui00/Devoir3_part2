from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from googleapiclient.discovery import build
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from transformers import pipeline
# Paramètres de l'API YouTube
API_KEY = 'AIzaSyAxs7uzwcGOqJftvR_p9gqZ2nT6KmwtKj4'
CHANNEL_IDS = ['UCoOae5nYA7VqaXzerajD0lg']# Exemple de chaîne YouTube
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# scrapping task
def get_video_details():
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

    # Extraire les données d'une playlist (exemple)
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId="UUoOae5nYA7VqaXzerajD0lg",
        maxResults=50
    )
    response = request.execute()

    # Transformer les données en DataFrame
    video_data = []
    for item in response['items']:
        video_data.append({
            'videoId': item['contentDetails']['videoId'],
            'title': item['snippet']['title'],
            'publishedAt': item['snippet']['publishedAt']
        })

    df = pd.DataFrame(video_data)

    # Sauvegarder avec horodatage
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    df.to_csv(f"data/raw_data/youtube_data_{timestamp}.csv", index=False)
    print(f"Données sauvegardées : youtube_data_{timestamp}.csv")
#preprocessing task
def preprocess_data():
    # Charger les données les plus récentes
    latest_file = max(Path("data/raw_data").glob("youtube_data_*.csv"))
    df = pd.read_csv(latest_file)

    # Préprocessing des données
    df['publishedAt'] = pd.to_datetime(df['publishedAt'])
    df['title_length'] = df['title'].apply(len)

    # Sauvegarder les données prétraitées
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    df.to_csv(f"data/raw_data/youtube_data_cleaned_{timestamp}.csv", index=False)
    print(f"Données prétraitées sauvegardées : youtube_data_cleaned_{timestamp}.csv")
#Analysing task
def analyze_sentiment():
    # Charger les données prétraitées les plus récentes
    latest_file = max(Path("data/raw_data").glob("youtube_data_cleaned_*.csv"))
    df = pd.read_csv(latest_file)

    # Analyse de sentiment avec HuggingFace
    sentiment_pipeline = pipeline("sentiment-analysis")
    df['sentiment'] = df['title'].apply(lambda x: sentiment_pipeline(x)[0]['label'])

    # Sauvegarder les résultats
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    df.to_csv(f"data/raw_data/youtube_data_analyzed_{timestamp}.csv", index=False)
    print(f"Données analysées sauvegardées : youtube_data_analyzed_{timestamp}.csv")

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#Visualisation task
def generate_visualizations():
    # Charger les données analysées les plus récentes
    latest_file = max(Path("data/raw_data").glob("youtube_data_analyzed_*.csv"))
    df = pd.read_csv(latest_file)

    # Créer un répertoire pour les visualisations
    output_dir = Path("results/output_plots")
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Visualisation : Répartition des sentiments
    plt.figure(figsize=(10, 6))
    sns.countplot(x='sentiment', data=df, palette='coolwarm')
    plt.title('Répartition des sentiments des titres de vidéos')
    plt.xlabel('Sentiment')
    plt.ylabel('Nombre de titres')
    plt.savefig(output_dir / "sentiment_distribution.png")
    print(f"Graphique sauvegardé : sentiment_distribution.png")

    # 2. Visualisation : Longueur des titres vs Sentiment
    plt.figure(figsize=(10, 6))
    sns.boxplot(x='sentiment', y='title_length', data=df, palette='coolwarm')
    plt.title('Longueur des titres en fonction du sentiment')
    plt.xlabel('Sentiment')
    plt.ylabel('Longueur du titre')
    plt.savefig(output_dir / "title_length_vs_sentiment.png")
    print(f"Graphique sauvegardé : title_length_vs_sentiment.png")

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'youtube_scraping_dag',
    default_args=default_args,
    description='DAG pour scraper les données YouTube',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),
    catchup=False
) as dag:

    scrape_data = PythonOperator(
        task_id='scrape_youtube_data',
        python_callable=get_video_details
    )
    preprocess_task = PythonOperator(
        task_id='preprocess_youtube_data',
        python_callable=preprocess_data
    )
    analyze_task = PythonOperator(
        task_id='analyze_sentiment',
        python_callable=analyze_sentiment
    )
    visualize_results = PythonOperator(
        task_id='generate_visualizations',
        python_callable=generate_visualizations
    )

    scrape_data >> preprocess_task >> analyze_task >> visualize_results
