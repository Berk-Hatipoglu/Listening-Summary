import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 16, 15, 20),
    'retries': 0,
}

dag = DAG(
    'music_analysis_dag',
    default_args=default_args,
    description='A simple music analysis DAG',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

current_directory = os.getcwd()

def extract():
    listening_df = pd.read_csv(os.path.join(current_directory, 'data', 'listening.csv'))
    singers_df = pd.read_csv(os.path.join(current_directory, 'data', 'singer.csv'))
    songs_df = pd.read_csv(os.path.join(current_directory, 'data', 'songs.csv'))
    users_df = pd.read_csv(os.path.join(current_directory, 'data', 'users.csv'))
    return listening_df, songs_df, users_df, singers_df

def transform(ti):
    listening_df, songs_df, users_df, singers_df = ti.xcom_pull(task_ids='extract')
    listening_df = listening_df.merge(songs_df, left_on='song_id', right_on='song_id', how='left')
    listening_df = listening_df.merge(users_df, on='user_id', how='left')
    listening_df = listening_df.merge(singers_df, left_on='singer_id', right_on='singer_id', how='left')
    listening_df['listen_datetime'] = pd.to_datetime(listening_df['listen_datetime'])
    listening_df['year'] = listening_df['listen_datetime'].dt.year
    listening_df['hour'] = listening_df['listen_datetime'].dt.hour
    ti.xcom_push(key='listening_df', value=listening_df)
    ti.xcom_push(key='users_df', value=users_df)

def analysis(ti):
    listening_df = ti.xcom_pull(task_ids='transform', key='listening_df')
    users_df = ti.xcom_pull(task_ids='transform', key='users_df')

    def most_listened_by_year(df, group_col):
        return df.groupby(['year', group_col]).size().reset_index(name='count').sort_values(['year', 'count'], ascending=[True, False]).drop_duplicates('year')

    def most_listened_overall(df, group_col):
        return df.groupby(group_col).size().reset_index(name='count').sort_values('count', ascending=False).head(1)

    def popularity_change_over_time(df, group_col):
        return df.groupby(['year', group_col]).size().reset_index(name='count').pivot(index='year', columns=group_col, values='count').fillna(0)

    def user_segmentation_analysis(df, group_col):
        return df.groupby(group_col).size().reset_index(name='count').sort_values('count', ascending=False)

    analyses = {
        "most_listened_singer_by_year": most_listened_by_year(listening_df, 'singer'),
        "most_listened_song_by_year": most_listened_by_year(listening_df, 'song'),
        "most_active_user_by_year": most_listened_by_year(listening_df, 'user_id').merge(users_df, on='user_id', how='left'),
        "most_listened_by_city_and_year": listening_df.groupby(['year', 'city', 'singer', 'song']).size().reset_index(name='count').sort_values(['year', 'city', 'count'], ascending=[True, True, False]).drop_duplicates(['year', 'city']),
        "most_listened_singer_overall": most_listened_overall(listening_df, 'singer'),
        "most_listened_song_overall": most_listened_overall(listening_df, 'song'),
        "most_active_user_overall": most_listened_overall(listening_df, 'user_id').merge(users_df, on='user_id', how='left'),
        "most_popular_hours": listening_df.groupby('hour').size().reset_index(name='count').sort_values('count', ascending=False),
        "popularity_change_of_singers": popularity_change_over_time(listening_df, 'singer'),
        "popularity_change_of_songs": popularity_change_over_time(listening_df, 'song'),
        "user_segmentation_by_age": user_segmentation_analysis(listening_df, 'age'),
        "user_segmentation_by_city": user_segmentation_analysis(listening_df, 'city')
    }
    ti.xcom_push(key='analyses', value=analyses)


def output(ti):
    analyses = ti.xcom_pull(task_ids='analysis', key='analyses')
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{current_directory}/output/output_{current_time}.txt"

    with open(output_file, "w") as f:
        for analysis_name, result in analyses.items():
            f.write(f"# {analysis_name.replace('_', ' ').title()}\n")
            f.write(result.to_string(index=False))
            f.write("\n\n")

    print(f"Results have been written to {output_file}")

with dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
    )

    analysis_task = PythonOperator(
        task_id='analysis',
        python_callable=analysis,
        provide_context=True,
    )

    output_task = PythonOperator(
        task_id='output',
        python_callable=output,
        provide_context=True,
    )

    extract_task >> transform_task >> analysis_task >> output_task
