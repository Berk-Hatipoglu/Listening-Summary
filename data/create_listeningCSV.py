import pandas as pd
import random
import os
from datetime import datetime, timedelta

# Load user and song data
users_df = pd.read_csv("users.csv")
songs_df = pd.read_csv("songs.csv")

# Function to generate random dates and times
def random_datetime(start, end):
    return start + timedelta(
        days=random.randint(0, (end - start).days),
        seconds=random.randint(0, 86400 - 1)  # 86400 seconds in a day
    )

# Define date range
start_date = datetime(2021, 1, 1)
end_date = datetime(2024, 12, 31)

# Check if listening.csv file exists
if not os.path.exists("listening.csv"):
    print("listening.csv file not found, creating a new file...")

    # Create random matches with dates
    listening_data = []

    for i in range(1, 5000001):
        user_id = random.choice(users_df['user_id'])
        song_id = random.choice(songs_df['singer_id'])  # Use singer_id column from songs.csv for random selection
        listen_datetime = random_datetime(start_date, end_date)
        listening_data.append([i, user_id, song_id, listen_datetime])

    # Create DataFrame and save to CSV
    listening_df = pd.DataFrame(listening_data, columns=["listening_id", "user_id", "song_id", "listen_datetime"])
    listening_df.to_csv("listening.csv", index=False)

    print("listening.csv file created successfully!")
else:
    print("listening.csv file already exists.")
