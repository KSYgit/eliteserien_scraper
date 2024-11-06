import requests
from bs4 import BeautifulSoup
import sqlite3
import re  # Import regular expressions module
import time  # Import the time module for delays

# Connect to the SQLite database
conn = sqlite3.connect('data/eliteserien_data.db')
cursor = conn.cursor()

# Create teams table if it doesn't exist
cursor.execute(''' 
CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY,
    team_name TEXT UNIQUE,
    team_link TEXT
)
''')

# List of URLs to scrape
urls = [
    'https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId=192924',  # 2024 (current)
    'https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId=186850',  # 2023
    'https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId=181484',  # 2022
    'https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId=174382',  # 2021
    'https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId=168990',  # 2020
    'https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId=164089',  # 2019
    'https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId=158475',  # 2018
    'https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId=153173',  # 2017
    # Add more URLs as needed
]

# Set headers to mimic a browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
}

# Dictionary to hold all teams across URLs
all_teams = {}

for url in urls:
    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8'

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        # Scrape the season name
        season_name = soup.find('h1').text.strip()

        # Extract the year from the season name
        year_match = re.search(r'-\s*(\d{4})\s*-', season_name)
        season_year = year_match.group(1) if year_match else "unknown"

        # Scraping the matches table
        table = soup.find('table')

        if table:
            rows = table.find_all('tr')
            matches = []

            for row in rows:
                columns = row.find_all('td')
                if columns:
                    match_data = [col.text.strip() for col in columns]

                    # Extract game link
                    game_number_cell = columns[-1]
                    game_link_tag = game_number_cell.find('a')
                    game_link = f"https://www.fotball.no{game_link_tag['href']}" if game_link_tag else None

                    match_data.append(game_link)
                    matches.append(match_data)

            # Create matches table
            table_name = f"matches_{season_year}"
            cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id INTEGER PRIMARY KEY,
                game_round TEXT,
                date TEXT,
                day_of_week TEXT,
                time TEXT,
                home_team TEXT,
                result TEXT,
                away_team TEXT,
                stadium TEXT,
                game_number TEXT,
                game_link TEXT
            )
            ''')

            # Insert matches into the database
            for match in matches:
                cursor.execute(f'''
                INSERT OR IGNORE INTO "{table_name}" (game_round, date, day_of_week, time, home_team, result, away_team, stadium, game_number, game_link)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', match)

            # Extract and store team links
            for row in rows:
                columns = row.find_all('td')
                if columns:
                    home_team_cell = columns[4].find('a')
                    away_team_cell = columns[6].find('a')

                    if home_team_cell:
                        team_name = home_team_cell.text.strip()
                        team_link = f"https://www.fotball.no{home_team_cell['href']}" if home_team_cell['href'] else None
                        if team_name not in all_teams:  # Check if team is already in the dictionary
                            all_teams[team_name] = team_link
                            # Debug print for home team
                            print(f"Inserting home team: {team_name} with link: {team_link}")

                    if away_team_cell:
                        team_name = away_team_cell.text.strip()
                        team_link = f"https://www.fotball.no{away_team_cell['href']}" if away_team_cell['href'] else None
                        if team_name not in all_teams:  # Check if team is already in the dictionary
                            all_teams[team_name] = team_link
                            # Debug print for away team
                            print(f"Inserting away team: {team_name} with link: {team_link}")

            conn.commit()
            print(f"Season {season_year} scraped and added to DB successfully.")
            time.sleep(3)
        else:
            print(f"Table not found for URL: {url}")
    else:
        print(f"Failed to retrieve data for URL: {url}. Status code: {response.status_code}")

# Insert all unique teams after processing all URLs
for team_name, team_link in all_teams.items():
    cursor.execute('''
    INSERT OR IGNORE INTO teams (team_name, team_link)
    VALUES (?, ?)
    ''', (team_name, team_link))

print("Script is now closing")
conn.commit()
conn.close()
