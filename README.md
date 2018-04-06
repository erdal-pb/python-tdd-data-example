# Learning-ETL-with-Luigi
Using the Python library Luigi, I build two small data pipelines, then put them in a Docker container.

1. Grabs a ZIP file online, uploads it to a MongoDB database, then extracts some data out (world_food_data.py)
2. Scrapes a website, saves it to a CSV, then pulls interesting data out (plane_crashes.py)

CreateContainers.sh will create two Docker containers, the first is a stock MongoDB instance, the second is the Python envrionment which the two scripts run on.

Skills:
- Docker
- MongoDB
- Luigi
- BeautifulSoup
- Pandas
- Seaborn
- Bash
