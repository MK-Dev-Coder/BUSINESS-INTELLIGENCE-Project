import requests
import json
import os

# Configuration
API_URL = "https://api.fda.gov/animalandveterinary/event.json"
OUTPUT_FILE = os.path.join("data", "raw", "fda_events.json")
LIMIT = 1000

def fetch_fda_data():
    print(f"Fetching FDA Adverse Event data...")
    
    # Simpler query: species is Dog
    query = 'animal.species:"Dog"'
    params = {
        "search": query,
        "limit": LIMIT
    }
    
    try:
        print(f"Querying {API_URL} with params: {params}")
        response = requests.get(API_URL, params=params)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code} {response.text}")
            print(f"URL: {response.url}")
            return

        data = response.json()
        results = data.get("results", [])
        print(f"Successfully fetched {len(results)} records.")
        
        # Save to file
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Data saved to {OUTPUT_FILE}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    fetch_fda_data()
