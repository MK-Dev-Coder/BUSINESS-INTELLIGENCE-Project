import requests
import json
import os

# Configuration
API_URL = "https://api.thedogapi.com/v1/breeds"
OUTPUT_FILE = os.path.join("data", "raw", "dog_breeds.json")

def fetch_dog_breeds():
    print("Fetching Dog Breed data...")
    
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        
        print(f"Successfully fetched {len(data)} breeds.")
        
        # Save to file
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {OUTPUT_FILE}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    fetch_dog_breeds()
