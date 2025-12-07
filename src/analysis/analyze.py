import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

DB_FILE = os.path.join("data", "processed", "warehouse.db")
PLOTS_DIR = os.path.join("docs", "plots")

def get_conn():
    return sqlite3.connect(DB_FILE)

def analyze_reactions_by_breed():
    print("Analyzing reactions by breed...")
    conn = get_conn()
    query = """
    SELECT e.breed, r.reaction_term, COUNT(*) as count
    FROM events e
    JOIN reactions r ON e.event_id = r.event_id
    WHERE e.breed IS NOT NULL
    GROUP BY e.breed, r.reaction_term
    ORDER BY count DESC
    LIMIT 20
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("Top 20 Breed-Reaction pairs:")
    print(df)
    
    # Plot top 10
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df.head(10), x='count', y='reaction_term', hue='breed')
    plt.title('Top 10 Reactions by Breed')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'reactions_by_breed.png'))
    plt.close()

def analyze_active_ingredients():
    print("\nAnalyzing active ingredients...")
    conn = get_conn()
    query = """
    SELECT active_ingredient, COUNT(*) as count
    FROM drugs
    WHERE active_ingredient IS NOT NULL
    GROUP BY active_ingredient
    ORDER BY count DESC
    LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("Top 10 Active Ingredients:")
    print(df)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x='count', y='active_ingredient')
    plt.title('Top 10 Active Ingredients Causing Adverse Events')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'active_ingredients.png'))
    plt.close()

def analyze_size_correlation():
    print("\nAnalyzing size correlation...")
    conn = get_conn()
    query = """
    SELECT weight_kg, outcome
    FROM events
    WHERE weight_kg IS NOT NULL AND outcome IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data for size correlation.")
        return

    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='outcome', y='weight_kg')
    plt.title('Weight Distribution by Outcome')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'weight_by_outcome.png'))
    plt.close()

def analyze_gender_correlation():
    print("\nAnalyzing gender correlation...")
    conn = get_conn()
    query = """
    SELECT gender, outcome, COUNT(*) as count
    FROM events
    WHERE gender IS NOT NULL AND outcome IS NOT NULL
    GROUP BY gender, outcome
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data for gender correlation.")
        return

    # Pivot for heatmap
    pivot = df.pivot(index='outcome', columns='gender', values='count').fillna(0)
    
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot, annot=True, fmt='g', cmap='YlGnBu')
    plt.title('Outcome by Gender')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'gender_outcome_heatmap.png'))
    plt.close()

def analyze_days_to_reaction():
    print("\nAnalyzing days to reaction...")
    conn = get_conn()
    query = """
    SELECT days_to_reaction
    FROM events
    WHERE days_to_reaction IS NOT NULL AND days_to_reaction >= 0 AND days_to_reaction < 365
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data for days to reaction.")
        return

    plt.figure(figsize=(10, 6))
    sns.histplot(df['days_to_reaction'], bins=30)
    plt.title('Distribution of Days to Reaction')
    plt.xlabel('Days')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'days_to_reaction.png'))
    plt.close()

def analyze_breeding_groups():
    print("\nAnalyzing breeding groups...")
    conn = get_conn()
    # This is a simplified join. In production, use better fuzzy matching.
    # We try to match if the FDA breed string contains the API breed name.
    # This is slow in SQL, so we fetch and process in Python for this prototype.
    
    events_df = pd.read_sql_query("SELECT event_id, breed FROM events WHERE breed IS NOT NULL", conn)
    breeds_df = pd.read_sql_query("SELECT breed_name, breeding_group FROM breed_info WHERE breeding_group IS NOT NULL", conn)
    
    # Simple matching logic
    def find_group(fda_breed):
        # Normalize
        fda_breed_norm = fda_breed.lower()
        for _, row in breeds_df.iterrows():
            if row['breed_name'].lower() in fda_breed_norm:
                return row['breeding_group']
        return "Unknown"

    events_df['breeding_group'] = events_df['breed'].apply(find_group)
    
    # Now join with reactions
    reactions_df = pd.read_sql_query("SELECT event_id, reaction_term FROM reactions", conn)
    merged = pd.merge(events_df, reactions_df, on='event_id')
    
    # Count reactions by group
    group_counts = merged.groupby(['breeding_group', 'reaction_term']).size().reset_index(name='count')
    group_counts = group_counts.sort_values('count', ascending=False)
    
    print("Top Reactions by Breeding Group:")
    print(group_counts.head(10))
    
    # Plot top groups
    top_groups = group_counts[group_counts['breeding_group'] != 'Unknown'].head(15)
    if not top_groups.empty:
        plt.figure(figsize=(12, 6))
        sns.barplot(data=top_groups, x='count', y='reaction_term', hue='breeding_group')
        plt.title('Top Reactions by Breeding Group')
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, 'reactions_by_breeding_group.png'))
        plt.close()
    
    conn.close()

if __name__ == "__main__":
    analyze_reactions_by_breed()
    analyze_active_ingredients()
    analyze_size_correlation()
    analyze_gender_correlation()
    analyze_days_to_reaction()
    analyze_breeding_groups()
