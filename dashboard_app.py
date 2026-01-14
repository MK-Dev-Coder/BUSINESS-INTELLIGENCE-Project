import streamlit as st
import pandas as pd
import sqlite3
import os
import matplotlib.pyplot as plt
import pydeck as pdk

# Configuration
# Resolving DB Path relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'veterinary_dw.db')

st.set_page_config(page_title="Veterinary BI Dashboard", layout="wide")

def get_connection():
    if not os.path.exists(DB_PATH):
        st.error(f"Database not found at {DB_PATH}. Please run the ETL pipeline first (main_pipeline.py).")
        return None
    return sqlite3.connect(DB_PATH)

def main():
    st.title("🐾 Veterinary Business Intelligence Dashboard")
    st.markdown("""
    This interactive dashboard monitors adverse events reported to the FDA.
    It serves **Task 3b: Dashboard Visualization** of the project requirements.
    """)

    conn = get_connection()
    if not conn:
        return

    # --- Sidebar Filters ---
    st.sidebar.header("Filter Data")
    
    # Get available years for filtering
    try:
        available_years = pd.read_sql("SELECT DISTINCT Year FROM DimTime ORDER BY Year DESC", conn)
        years = available_years['Year'].dropna().tolist()
    except Exception:
        years = []
    
    selected_year = st.sidebar.selectbox("Select Year", ["All"] + years)

    # --- KPIs ---
    st.header("Key Performance Indicators")
    
    # Base Query Conditions
    where_clause = ""
    params = []
    if selected_year != "All":
        where_clause = " WHERE t.Year = ?"
        params = [selected_year]

    col1, col2, col3 = st.columns(3)

    # KPI 1: Total Events
    query_total = f"""
        SELECT COUNT(f.EventID) 
        FROM FactAdverseEvents f
        JOIN DimTime t ON f.TimeKey = t.TimeKey
        {where_clause}
    """
    total_events = conn.execute(query_total, params).fetchone()[0]
    col1.metric("Total Adverse Events", total_events)

    # KPI 2: Critical Cases
    query_critical = f"""
        SELECT COUNT(f.EventID)
        FROM FactAdverseEvents f
        JOIN DimTime t ON f.TimeKey = t.TimeKey
        JOIN DimOutcome o ON f.OutcomeKey = o.OutcomeKey
        WHERE o.SeverityLevel = 'Critical'
    """
    if selected_year != "All":
        query_critical += " AND t.Year = ?"
    
    critical_events = conn.execute(query_critical, params).fetchone()[0]
    critical_pct = (critical_events / total_events * 100) if total_events > 0 else 0
    col2.metric("Critical Cases", critical_events, f"{critical_pct:.1f}% Rate")

    # KPI 3: Top Species Reported
    query_species = f"""
        SELECT a.Species, COUNT(f.EventID) as Cnt
        FROM FactAdverseEvents f
        JOIN DimTime t ON f.TimeKey = t.TimeKey
        JOIN DimAnimal a ON f.AnimalKey = a.AnimalKey
        {where_clause}
        GROUP BY a.Species
        ORDER BY Cnt DESC
        LIMIT 1
    """
    top_species_row = conn.execute(query_species, params).fetchone()
    top_species = top_species_row[0] if top_species_row else "N/A"
    col3.metric("Most Affected Species", top_species)

    st.markdown("---")

    # --- Visualizations ---
    
    # Row 1
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Monthly Trend Analysis")
        query_trend = f"""
            SELECT 
                t.Year, t.Month, COUNT(f.EventID) as TotalEvents,
                t.Year || '-' || printf('%02d', t.Month) as MonthStr
            FROM FactAdverseEvents f
            JOIN DimTime t ON f.TimeKey = t.TimeKey
            {where_clause}
            GROUP BY t.Year, t.Month
            ORDER BY t.Year, t.Month
        """
        df_trend = pd.read_sql_query(query_trend, conn, params=params)
        if not df_trend.empty:
            st.line_chart(df_trend.set_index('MonthStr')['TotalEvents'])
        else:
            st.info("No trend data available.")

    with c2:
        st.subheader("Severity Distribution")
        query_severity = f"""
            SELECT o.SeverityLevel, COUNT(f.EventID) as Count
            FROM FactAdverseEvents f
            JOIN DimTime t ON f.TimeKey = t.TimeKey
            JOIN DimOutcome o ON f.OutcomeKey = o.OutcomeKey
            {where_clause}
            GROUP BY o.SeverityLevel
        """
        df_severity = pd.read_sql_query(query_severity, conn, params=params)
        
        if not df_severity.empty:
            # Using Matplotlib primarily because Streamlit's native pie chart support is limited without other libs
            # But st.bar_chart is easy. Let's stick to matplotlib for the Pie as requested in the script originally
            fig1, ax1 = plt.subplots()
            colors = ['#ff9999', '#66b3ff']
            ax1.pie(df_severity['Count'], labels=df_severity['SeverityLevel'], autopct='%1.1f%%', startangle=90, colors=colors)
            ax1.axis('equal')  
            st.pyplot(fig1)
        else:
            st.info("No severity data available.")

    # Row 2
    st.subheader("Top 10 Adverse Reactions")
    query_reactions = f"""
        SELECT r.ReactionName, COUNT(f.EventID) as Count
        FROM FactAdverseEvents f
        JOIN DimTime t ON f.TimeKey = t.TimeKey
        JOIN DimReaction r ON f.ReactionKey = r.ReactionKey
        WHERE r.ReactionName != 'Unknown'
        """
    if selected_year != "All":
        query_reactions += " AND t.Year = ?"
    
    query_reactions += """
        GROUP BY r.ReactionName
        ORDER BY Count DESC
        LIMIT 10
    """
    
    df_reactions = pd.read_sql_query(query_reactions, conn, params=params)
    if not df_reactions.empty:
        st.bar_chart(df_reactions.set_index('ReactionName'))
    else:
        st.info("No reaction data available.")

    # Row 3
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("Geographic Distribution")
        
        # 1. Fetch Aggregated Data
        query_geo = f"""
            SELECT g.Country, COUNT(f.EventID) as EventCount
            FROM FactAdverseEvents f
            JOIN DimTime t ON f.TimeKey = t.TimeKey
            JOIN DimGeography g ON f.GeographyKey = g.GeographyKey
            {where_clause}
            GROUP BY g.Country
        """
        df_geo = pd.read_sql_query(query_geo, conn, params=params)
        
        if not df_geo.empty:
            # 2. Add Coordinates Concept (Map)
            # Create a simple tab view: Chart vs Map
            tab1, tab2 = st.tabs(["📊 Filtered Bar Chart", "🌍 Interactive Map"])
            
            with tab1:
                # Option B: Log Scaling
                use_log = st.checkbox("Use Log Scale", key="geo_log")
                
                df_sorted = df_geo.sort_values('EventCount', ascending=False).head(10)
                
                if use_log:
                    fig, ax = plt.subplots(figsize=(6, 4))
                    ax.bar(df_sorted['Country'], df_sorted['EventCount'], color='#4c72b0')
                    ax.set_yscale('log')
                    ax.set_ylabel("Event Count (Log Scale)")
                    ax.set_title("Top 10 Countries (Log Scale)")
                    st.pyplot(fig)
                else:
                    st.bar_chart(df_sorted.set_index('Country'))

                # Option A: Drill Down to US States
                # Check for either ISO code 'US' or 'USA'
                is_us_present = ('US' in df_geo['Country'].values) or ('USA' in df_geo['Country'].values)
                
                if is_us_present:
                    st.markdown("---")
                    if st.checkbox("🇺🇸 Drill down to US States"):
                        # Re-construct query context for States
                        state_where = "WHERE g.Country IN ('US', 'USA')"
                        state_params = []
                        
                        if selected_year != "All":
                            state_where += " AND t.Year = ?"
                            state_params = [selected_year]
                            
                        query_states = f"""
                            SELECT g.State, COUNT(f.EventID) as EventCount
                            FROM FactAdverseEvents f
                            JOIN DimTime t ON f.TimeKey = t.TimeKey
                            JOIN DimGeography g ON f.GeographyKey = g.GeographyKey
                            {state_where}
                            GROUP BY g.State
                            ORDER BY EventCount DESC
                            LIMIT 10
                        """
                        df_states = pd.read_sql_query(query_states, conn, params=state_params)
                        
                        if not df_states.empty:
                            st.write("Top 10 US States by Reports")
                            st.bar_chart(df_states.set_index('State'))
                        else:
                            st.info("No detailed state data available for US.")
            
            with tab2:
                # Coordinate Mapping (ISO Alpha-2 to Lat/Lon)
                country_coords = {
                    "US": [37.0902, -95.7129], "USA": [37.0902, -95.7129],
                    "CA": [56.1304, -106.3468], "Canada": [56.1304, -106.3468],
                    "GB": [55.3781, -3.4360], "UK": [55.3781, -3.4360],
                    "AU": [-25.2744, 133.7751], "Australia": [-25.2744, 133.7751],
                    "DE": [51.1657, 10.4515], "Germany": [51.1657, 10.4515],
                    "FR": [46.2276, 2.2137], "France": [46.2276, 2.2137],
                    "JP": [36.2048, 138.2529], "Japan": [36.2048, 138.2529],
                    "IN": [20.5937, 78.9629], "India": [20.5937, 78.9629],
                    "CN": [35.8617, 104.1954], "China": [35.8617, 104.1954],
                    "BR": [-14.2350, -51.9253], "Brazil": [-14.2350, -51.9253],
                    "MX": [23.6345, -102.5528], "Mexico": [23.6345, -102.5528],
                    "IT": [41.8719, 12.5674], "Italy": [41.8719, 12.5674],
                    "ES": [40.4637, -3.7492], "Spain": [40.4637, -3.7492],
                    "NL": [52.1326, 5.2913], "Netherlands": [52.1326, 5.2913],
                    "CH": [46.8182, 8.2275], "Switzerland": [46.8182, 8.2275],
                    "SE": [60.1282, 18.6435], "Sweden": [60.1282, 18.6435],
                    "NO": [60.4720, 8.4689], "Norway": [60.4720, 8.4689],
                    "DK": [56.2639, 9.5018], "Denmark": [56.2639, 9.5018],
                    "NZ": [-40.9006, 174.8860], "New Zealand": [-40.9006, 174.8860],
                    "IE": [53.1424, -7.6921], "Ireland": [53.1424, -7.6921],
                    "ZA": [-30.5595, 22.9375], "South Africa": [-30.5595, 22.9375]
                }
                
                # US State Coordinates (Approximate Centers)
                state_coords = {
                    'AL': [32.806671, -86.791130], 'AK': [61.370716, -152.404419], 'AZ': [33.729759, -111.431221],
                    'AR': [34.969704, -92.373123], 'CA': [36.116203, -119.681564], 'CO': [39.059811, -105.311104],
                    'CT': [41.597782, -72.755371], 'DE': [39.318523, -75.507141], 'FL': [27.766279, -81.686783],
                    'GA': [33.040619, -83.643074], 'HI': [21.094318, -157.498337], 'ID': [44.240459, -114.478828],
                    'IL': [40.349457, -88.986137], 'IN': [39.849426, -86.258278], 'IA': [42.011539, -93.210526],
                    'KS': [38.526600, -96.726486], 'KY': [37.668140, -84.670067], 'LA': [31.169546, -91.867805],
                    'ME': [44.693947, -69.381927], 'MD': [39.063946, -76.802101], 'MA': [42.230171, -71.530106],
                    'MI': [43.326618, -84.536095], 'MN': [45.694454, -93.900192], 'MS': [32.741646, -89.678696],
                    'MO': [38.456085, -92.288368], 'MT': [46.921925, -110.454353], 'NE': [41.125370, -98.268082],
                    'NV': [38.313515, -117.055374], 'NH': [43.452492, -71.563896], 'NJ': [40.298904, -74.521011],
                    'NM': [34.840515, -106.248482], 'NY': [42.165726, -74.948051], 'NC': [35.630066, -79.806419],
                    'ND': [47.528912, -99.784012], 'OH': [40.388783, -82.764915], 'OK': [35.565342, -96.928917],
                    'OR': [44.572021, -122.070938], 'PA': [41.203323, -77.194527], 'RI': [41.680893, -71.511780],
                    'SC': [33.856892, -80.945007], 'SD': [44.299782, -99.438828], 'TN': [35.747845, -86.692345],
                    'TX': [31.054487, -97.563461], 'UT': [40.150032, -111.862434], 'VT': [44.045876, -72.710686],
                    'VA': [37.769337, -78.169968], 'WA': [47.400902, -121.490494], 'WV': [38.491226, -80.954453],
                    'WI': [44.268543, -89.616508], 'WY': [42.755966, -107.302490]
                }
                
                # Logic to determine if we show Global or State map
                show_states_map = False
                if is_us_present:
                     if st.checkbox("🇺🇸 Show US States on Map (with labels)", key="map_toggle"):
                         show_states_map = True

                if show_states_map:
                    # 1. Query State Data
                    query_states_map = f"""
                        SELECT g.State, COUNT(f.EventID) as EventCount
                        FROM FactAdverseEvents f
                        JOIN DimTime t ON f.TimeKey = t.TimeKey
                        JOIN DimGeography g ON f.GeographyKey = g.GeographyKey
                        WHERE g.Country IN ('US', 'USA')
                    """
                    if selected_year != "All":
                        query_states_map += " AND t.Year = ?"
                        
                    query_states_map += " GROUP BY g.State"
                    
                    df_states_map = pd.read_sql_query(query_states_map, conn, params=params)
                    
                    # 2. Map Coordinates
                    def get_state_coords(code):
                        # Some preprocessing, e.g., trim whitespace
                        code = str(code).strip()
                        return state_coords.get(code, [None, None])

                    df_states_map['lat'] = df_states_map['State'].apply(lambda x: get_state_coords(x)[0])
                    df_states_map['lon'] = df_states_map['State'].apply(lambda x: get_state_coords(x)[1])
                    df_states_map = df_states_map.dropna(subset=['lat', 'lon'])
                    
                    # 3. Render with PyDeck (Scatter + Text)
                    if not df_states_map.empty:
                        # Normalize size for display
                        # PyDeck needs a radius. A simple multiplier is used here.
                        # Using logarithmic scale for visual comfort if counts vary wildly
                        df_states_map['radius'] = df_states_map['EventCount'].apply(lambda x: x * 5000 if x < 100 else x * 200)

                        layer_scatter = pdk.Layer(
                            "ScatterplotLayer",
                            df_states_map,
                            get_position='[lon, lat]',
                            get_color='[200, 30, 0, 160]',
                            get_radius='radius',
                            pickable=True,
                        )
                        
                        # Add Text labels (State Name + Count)
                        df_states_map['label'] = df_states_map['State']  # + "\n" + df_states_map['EventCount'].astype(str)
                        
                        layer_text = pdk.Layer(
                            "TextLayer",
                            df_states_map,
                            get_position='[lon, lat]',
                            get_text='label',
                            get_color=[0, 0, 0, 200],
                            get_size=16,
                            get_alignment_baseline="'bottom'",
                        )
                        
                        view_state = pdk.ViewState(latitude=37.0902, longitude=-95.7129, zoom=3, pitch=0)
                        
                        r = pdk.Deck(
                            layers=[layer_scatter, layer_text], 
                            initial_view_state=view_state,
                            tooltip={"text": "{State}: {EventCount} Events"}
                        )
                        st.pydeck_chart(r)
                    else:
                        st.warning("No mappable US State data found.")

                else:
                    # GLOBAL MAP LOGIC (Existing)
                    def get_coords(code):
                        return country_coords.get(code, [None, None])

                    df_geo['lat'] = df_geo['Country'].apply(lambda x: get_coords(x)[0])
                    df_geo['lon'] = df_geo['Country'].apply(lambda x: get_coords(x)[1])
                    df_map = df_geo.dropna(subset=['lat', 'lon'])
                    
                    if not df_map.empty:
                        st.map(df_map, latitude='lat', longitude='lon', size='EventCount', zoom=1)
                    else:
                        st.warning("Could not map country codes to coordinates.")
                        unmapped = df_geo[df_geo['lat'].isna()]['Country'].unique()
                        if len(unmapped) > 0:
                             st.code(f"Found unmapped countries: {', '.join(map(str, unmapped))}")

        else:
            st.info("No geographic data available.")

    with c4:
        st.subheader("Weight Analysis: Impact on Outcome")
        # Correlating Weight to Outcome Severity (Average Weight of Animal per Severity)
        query_weight = f"""
            SELECT o.SeverityLevel, AVG(f.Weight) as AvgWeight
            FROM FactAdverseEvents f
            JOIN DimTime t ON f.TimeKey = t.TimeKey
            JOIN DimOutcome o ON f.OutcomeKey = o.OutcomeKey
            WHERE f.Weight > 0 
            {where_clause.replace('WHERE', 'AND') if where_clause else ''}
            GROUP BY o.SeverityLevel
        """
        # Note: If where_clause was empty, we need to handle the WHERE/AND logic correctly.
        # Simpler approach: Rebuild query specific to this block or rely on string manip (fragile).
        # Let's rebuild properly.
        
        wb_base = "WHERE f.Weight > 0"
        wb_params = []
        if selected_year != "All":
            wb_base += " AND t.Year = ?"
            wb_params = [selected_year]
            
        final_weight_query = f"""
            SELECT o.SeverityLevel, AVG(f.Weight) as AvgWeightkgs
            FROM FactAdverseEvents f
            JOIN DimTime t ON f.TimeKey = t.TimeKey
            JOIN DimOutcome o ON f.OutcomeKey = o.OutcomeKey
            {wb_base}
            GROUP BY o.SeverityLevel
        """
        
        df_weight = pd.read_sql_query(final_weight_query, conn, params=wb_params)
        if not df_weight.empty:
            # Bar chart for Avg Weight
            st.bar_chart(df_weight.set_index('SeverityLevel'))
            st.caption("Average weight (kg) of animals by outcome severity.")
        else:
            st.info("No weight data available.")

    st.markdown("---")
    
    # Row 3: Species & Drug Analysis
    st.subheader("Species Impact by Drug (Top 10 Drugs)")
    query_drugs_species = f"""
        SELECT 
            d.BrandName, 
            a.Species, 
            COUNT(f.EventID) as EventCount
        FROM FactAdverseEvents f
        JOIN DimDrug d ON f.DrugKey = d.DrugKey
        JOIN DimAnimal a ON f.AnimalKey = a.AnimalKey
        JOIN DimTime t ON f.TimeKey = t.TimeKey
        {where_clause}
        GROUP BY d.BrandName, a.Species
    """
    
    df_ds = pd.read_sql_query(query_drugs_species, conn, params=params)
    
    if not df_ds.empty:
        # Pivot the data: Index=Drug, Columns=Species, Values=Count
        pivot_ds = df_ds.pivot(index='BrandName', columns='Species', values='EventCount').fillna(0)
        
        # Calculate totals to find Top 10
        pivot_ds['Total'] = pivot_ds.sum(axis=1)
        pivot_ds = pivot_ds.sort_values('Total', ascending=False).head(10)
        pivot_ds = pivot_ds.drop(columns=['Total'])
        
        st.bar_chart(pivot_ds)
        st.caption("Distribution of adverse events across species for the most reported drugs.")
    else:
        st.info("No data available for species/drug analysis.")

    # --- Data Grid ---
    with st.expander("View Raw Data Report (Operational Safety Report)"):
        st.subheader("Critical Outcomes by Drug")
        query_critical_drug = f"""
            SELECT 
                d.BrandName, 
                a.Species,
                COUNT(f.EventID) as DeathCount
            FROM FactAdverseEvents f
            JOIN DimTime t ON f.TimeKey = t.TimeKey
            JOIN DimDrug d ON f.DrugKey = d.DrugKey
            JOIN DimAnimal a ON f.AnimalKey = a.AnimalKey
            JOIN DimOutcome o ON f.OutcomeKey = o.OutcomeKey
            WHERE o.OutcomeName IN ('Died', 'Euthanized')
            """
        if selected_year != "All":
            query_critical_drug += " AND t.Year = ?"
            
        query_critical_drug += """
            GROUP BY d.BrandName, a.Species
            ORDER BY DeathCount DESC
            LIMIT 50
        """
        df_report = pd.read_sql_query(query_critical_drug, conn, params=params)
        st.dataframe(df_report)

    conn.close()

if __name__ == "__main__":
    main()
