"""
Veterinary Clinic Business Intelligence Dashboard
Interactive data visualization and reporting for adverse event analysis
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from bi.analytics import VeterinaryAnalytics

# Page configuration
st.set_page_config(
    page_title="Veterinary BI Dashboard",
    page_icon="🐾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize analytics
@st.cache_resource
def get_analytics():
    return VeterinaryAnalytics()

analytics = get_analytics()

# Sidebar navigation
st.sidebar.title("🐾 Veterinary BI System")
st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Navigation",
    ["📊 Overview", "🐕 Breed Analysis", "💊 Ingredient Analysis",
     "📍 Geographic Distribution", "⏱️ Reaction Timing", "🎯 Breeding Groups"]
)

# Main title
st.title("Veterinary Clinic Business Intelligence Dashboard")
st.markdown("### Decision Support System for Adverse Event Analysis")
st.markdown("---")

# =======================
# OVERVIEW PAGE
# =======================
if page == "📊 Overview":
    st.header("📊 System Overview")

    # Summary statistics
    stats = analytics.get_summary_statistics()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Events", f"{stats.get('total_events', 0):,}")
    with col2:
        st.metric("Unique Breeds", f"{stats.get('total_breeds', 0):,}")
    with col3:
        st.metric("Unique Reactions", f"{stats.get('total_reactions', 0):,}")
    with col4:
        st.metric("Active Ingredients", f"{stats.get('total_ingredients', 0):,}")

    st.markdown("---")

    # Two columns for charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 10 Most Common Reactions")
        reactions_df = pd.DataFrame(analytics.get_top_outcomes(10))
        if not reactions_df.empty:
            fig = px.bar(
                reactions_df,
                x='occurrence_count',
                y='outcome_name',
                orientation='h',
                text='percentage',
                labels={'occurrence_count': 'Count', 'outcome_name': 'Outcome'},
                color='occurrence_count',
                color_continuous_scale='Reds'
            )
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Reaction Timing Distribution")
        timing_df = pd.DataFrame(analytics.get_reaction_timing_distribution())
        if not timing_df.empty:
            fig = px.pie(
                timing_df,
                values='event_count',
                names='timing_category',
                title='Time Until Reaction Appears',
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Weight correlation
    st.subheader("Animal Weight vs Adverse Events")
    weight_df = pd.DataFrame(analytics.get_weight_reaction_correlation())
    if not weight_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name='Event Count',
            x=weight_df['weight_category'],
            y=weight_df['event_count'],
            marker_color='lightblue'
        ))
        fig.add_trace(go.Bar(
            name='Unique Reactions',
            x=weight_df['weight_category'],
            y=weight_df['unique_reactions'],
            marker_color='coral'
        ))
        fig.update_layout(
            barmode='group',
            xaxis_title='Weight Category',
            yaxis_title='Count',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

    # Gender and reproductive status
    st.subheader("Gender & Reproductive Status Analysis")
    gender_df = pd.DataFrame(analytics.get_gender_reproductive_analysis())
    if not gender_df.empty:
        gender_df = gender_df.head(10)  # Top 10 combinations
        fig = px.sunburst(
            gender_df,
            path=['gender', 'reproductive_status'],
            values='event_count',
            color='event_count',
            color_continuous_scale='Viridis'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

# =======================
# BREED ANALYSIS PAGE
# =======================
elif page == "🐕 Breed Analysis":
    st.header("🐕 Breed-Specific Analysis")

    # Get reactions by breed
    reactions_by_breed = pd.DataFrame(analytics.get_reactions_by_breed(200))

    if not reactions_by_breed.empty:
        # Breed selector
        breeds = reactions_by_breed[['breed_name', 'species']].drop_duplicates()
        breed_options = [f"{row['breed_name']} ({row['species']})" for _, row in breeds.iterrows()]

        selected_breed_str = st.selectbox("Select a breed to analyze:", breed_options)

        if selected_breed_str:
            breed_name = selected_breed_str.rsplit(' (', 1)[0]
            species = selected_breed_str.rsplit('(', 1)[1].rstrip(')')

            # Get breed risk profile
            profile = analytics.get_breed_risk_profile(breed_name, species)

            st.subheader(f"Risk Profile: {breed_name} ({species})")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Events", f"{profile['total_events']:,}")
            with col2:
                st.metric("Top Reactions", len(profile['top_reactions']))
            with col3:
                st.metric("Risky Ingredients", len(profile['risky_ingredients']))

            st.markdown("---")

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Most Common Reactions")
                reactions_df = pd.DataFrame(profile['top_reactions'])
                if not reactions_df.empty:
                    fig = px.bar(
                        reactions_df,
                        x='reaction_count',
                        y='reaction_name',
                        orientation='h',
                        text='percentage',
                        color='reaction_count',
                        color_continuous_scale='Reds'
                    )
                    fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                    fig.update_layout(showlegend=False, height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No reaction data available")

            with col2:
                st.subheader("Most Common Outcomes")
                outcomes_df = pd.DataFrame(profile['top_outcomes'])
                if not outcomes_df.empty:
                    fig = px.pie(
                        outcomes_df,
                        values='count',
                        names='outcome_name',
                        color_discrete_sequence=px.colors.sequential.RdBu
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No outcome data available")

            st.subheader("Risky Ingredients for This Breed")
            ingredients_df = pd.DataFrame(profile['risky_ingredients'])
            if not ingredients_df.empty:
                fig = px.bar(
                    ingredients_df,
                    x='count',
                    y='ingredient_name',
                    orientation='h',
                    color='count',
                    color_continuous_scale='Oranges'
                )
                fig.update_layout(showlegend=False, height=300)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No ingredient data available")

# =======================
# INGREDIENT ANALYSIS PAGE
# =======================
elif page == "💊 Ingredient Analysis":
    st.header("💊 Active Ingredient Analysis")
    st.markdown("### Ingredients Most Commonly Associated with Adverse Events")

    ingredients_df = pd.DataFrame(analytics.get_dangerous_ingredients(30))

    if not ingredients_df.empty:
        # Top ingredients chart
        fig = px.bar(
            ingredients_df.head(20),
            x='event_count',
            y='ingredient_name',
            orientation='h',
            text='event_count',
            color='unique_reactions',
            color_continuous_scale='Reds',
            labels={'event_count': 'Number of Events', 'unique_reactions': 'Unique Reactions'}
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(height=600, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Detailed Ingredient Data")

        # Display table
        st.dataframe(
            ingredients_df,
            use_container_width=True,
            height=400
        )
    else:
        st.warning("No ingredient data available")

# =======================
# GEOGRAPHIC DISTRIBUTION PAGE
# =======================
elif page == "📍 Geographic Distribution":
    st.header("📍 Geographic Distribution of Adverse Events")

    geo_df = pd.DataFrame(analytics.get_geographic_distribution())

    if not geo_df.empty:
        # Filter out unknown locations for visualization
        geo_known = geo_df[geo_df['state'] != 'Unknown'].copy()

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Events by State (Top 20)")
            top_states = geo_known.head(20)
            fig = px.bar(
                top_states,
                x='event_count',
                y='state',
                orientation='h',
                color='unique_reactions',
                color_continuous_scale='Blues',
                text='event_count'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(height=500, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Distribution by Country")
            country_summary = geo_df.groupby('country').agg({
                'event_count': 'sum',
                'unique_breeds': 'sum',
                'unique_reactions': 'sum'
            }).reset_index()

            fig = px.pie(
                country_summary,
                values='event_count',
                names='country',
                title='Events by Country',
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Geographic Data Table")
        st.dataframe(geo_df.head(50), use_container_width=True)

# =======================
# REACTION TIMING PAGE
# =======================
elif page == "⏱️ Reaction Timing":
    st.header("⏱️ Reaction Timing Analysis")
    st.markdown("### How Quickly Do Adverse Reactions Appear?")

    timing_df = pd.DataFrame(analytics.get_reaction_timing_distribution())

    if not timing_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Event Distribution by Timing")
            fig = px.bar(
                timing_df,
                x='timing_category',
                y='event_count',
                text='event_count',
                color='event_count',
                color_continuous_scale='Viridis'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Timing Statistics")
            # Create a nice table
            display_df = timing_df[['timing_category', 'event_count', 'avg_days']].copy()
            display_df.columns = ['Timing Category', 'Events', 'Avg Days']
            st.dataframe(display_df, use_container_width=True, height=400)

        # Detailed breakdown
        st.markdown("---")
        st.subheader("Detailed Timing Breakdown")
        st.dataframe(timing_df, use_container_width=True)

# =======================
# BREEDING GROUPS PAGE
# =======================
elif page == "🎯 Breeding Groups":
    st.header("🎯 Breeding Group & Purpose Analysis")
    st.markdown("### Analysis by Dog Breeding Categories")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("By Breeding Group")
        group_df = pd.DataFrame(analytics.get_breeding_group_analysis())

        if not group_df.empty:
            fig = px.bar(
                group_df,
                x='event_count',
                y='breeding_group',
                orientation='h',
                text='event_count',
                color='unique_reactions',
                color_continuous_scale='Teal'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(height=400, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(group_df, use_container_width=True)
        else:
            st.info("No breeding group data available")

    with col2:
        st.subheader("By Breeding Purpose")
        purpose_df = pd.DataFrame(analytics.get_breeding_purpose_analysis())

        if not purpose_df.empty:
            # Limit to top 10 for readability
            purpose_top = purpose_df.head(10)
            fig = px.bar(
                purpose_top,
                x='event_count',
                y='breeding_purpose',
                orientation='h',
                text='event_count',
                color='unique_reactions',
                color_continuous_scale='Purples'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(height=400, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(purpose_df, use_container_width=True)
        else:
            st.info("No breeding purpose data available")

# Footer
st.markdown("---")
st.markdown("**Veterinary Clinic Business Intelligence System** | Data Source: FDA Animal & Veterinary Adverse Events Database")
