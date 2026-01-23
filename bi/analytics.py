"""
Business Intelligence Analytics Module
Provides analytical queries for veterinary clinic decision support
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .config import WAREHOUSE_DB


class VeterinaryAnalytics:
    """Analytics queries for veterinary BI system"""

    def __init__(self, db_path: Path = WAREHOUSE_DB):
        self.db_path = db_path

    def _execute_query(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute query and return results as list of dicts"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_reactions_by_breed(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get most common reactions for every breed"""
        query = """
        SELECT
            b.breed_name,
            b.species,
            r.reaction_name,
            COUNT(*) as reaction_count,
            ROUND(COUNT(*) * 100.0 / breed_totals.total_events, 2) as percentage
        FROM fact_event e
        JOIN dim_breed b ON e.breed_key = b.breed_key
        JOIN bridge_event_reaction ber ON e.event_key = ber.event_key
        JOIN dim_reaction r ON ber.reaction_key = r.reaction_key
        JOIN (
            SELECT breed_key, COUNT(*) as total_events
            FROM fact_event
            GROUP BY breed_key
        ) breed_totals ON b.breed_key = breed_totals.breed_key
        GROUP BY b.breed_name, b.species, r.reaction_name
        ORDER BY b.breed_name, reaction_count DESC
        LIMIT ?
        """
        return self._execute_query(query, (limit,))

    def get_top_reactions_by_breed(self, breed_name: str, species: str, limit: int = 10) -> list[dict[str, Any]]:
        """Get top reactions for a specific breed"""
        query = """
        SELECT
            r.reaction_name,
            COUNT(*) as reaction_count,
            ROUND(COUNT(*) * 100.0 / breed_totals.total, 2) as percentage
        FROM fact_event e
        JOIN dim_breed b ON e.breed_key = b.breed_key
        JOIN bridge_event_reaction ber ON e.event_key = ber.event_key
        JOIN dim_reaction r ON ber.reaction_key = r.reaction_key
        JOIN (
            SELECT COUNT(*) as total
            FROM fact_event e2
            JOIN dim_breed b2 ON e2.breed_key = b2.breed_key
            WHERE b2.breed_name = ? AND b2.species = ?
        ) breed_totals
        WHERE b.breed_name = ? AND b.species = ?
        GROUP BY r.reaction_name
        ORDER BY reaction_count DESC
        LIMIT ?
        """
        return self._execute_query(query, (breed_name, species, breed_name, species, limit))

    def get_dangerous_ingredients(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get most common active ingredients causing side effects"""
        query = """
        SELECT
            ai.ingredient_name,
            COUNT(DISTINCT e.event_key) as event_count,
            COUNT(DISTINCT ber.reaction_key) as unique_reactions
        FROM dim_active_ingredient ai
        JOIN bridge_event_ingredient bei ON ai.ingredient_key = bei.ingredient_key
        JOIN fact_event e ON bei.event_key = e.event_key
        JOIN bridge_event_reaction ber ON e.event_key = ber.event_key
        JOIN dim_reaction r ON ber.reaction_key = r.reaction_key
        GROUP BY ai.ingredient_name
        ORDER BY event_count DESC
        LIMIT ?
        """
        return self._execute_query(query, (limit,))

    def get_weight_reaction_correlation(self) -> list[dict[str, Any]]:
        """Analyze correlation between animal weight and reactions/outcomes"""
        query = """
        SELECT
            CASE
                WHEN e.weight_kg IS NULL THEN 'Unknown'
                WHEN e.weight_kg < 5 THEN 'Very Small (<5kg)'
                WHEN e.weight_kg < 10 THEN 'Small (5-10kg)'
                WHEN e.weight_kg < 25 THEN 'Medium (10-25kg)'
                WHEN e.weight_kg < 50 THEN 'Large (25-50kg)'
                ELSE 'Very Large (>50kg)'
            END as weight_category,
            COUNT(DISTINCT e.event_key) as event_count,
            COUNT(DISTINCT ber.reaction_key) as unique_reactions,
            COUNT(DISTINCT beo.outcome_key) as unique_outcomes,
            ROUND(AVG(e.weight_kg), 2) as avg_weight_kg
        FROM fact_event e
        LEFT JOIN bridge_event_reaction ber ON e.event_key = ber.event_key
        LEFT JOIN bridge_event_outcome beo ON e.event_key = beo.event_key
        GROUP BY weight_category
        ORDER BY avg_weight_kg
        """
        return self._execute_query(query)

    def get_gender_reproductive_analysis(self) -> list[dict[str, Any]]:
        """Analyze correlation between gender/reproductive status and reactions/outcomes"""
        query = """
        SELECT
            COALESCE(e.sex, 'Unknown') as gender,
            COALESCE(e.reproductive_status, 'Unknown') as reproductive_status,
            COUNT(DISTINCT e.event_key) as event_count,
            COUNT(DISTINCT ber.reaction_key) as unique_reactions,
            COUNT(DISTINCT beo.outcome_key) as unique_outcomes
        FROM fact_event e
        LEFT JOIN bridge_event_reaction ber ON e.event_key = ber.event_key
        LEFT JOIN bridge_event_outcome beo ON e.event_key = beo.event_key
        GROUP BY gender, reproductive_status
        ORDER BY event_count DESC
        """
        return self._execute_query(query)

    def get_geographic_distribution(self) -> list[dict[str, Any]]:
        """Get geographic distribution of adverse events"""
        query = """
        SELECT
            COALESCE(g.state, 'Unknown') as state,
            COALESCE(g.country, 'Unknown') as country,
            COUNT(DISTINCT e.event_key) as event_count,
            COUNT(DISTINCT e.breed_key) as unique_breeds,
            COUNT(DISTINCT ber.reaction_key) as unique_reactions
        FROM fact_event e
        LEFT JOIN dim_geo g ON e.geo_key = g.geo_key
        LEFT JOIN bridge_event_reaction ber ON e.event_key = ber.event_key
        GROUP BY state, country
        ORDER BY event_count DESC
        """
        return self._execute_query(query)

    def get_reaction_timing_distribution(self) -> list[dict[str, Any]]:
        """Analyze how many days it takes for reactions to appear"""
        query = """
        SELECT
            CASE
                WHEN e.days_to_reaction IS NULL THEN 'Unknown'
                WHEN e.days_to_reaction = 0 THEN 'Same Day'
                WHEN e.days_to_reaction <= 3 THEN '1-3 Days'
                WHEN e.days_to_reaction <= 7 THEN '4-7 Days'
                WHEN e.days_to_reaction <= 14 THEN '1-2 Weeks'
                WHEN e.days_to_reaction <= 30 THEN '2-4 Weeks'
                ELSE 'Over 1 Month'
            END as timing_category,
            COUNT(*) as event_count,
            ROUND(AVG(e.days_to_reaction), 2) as avg_days,
            MIN(e.days_to_reaction) as min_days,
            MAX(e.days_to_reaction) as max_days
        FROM fact_event e
        GROUP BY timing_category
        ORDER BY avg_days
        """
        return self._execute_query(query)

    def get_breeding_group_analysis(self) -> list[dict[str, Any]]:
        """Analyze adverse events by dog breeding groups"""
        query = """
        SELECT
            COALESCE(b.group_name, 'Unknown') as breeding_group,
            COUNT(DISTINCT e.event_key) as event_count,
            COUNT(DISTINCT b.breed_key) as breed_count,
            COUNT(DISTINCT ber.reaction_key) as unique_reactions,
            COUNT(DISTINCT beo.outcome_key) as unique_outcomes
        FROM fact_event e
        JOIN dim_breed b ON e.breed_key = b.breed_key
        LEFT JOIN bridge_event_reaction ber ON e.event_key = ber.event_key
        LEFT JOIN bridge_event_outcome beo ON e.event_key = beo.event_key
        WHERE b.species = 'dog' AND b.source = 'thedogapi'
        GROUP BY breeding_group
        ORDER BY event_count DESC
        """
        return self._execute_query(query)

    def get_breeding_purpose_analysis(self) -> list[dict[str, Any]]:
        """Analyze adverse events by dog breeding purpose"""
        query = """
        SELECT
            COALESCE(b.purpose, 'Unknown') as breeding_purpose,
            COUNT(DISTINCT e.event_key) as event_count,
            COUNT(DISTINCT b.breed_key) as breed_count,
            COUNT(DISTINCT ber.reaction_key) as unique_reactions
        FROM fact_event e
        JOIN dim_breed b ON e.breed_key = b.breed_key
        LEFT JOIN bridge_event_reaction ber ON e.event_key = ber.event_key
        WHERE b.species = 'dog' AND b.source = 'thedogapi'
        GROUP BY breeding_purpose
        ORDER BY event_count DESC
        """
        return self._execute_query(query)

    def get_top_outcomes(self, limit: int = 15) -> list[dict[str, Any]]:
        """Get most common outcomes"""
        query = """
        SELECT
            o.outcome_name,
            COUNT(*) as occurrence_count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bridge_event_outcome), 2) as percentage
        FROM bridge_event_outcome beo
        JOIN dim_outcome o ON beo.outcome_key = o.outcome_key
        GROUP BY o.outcome_name
        ORDER BY occurrence_count DESC
        LIMIT ?
        """
        return self._execute_query(query, (limit,))

    def get_summary_statistics(self) -> dict[str, Any]:
        """Get overall summary statistics"""
        query = """
        SELECT
            (SELECT COUNT(*) FROM fact_event) as total_events,
            (SELECT COUNT(*) FROM dim_breed) as total_breeds,
            (SELECT COUNT(*) FROM dim_reaction) as total_reactions,
            (SELECT COUNT(*) FROM dim_outcome) as total_outcomes,
            (SELECT COUNT(*) FROM dim_active_ingredient) as total_ingredients,
            (SELECT COUNT(DISTINCT geo_key) FROM fact_event WHERE geo_key IS NOT NULL) as total_locations,
            (SELECT COUNT(*) FROM fact_event WHERE weight_kg IS NOT NULL) as events_with_weight,
            (SELECT COUNT(*) FROM fact_event WHERE days_to_reaction IS NOT NULL) as events_with_timing
        """
        results = self._execute_query(query)
        return results[0] if results else {}

    def get_breed_risk_profile(self, breed_name: str, species: str) -> dict[str, Any]:
        """Get comprehensive risk profile for a specific breed"""
        # Event count
        event_query = """
        SELECT COUNT(*) as event_count
        FROM fact_event e
        JOIN dim_breed b ON e.breed_key = b.breed_key
        WHERE b.breed_name = ? AND b.species = ?
        """
        events = self._execute_query(event_query, (breed_name, species))

        # Top reactions
        reactions = self.get_top_reactions_by_breed(breed_name, species, 5)

        # Top outcomes
        outcome_query = """
        SELECT o.outcome_name, COUNT(*) as count
        FROM fact_event e
        JOIN dim_breed b ON e.breed_key = b.breed_key
        JOIN bridge_event_outcome beo ON e.event_key = beo.event_key
        JOIN dim_outcome o ON beo.outcome_key = o.outcome_key
        WHERE b.breed_name = ? AND b.species = ?
        GROUP BY o.outcome_name
        ORDER BY count DESC
        LIMIT 5
        """
        outcomes = self._execute_query(outcome_query, (breed_name, species))

        # Top ingredients
        ingredient_query = """
        SELECT ai.ingredient_name, COUNT(*) as count
        FROM fact_event e
        JOIN dim_breed b ON e.breed_key = b.breed_key
        JOIN bridge_event_ingredient bei ON e.event_key = bei.event_key
        JOIN dim_active_ingredient ai ON bei.ingredient_key = ai.ingredient_key
        WHERE b.breed_name = ? AND b.species = ?
        GROUP BY ai.ingredient_name
        ORDER BY count DESC
        LIMIT 5
        """
        ingredients = self._execute_query(ingredient_query, (breed_name, species))

        return {
            "breed_name": breed_name,
            "species": species,
            "total_events": events[0]["event_count"] if events else 0,
            "top_reactions": reactions,
            "top_outcomes": outcomes,
            "risky_ingredients": ingredients,
        }
