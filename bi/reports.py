"""
Business Reports Module
Generate formatted reports for veterinary clinic decision support
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .analytics import VeterinaryAnalytics


class ReportGenerator:
    """Generate business reports for veterinary clinic"""

    def __init__(self, analytics: VeterinaryAnalytics):
        self.analytics = analytics

    def generate_breed_safety_report(self, breed_name: str, species: str) -> str:
        """Generate a safety report for a specific breed"""
        profile = self.analytics.get_breed_risk_profile(breed_name, species)

        report = f"""
========================================
BREED SAFETY REPORT
========================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Breed: {breed_name}
Species: {species}
Total Adverse Events: {profile['total_events']}

========================================
TOP REACTIONS TO MONITOR
========================================
"""
        for i, reaction in enumerate(profile['top_reactions'][:10], 1):
            report += f"{i}. {reaction['reaction_name']}\n"
            report += f"   Events: {reaction['reaction_count']} ({reaction['percentage']}%)\n\n"

        report += """
========================================
MOST COMMON OUTCOMES
========================================
"""
        for i, outcome in enumerate(profile['top_outcomes'][:10], 1):
            report += f"{i}. {outcome['outcome_name']} - {outcome['count']} cases\n"

        report += """
========================================
HIGH-RISK ACTIVE INGREDIENTS
========================================
CAUTION: The following ingredients have been frequently
associated with adverse events in this breed:

"""
        if profile['risky_ingredients']:
            for i, ingredient in enumerate(profile['risky_ingredients'], 1):
                report += f"{i}. {ingredient['ingredient_name']} ({ingredient['count']} events)\n"
        else:
            report += "No specific ingredient data available.\n"

        report += """
========================================
RECOMMENDATIONS
========================================
1. Monitor animals closely after administering medications
   containing the above active ingredients
2. Be aware of the most common reactions for this breed
3. Educate pet owners about potential adverse effects
4. Report any new adverse events to FDA

========================================
"""
        return report

    def generate_ingredient_risk_report(self, limit: int = 50) -> str:
        """Generate a report on risky active ingredients"""
        ingredients = self.analytics.get_dangerous_ingredients(limit)

        report = f"""
========================================
ACTIVE INGREDIENT RISK ASSESSMENT
========================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

This report identifies active ingredients most commonly
associated with adverse events across all species.

========================================
TOP {min(limit, len(ingredients))} HIGH-RISK INGREDIENTS
========================================

"""
        for i, ing in enumerate(ingredients, 1):
            report += f"{i}. {ing['ingredient_name']}\n"
            report += f"   Total Events: {ing['event_count']}\n"
            report += f"   Unique Reactions: {ing['unique_reactions']}\n\n"

        report += """
========================================
CLINICAL RECOMMENDATIONS
========================================
1. Review patient history before prescribing medications
   with these ingredients
2. Consider alternative treatments when possible
3. Implement enhanced monitoring protocols
4. Maintain detailed records of adverse reactions
5. Collaborate with specialists for high-risk cases

========================================
"""
        return report

    def generate_executive_summary(self) -> str:
        """Generate an executive summary report"""
        stats = self.analytics.get_summary_statistics()
        top_reactions = self.analytics.get_top_outcomes(10)
        top_ingredients = self.analytics.get_dangerous_ingredients(10)
        timing = self.analytics.get_reaction_timing_distribution()

        report = f"""
========================================
EXECUTIVE SUMMARY
Veterinary Adverse Event Analysis
========================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

========================================
DATASET OVERVIEW
========================================
Total Adverse Events Analyzed: {stats.get('total_events', 0):,}
Unique Animal Breeds: {stats.get('total_breeds', 0):,}
Unique Adverse Reactions: {stats.get('total_reactions', 0):,}
Unique Outcomes: {stats.get('total_outcomes', 0):,}
Active Ingredients Tracked: {stats.get('total_ingredients', 0):,}
Geographic Locations: {stats.get('total_locations', 0):,}

========================================
KEY FINDINGS
========================================

1. MOST COMMON ADVERSE OUTCOMES
"""
        for i, outcome in enumerate(top_reactions[:5], 1):
            report += f"   {i}. {outcome['outcome_name']} - {outcome['occurrence_count']:,} cases ({outcome['percentage']}%)\n"

        report += """
2. HIGHEST RISK ACTIVE INGREDIENTS
"""
        for i, ing in enumerate(top_ingredients[:5], 1):
            report += f"   {i}. {ing['ingredient_name']} - {ing['event_count']:,} events\n"

        report += """
3. REACTION TIMING INSIGHTS
"""
        for t in timing[:5]:
            report += f"   {t['timing_category']}: {t['event_count']:,} events"
            if t['avg_days']:
                report += f" (avg {t['avg_days']} days)"
            report += "\n"

        report += """
========================================
STRATEGIC RECOMMENDATIONS
========================================

IMMEDIATE ACTIONS:
• Implement enhanced screening protocols for high-risk
  active ingredients identified in this analysis
• Train staff on early recognition of common adverse
  reactions
• Establish monitoring schedules based on reaction
  timing data

SHORT-TERM INITIATIVES:
• Develop breed-specific prescribing guidelines
• Create client education materials on adverse events
• Implement standardized adverse event reporting
  procedures

LONG-TERM STRATEGIES:
• Build predictive models for adverse event risk
• Integrate BI system with clinic management software
• Establish collaborative research partnerships
• Contribute to industry-wide adverse event databases

========================================
SYSTEM CAPABILITIES
========================================

This BI system enables veterinarians to:
✓ Query adverse event history by breed
✓ Identify high-risk active ingredients
✓ Analyze reaction patterns and timing
✓ Assess geographic risk distributions
✓ Compare breeding groups and purposes
✓ Generate breed-specific safety reports
✓ Support evidence-based prescribing decisions

========================================
DATA QUALITY NOTES
========================================

Events with Weight Data: {stats.get('events_with_weight', 0):,}
Events with Timing Data: {stats.get('events_with_timing', 0):,}

Note: Some records may contain masked (MSK) values
for drug names or manufacturers due to privacy or
security reasons. Analysis focuses on active
ingredients where available.

========================================
CONCLUSION
========================================

This Business Intelligence system provides veterinarians
with comprehensive, data-driven insights to support
safer prescribing decisions and improve patient outcomes.

Regular review of these reports and dashboards is
recommended to stay current with emerging adverse
event patterns.

========================================
"""
        return report

    def save_report(self, report: str, filename: str, output_dir: Path = None) -> Path:
        """Save report to file"""
        if output_dir is None:
            output_dir = Path("reports")
        output_dir.mkdir(parents=True, exist_ok=True)

        filepath = output_dir / filename
        filepath.write_text(report, encoding='utf-8')
        return filepath


def generate_all_reports(output_dir: Path = None) -> list[Path]:
    """Generate all standard reports"""
    analytics = VeterinaryAnalytics()
    generator = ReportGenerator(analytics)

    reports = []

    # Executive summary
    exec_report = generator.generate_executive_summary()
    reports.append(generator.save_report(
        exec_report,
        f"executive_summary_{datetime.now().strftime('%Y%m%d')}.txt",
        output_dir
    ))

    # Ingredient risk report
    ing_report = generator.generate_ingredient_risk_report(30)
    reports.append(generator.save_report(
        ing_report,
        f"ingredient_risk_{datetime.now().strftime('%Y%m%d')}.txt",
        output_dir
    ))

    return reports
