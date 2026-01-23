"""
Generate Business Reports
Creates standard reports for veterinary clinic BI system
"""
from bi.reports import generate_all_reports

if __name__ == "__main__":
    print("Generating business reports...")
    reports = generate_all_reports()

    print("\nReports generated successfully:")
    for report_path in reports:
        print(f"  ✓ {report_path}")

    print("\nReports saved to 'reports/' directory")
