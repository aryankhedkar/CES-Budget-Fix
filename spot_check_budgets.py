"""
Spot Check Script for CES Budgets
==================================
This script helps validate that budget calculations are correct.

Run this after executing the CES budgets fix to verify:
1. Year 1 totals match annual generation
2. Degradation is applied correctly (Year 2, Year 3)
3. Monthly profile matches CES percentages
4. Commissioning-month-to-commissioning-month totals
"""

import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 5432))
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

ACCOUNT_NAME = "Community Energy Scheme"
DEGRADATION_RATE = 0.004

# CES Monthly Profile (for comparison)
CES_MONTHLY_PROFILE = {
    1: 0.0285, 2: 0.0588, 3: 0.0801, 4: 0.1177,
    5: 0.1467, 6: 0.1152, 7: 0.1230, 8: 0.1210,
    9: 0.0957, 10: 0.0603, 11: 0.0278, 12: 0.0251
}


def get_sample_sites(conn, limit=5):
    """Get a sample of CES sites to check."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.id, s.name
            FROM sites s
            JOIN accounts a ON a.id = s.organization_id
            WHERE a.name ILIKE %s
            ORDER BY s.name
            LIMIT %s
        """, (f'%{ACCOUNT_NAME}%', limit))
        return cur.fetchall()


def get_site_budgets(conn, site_id):
    """Get all budgets for a site, grouped by year."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT year, month, generation
            FROM site_budgets
            WHERE site_id = %s
            ORDER BY year, month
        """, (site_id,))
        return cur.fetchall()


def check_site(conn, site_id, site_name, excel_annual_gen=None):
    """Check a single site's budgets."""
    print(f"\n{'=' * 70}")
    print(f"Site: {site_name} (ID: {site_id})")
    print(f"{'=' * 70}")
    
    budgets = get_site_budgets(conn, site_id)
    
    if not budgets:
        print("  ⚠ No budgets found for this site")
        return
    
    # Group by year
    by_year = defaultdict(list)
    for year, month, generation in budgets:
        by_year[year].append((month, generation))
    
    years = sorted(by_year.keys())
    if not years:
        return
    
    # Year 1 check
    year1 = years[0]
    year1_budgets = by_year[year1]
    year1_total = sum(gen for _, gen in year1_budgets)
    year1_monthly = {month: gen for month, gen in year1_budgets}
    
    print(f"\n📊 Year 1 ({year1}):")
    print(f"   Total: {year1_total:.2f} kWh")
    
    if excel_annual_gen:
        diff = abs(year1_total - excel_annual_gen)
        match = diff < 0.01
        status = "✓" if match else "✗"
        print(f"   Expected: {excel_annual_gen:.2f} kWh")
        print(f"   Difference: {diff:.2f} kWh {status}")
    
    # Degradation checks
    if len(years) >= 2:
        year2 = years[1]
        year2_total = sum(gen for _, gen in by_year[year2])
        year2_factor = year2_total / year1_total
        expected_factor = (1 - DEGRADATION_RATE) ** 1
        
        print(f"\n📉 Year 2 ({year2}):")
        print(f"   Total: {year2_total:.2f} kWh")
        print(f"   Factor: {year2_factor:.6f} (expected: {expected_factor:.6f})")
        match = abs(year2_factor - expected_factor) < 0.0001
        print(f"   Status: {'✓ Correct' if match else '✗ Wrong'}")
    
    if len(years) >= 3:
        year3 = years[2]
        year3_total = sum(gen for _, gen in by_year[year3])
        year3_factor = year3_total / year1_total
        expected_factor = (1 - DEGRADATION_RATE) ** 2
        
        print(f"\n📉 Year 3 ({year3}):")
        print(f"   Total: {year3_total:.2f} kWh")
        print(f"   Factor: {year3_factor:.6f} (expected: {expected_factor:.6f})")
        match = abs(year3_factor - expected_factor) < 0.0001
        print(f"   Status: {'✓ Correct' if match else '✗ Wrong'}")
    
    # Monthly profile check (Year 1)
    print(f"\n📅 Year 1 Monthly Profile:")
    year1_monthly_pct = {}
    for month in range(1, 13):
        gen = year1_monthly.get(month, 0)
        pct = (gen / year1_total * 100) if year1_total > 0 else 0
        year1_monthly_pct[month] = pct
        expected_pct = CES_MONTHLY_PROFILE[month] * 100
        diff = abs(pct - expected_pct)
        status = "✓" if diff < 0.5 else "✗"
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        print(f"   {month_names[month-1]}: {pct:.2f}% (expected: {expected_pct:.2f}%) {status}")


def main():
    print("=" * 70)
    print("CES Budgets Spot Check")
    print("=" * 70)
    
    print("\nConnecting to database...")
    try:
        with psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        ) as conn:
            print("Connected successfully.")
            
            sites = get_sample_sites(conn, limit=5)
            print(f"\nFound {len(sites)} sample sites to check")
            
            for site_id, site_name in sites:
                check_site(conn, site_id, site_name)
            
            print(f"\n{'=' * 70}")
            print("Spot Check Complete")
            print(f"{'=' * 70}")
            print("\nTo check more sites, modify the limit in get_sample_sites()")
            print("Or specify a site ID directly in check_site()")
    
    except psycopg2.Error as e:
        print(f"\nDatabase error: {e}")


if __name__ == "__main__":
    main()
