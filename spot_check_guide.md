# CES Budgets Spot Check Guide

This guide helps you verify that the budget calculations are correct before and after running the fix script.

## Pre-Execution Checks

### 1. Verify Year 1 Totals Match Annual Generation

Run the script in **Mode 1 (Analyze)** to see sample calculations:

```bash
python ces_budgets_fix.py
# Select mode 1
```

Check the validation report output - it shows sample calculations for the first 5 sites.

### 2. Verify Degradation Formula

The script already includes a spot check. You can also manually verify:

**Expected values for a site with 2348.25 kWh annual generation:**

- **Year 1**: 2348.25 kWh (no degradation)
- **Year 2**: 2348.25 × 0.996 = 2338.86 kWh
- **Year 3**: 2348.25 × 0.992016 = 2329.52 kWh

## Post-Execution Checks

### 1. Check Commissioning-Month-to-Commissioning-Month Totals

**Important:** As Maria mentioned, totals should be checked from commissioning month to commissioning month, not calendar year to calendar year.

#### SQL Query for a Specific Site

```sql
-- Replace 'SITE_ID' with actual site ID
-- Replace '2019-03' with commissioning year-month

WITH site_data AS (
  SELECT 
    site_id,
    year,
    month,
    generation,
    -- Calculate running total from commissioning month
    SUM(generation) OVER (
      PARTITION BY site_id 
      ORDER BY year, month 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total
  FROM site_budgets
  WHERE site_id = 'SITE_ID'
  ORDER BY year, month
)
SELECT 
  year,
  month,
  generation,
  running_total
FROM site_data
WHERE year >= 2019  -- Commissioning year
ORDER BY year, month
LIMIT 15;  -- First 12-15 months to see commissioning year window
```

#### Check Annual Totals (Commissioning Month to Commissioning Month)

```sql
-- For a site commissioned in March 2019:
-- Year 1 = March 2019 to February 2020
-- Year 2 = March 2020 to February 2021
-- etc.

WITH commissioning_window AS (
  SELECT 
    site_id,
    year,
    month,
    generation,
    -- Create a "budget year" that starts from commissioning month
    CASE 
      WHEN month >= 3 THEN year  -- March onwards = same year
      ELSE year - 1              -- Jan-Feb = previous year
    END as budget_year
  FROM site_budgets
  WHERE site_id = 'SITE_ID'
)
SELECT 
  budget_year,
  COUNT(*) as months,
  SUM(generation) as total_generation,
  ROUND(SUM(generation), 2) as total_rounded
FROM commissioning_window
WHERE budget_year >= 2019
GROUP BY budget_year
ORDER BY budget_year
LIMIT 5;
```

### 2. Verify Degradation is Applied Correctly

```sql
-- Check Year 1, Year 2, Year 3 totals for a site
-- This assumes commissioning in 2019

SELECT 
  year,
  COUNT(*) as months,
  SUM(generation) as total_generation,
  ROUND(SUM(generation), 2) as total_rounded
FROM site_budgets
WHERE site_id = 'SITE_ID'
  AND year BETWEEN 2019 AND 2021
GROUP BY year
ORDER BY year;
```

**Expected results:**
- Year 2019: ~2348.25 kWh (Year 1, no degradation)
- Year 2020: ~2338.86 kWh (Year 2, × 0.996)
- Year 2021: ~2329.52 kWh (Year 3, × 0.992016)

**Verify the ratio:**
```sql
WITH yearly_totals AS (
  SELECT 
    year,
    SUM(generation) as total
  FROM site_budgets
  WHERE site_id = 'SITE_ID'
  GROUP BY year
)
SELECT 
  y1.year as year1,
  y1.total as year1_total,
  y2.year as year2,
  y2.total as year2_total,
  ROUND(y2.total / y1.total, 6) as degradation_factor,
  CASE 
    WHEN ABS(y2.total / y1.total - 0.996) < 0.0001 THEN '✓ Correct'
    ELSE '✗ Wrong'
  END as check
FROM yearly_totals y1
JOIN yearly_totals y2 ON y2.year = y1.year + 1
WHERE y1.year = 2019;  -- Adjust for commissioning year
```

### 3. Verify Monthly Profile Shape

Check that the monthly distribution matches CES profile (high in summer, low in winter):

```sql
-- Get Year 1 monthly breakdown for a site
SELECT 
  month,
  generation,
  ROUND(generation * 100.0 / SUM(generation) OVER (), 2) as percentage
FROM site_budgets
WHERE site_id = 'SITE_ID'
  AND year = 2019  -- Year 1 (commissioning year)
ORDER BY month;
```

**Expected percentages (CES profile):**
- January: ~2.85%
- February: ~5.88%
- March: ~8.01%
- April: ~11.77%
- May: ~14.67%
- June: ~11.52%
- July: ~12.30%
- August: ~12.10%
- September: ~9.57%
- October: ~6.03%
- November: ~2.78%
- December: ~2.51%

### 4. Spot Check Multiple Sites

```sql
-- Get a sample of sites to check
SELECT 
  s.name as site_name,
  s.id as site_id,
  COUNT(sb.*) as budget_records,
  MIN(sb.year) as first_year,
  MAX(sb.year) as last_year,
  SUM(CASE WHEN sb.year = MIN(sb.year) THEN sb.generation ELSE 0 END) as year1_total
FROM sites s
JOIN site_budgets sb ON sb.site_id = s.id
JOIN accounts a ON a.id = s.organization_id
WHERE a.name ILIKE '%Community Energy Scheme%'
GROUP BY s.id, s.name
ORDER BY s.name
LIMIT 10;
```

### 5. Compare Before/After Totals

If you have the backup file, you can compare:

```sql
-- Count records before and after
SELECT 
  'Before' as period,
  COUNT(*) as total_records,
  COUNT(DISTINCT site_id) as sites,
  SUM(generation) as total_generation
FROM site_budgets_backup  -- If you imported backup to a temp table
UNION ALL
SELECT 
  'After' as period,
  COUNT(*) as total_records,
  COUNT(DISTINCT site_id) as sites,
  SUM(generation) as total_generation
FROM site_budgets
WHERE site_id IN (
  SELECT DISTINCT site_id FROM site_budgets_backup
);
```

## Quick Validation Script

You can also run this Python snippet to validate a few sites:

```python
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT', 5432)),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

# Get a sample site
with conn.cursor() as cur:
    cur.execute("""
        SELECT s.id, s.name, sb.year, SUM(sb.generation) as total
        FROM sites s
        JOIN site_budgets sb ON sb.site_id = s.id
        JOIN accounts a ON a.id = s.organization_id
        WHERE a.name ILIKE '%Community Energy Scheme%'
        GROUP BY s.id, s.name, sb.year
        ORDER BY s.name, sb.year
        LIMIT 15
    """)
    
    for row in cur.fetchall():
        print(f"{row[1]} - Year {row[2]}: {row[3]:.2f} kWh")
```

## Manual UI Checks

1. **Pick 3-5 random sites** from the matched list
2. **Navigate to each site** in the Metris platform
3. **Check the budgets tab**:
   - Verify Year 1 total matches annual generation from Excel
   - Verify Year 2 is ~0.4% less than Year 1
   - Verify monthly shape (high summer, low winter)
   - Verify commissioning-month-to-commissioning-month totals

## Red Flags to Watch For

- ❌ Year 1 total doesn't match annual generation from Excel
- ❌ Year 2/3 degradation factor is not ~0.996 / ~0.992
- ❌ Monthly percentages don't match CES profile (especially May should be highest)
- ❌ Commissioning-month-to-commissioning-month totals don't match expected annual values
- ❌ Missing months or years for any site

## Expected Results Summary

For a site with **2348.25 kWh annual generation** commissioned in **March 2019**:

| Period | Expected Total | Notes |
|--------|----------------|-------|
| Year 1 (2019) | 2348.25 kWh | No degradation |
| Year 2 (2020) | 2338.86 kWh | × 0.996 |
| Year 3 (2021) | 2329.52 kWh | × 0.992016 |
| Mar 2019 - Feb 2020 | 2348.25 kWh | Commissioning year window |
| Mar 2020 - Feb 2021 | 2338.86 kWh | Year 2 window |
