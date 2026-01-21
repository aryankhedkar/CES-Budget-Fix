# CES Budgets Fix Script

This script fixes the monthly budget profiles for **Community Energy Scheme (CES)** sites.

## Background

CES was originally onboarded using a generic PVGIS monthly profile instead of their own monthly generation profile. This means the **shape across months is wrong**, even if the annual total is roughly right.

This script re-calculates all CES site budgets using the correct CES monthly profile.

## What It Does

1. **Reads site data** from `Property Meter Directory.xlsx` - "Onboarding source sheet" (4,716 sites)
2. **Filters to sites in Metris** using "Sites on Metris" sheet from the same file (~4,414 sites)
3. **Matches sites** to database to get site IDs (by STO number)
4. **Calculates Year 1 monthly budgets** using CES's actual monthly profile (commissioning year, no degradation)
5. **Applies degradation** (0.4% per year) to Year 1 values for Years 2-25
6. **Backs up existing budgets** before deletion (safety)
7. **Deletes old and inserts new** budgets in batches
8. **Generates a validation report** for spot-checking

## CES Monthly Profile

The correct monthly generation split provided by CES:

| Month     | % of Annual |
|-----------|-------------|
| January   | 2.85%       |
| February  | 5.88%       |
| March     | 8.01%       |
| April     | 11.77%      |
| May       | 14.67%      |
| June      | 11.52%      |
| July      | 12.30%      |
| August    | 12.10%      |
| September | 9.57%       |
| October   | 6.03%       |
| November  | 2.78%       |
| December  | 2.51%       |

## Prerequisites

```bash
pip install psycopg2-binary python-dotenv openpyxl
```

## Setup

### 1. Create `.env` file

```
DB_HOST=your-database-host.com
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

### 2. Ensure Excel file is present

The script expects:
- `Property Meter Directory.xlsx` - Contains:
  - **Onboarding source sheet**: Site data (commissioning dates, annual generation)
  - **Sites on Metris sheet**: Authoritative list of sites that are in Metris

The "Sites on Metris" sheet is used to filter which sites should be processed (only sites that are actually in Metris).

## Usage

```bash
python ces_budgets_fix.py
```

### Available Modes

| Mode | Description |
|------|-------------|
| 1    | **Analyze only** - Check matching without any database changes |
| 2    | **Generate SQL** - Create SQL file for manual review and execution |
| 3    | **Execute with backup** - Run the fix with automatic backup (recommended) |
| 4    | **List DB sites** - Show all CES sites currently in database |
| 5    | **Validation report** - Generate detailed report without changes |

## Recommended Workflow

### Step 1: Analyze First

```
Enter mode (1-5): 1
```

This shows:
- How many sites will be matched
- Sites in Excel but not in DB
- Sites in DB but not in Excel

### Step 2: Generate SQL for Review (Optional)

```
Enter mode (1-5): 2
```

This creates `ces_budgets_fix.sql` that you can review before running.

### Step 3: Execute with Backup

```
Enter mode (1-5): 3
```

This will:
1. Create backup at `ces_budgets_backup.csv`
2. Ask for confirmation
3. Execute in batches (starting at 100, scaling to 500)
4. Generate validation report at `ces_budgets_validation_report.json`

## Output Files

| File | Description |
|------|-------------|
| `ces_budgets_backup.csv` | Snapshot of deleted budgets (for rollback) |
| `ces_budgets_fix.sql` | Generated SQL (mode 2) |
| `ces_budgets_validation_report.json` | Detailed validation report |

## Validation Checks

After running, verify:

1. **Annual totals match** - Sum of 12 months should equal annual design generation
2. **Commissioning year window** - Check commissioning-month-to-commissioning-month totals
3. **Spot check in UI** - Verify a handful of sites manually in the platform

The validation report includes sample calculations for the first 5 sites.

## Rollback

If something goes wrong, use the backup file:

```sql
-- Re-insert from backup
COPY site_budgets(site_id, year, month, generation, revenue, created_at, updated_at)
FROM '/path/to/ces_budgets_backup.csv'
WITH (FORMAT csv, HEADER true);
```

## Technical Details

- **Sites**: ~4,716 CES sites
- **Records per site**: 300 (12 months × 25 years)
- **Total records**: ~1.4 million
- **Degradation formula**: `Year N = Year 1 × (1 - 0.004)^(N-1)`
- **Batch processing**: Starts at 100, scales to 500 after stable batches
