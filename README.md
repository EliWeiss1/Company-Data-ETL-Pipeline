# Company Data Classification & Normalization Pipeline

An ETL pipeline that consolidates and normalizes 1,000+ company records from multiple Excel workbooks, automatically classifying name variants and standardizing URLs using SQL-based transformations.

## 🎯 Problem Solved

Companies often appear under multiple names (legal entities, brands, abbreviations, former names) with inconsistent URL formats. This pipeline:
- Deduplicates company records across workbooks
- Normalizes malformed URLs to standard formats
- Classifies each name variant (Ticker, Long/Short Version, FKA, Alternate Spelling, Brand)
- Validates data quality and identifies conflicts

## 🚀 Features

- **Multi-Workbook Consolidation** - Combines data from multiple Excel files
- **Intelligent URL Normalization** - Standardizes website and LinkedIn URLs
- **Rule-Based Classification** - Categorizes name variants using SQL CTEs and regex
- **Conflict Detection** - Identifies companies with inconsistent URLs
- **In-Memory Processing** - Uses DuckDB for fast SQL analytics on DataFrames

## 📋 Installation

```bash
pip install -r requirements.txt
```

## 📊 Expected Data Schema

The pipeline expects Excel files with the following columns:

| Column Name | Description | Example |
|------------|-------------|---------|
| Company ID | Unique identifier for each company | 1 |
| Company Name | Name variant | "Amazon.com Inc" |
| Company URL | Website URL (any format) | "WWW.AMAZON.COM/about" |
| Company LinkedIn page UR | LinkedIn URL (any format) | "linkedin.com/company/amazon/jobs" |

## 💻 Usage

1. Place your Excel workbooks in the same directory as the script
2. Update the workbook list in the code:
```python
excel_workbooks = ["YourFile1.xlsx", "YourFile2.xlsx"]
```
3. Run the pipeline:
```bash
python Company_Info_Normalization.py
```
4. Output: `Company_Name_Normalization.xlsx` with categorized, normalized data

## 🏗️ Pipeline Architecture
```
Excel Files → Pandas DataFrames → URL Normalization → DuckDB SQL Processing → Classification → Excel Output
```

### Processing Steps

1. **Extract** - Load multiple Excel workbooks into Pandas DataFrames
2. **Clean URLs** - Normalize website and LinkedIn URLs to standard formats
3. **Aggregate** - Consolidate valid URLs per Company ID
4. **Classify** - Categorize name variants using rule-based logic
5. **Validate** - Detect conflicting URLs for manual review
6. **Load** - Export cleaned, categorized data to Excel

## 🔍 Classification Logic

| Category | Rule | Example |
|----------|------|---------|
| **Ticker** | All uppercase | "AMZN" |
| **Long Version** | Contains legal/geographic suffixes | "Amazon.com Inc", "Amazon International" |
| **Short Version** | Substring of another variant | "Amazon" (when "Amazon Inc" exists) |
| **FKA** | Name doesn't match LinkedIn/website URL | "Whole Foods" (LinkedIn: amazon) |
| **Alternate Spelling** | Same letters, different punctuation | "DataCorp" vs "Data-Corp" |
| **Brand Name** | Primary brand name | "Amazon" |

### URL Normalization Examples

**Website URLs:**
```
Input:  "WWW.EXAMPLE.COM/about"
Output: "example.com"
```

**LinkedIn URLs:**
```
Input:  "https://www.LinkedIn.com/company/example-inc/jobs?filter=..."
Output: "linkedin.com/company/example-inc"
```

## 🛠️ Technical Highlights

- **SQL CTEs in DuckDB** - Complex joins and window functions for classification
- **Regex Normalization** - Removes special characters for name similarity matching
- **Pandas + DuckDB Integration** - Seamless SQL queries on DataFrames
- **Aggregate Functions** - Consolidates valid URLs per company ID
- **String Similarity Algorithms** - Substring matching and pattern detection
- **Modular Design** - Separate functions for URL cleaning and classification

## 📈 Sample Output

```
Company ID# | Name Variant        | Company URL  | LinkedIn URL              | Name Category
1           | AMZN                | amazon.com   | linkedin.com/company/amazon | Ticker
1           | Amazon              | amazon.com   | linkedin.com/company/amazon | Brand Name
1           | Amazon.com Inc      | amazon.com   | linkedin.com/company/amazon | Long Version
1           | Amazon Inc          | amazon.com   | linkedin.com/company/amazon | Long Version
```

## 🔍 Data Quality Checks

The pipeline identifies and prints conflicts to console:
- Companies with multiple different website URLs
- Companies with multiple different LinkedIn URLs

These require manual review to determine the correct URL.

## 🚧 Requirements

- Python 3.7+
- Valid "Company ID" column for grouping variants
- Column names: "Company Name", "Company URL", "Company LinkedIn page UR"
- Excel format (.xlsx or .xls)

## 📝 License

MIT License - See LICENSE file for details

## 👤 Author

**Eli Weiss**
- GitHub: https://github.com/EliWeiss1
- LinkedIn: https://www.linkedin.com/in/eli-weiss-40803a330/
- Email: eliisaweiss@gmail.com

---

⭐ If you find this project useful, please consider starring the repository!
