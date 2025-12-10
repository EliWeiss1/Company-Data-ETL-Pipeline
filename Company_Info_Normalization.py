import pandas as pd
from urllib.parse import urlparse

import duckdb

#Allow for multiple Excel workbooks and combine all tables into one
excel_workbooks = ["CompanyNames.xlsx"]
excel_dataFrames = []

#convert all excel workbooks into dataFrames
for workbook in excel_workbooks:
    excel_df = pd.read_excel(workbook)
    excel_dataFrames.append(excel_df)

#Combine all dataFrames
df_raw_data = pd.concat(excel_dataFrames)


def clean_website_url(url):    #Desired Website Format: company.__
     #Check for Null URL
    if not url or not isinstance(url, str):
        return None
    
    url = url.lower().strip()

    parsed_url = urlparse(url)

    #Normalize URL so parser recognizes netloc
    if not parsed_url.netloc:
        parsed_url = urlparse('https://' + url)

    netloc = parsed_url.netloc

    #Remove www. from netloc (if there)
    if netloc.startswith('www.'):
        netloc = netloc[4:]


    #Check for invalid results
    if not netloc or '.' not in netloc:
        return None    

    return netloc
     

def clean_linkedin_url(url):    #Desired LinkedIn Format: (linkedin.com/company/__)
    #Check for Null URL
    if not url or not isinstance(url, str):
        return None

    #Convert to lowercase to ensure valid search
    url = url.strip().lower()

    #Check for a url that isn't linkedIn
    if 'linkedin.com/company/' not in url:
        return None

    #Ensure URL is set to home page (e.g. not the 'Jobs' section)

        #STEP 1 --> Extract section of URL after linkedin.com/company/
    try:
        company_name = url.split('company/', 1)[1]
    except IndexError:
        return None     #Invalid URL - nothing after 'company/'
    
        #STEP 2 --> Remove subsequent queries
    company_name = company_name.split('?', 1)[0].split('#', 1)[0].split('/', 1)[0]

    if not company_name:
        return None

    return f"linkedin.com/company/{company_name}"       
        
#Clean URLs in DataFrame
df_raw_data["Company LinkedIn page UR"] = df_raw_data["Company LinkedIn page UR"].apply(clean_linkedin_url)
df_raw_data["Company URL"] = df_raw_data["Company URL"].apply(clean_website_url)


#Replace invalid URLs and make each name variant its own row
aggregated_data = duckdb.query("""
    WITH collected_links AS (       -- Aggregate valid URLs per Company ID
        SELECT
            "Company ID",
            MIN("Company URL") FILTER (WHERE "Company URL" IS NOT NULL) AS Website,
            MIN("Company LinkedIn page UR") FILTER (WHERE "Company LinkedIn page UR" IS NOT NULL) AS LinkedIn
        FROM df_raw_data
        GROUP BY "Company ID"
    ),
                                    --Ensure each row is distinct name variant
    distinct_names AS (             
        SELECT DISTINCT
            "Company ID",
            "Company Name",
        FROM df_raw_data)
                                    --Put everything together
    SELECT
        dn."Company ID",
        dn."Company Name",
        cl.Website AS "Company URL",
        cl.LinkedIn AS "Company LinkedIn page URL"
    FROM distinct_names AS dn
    LEFT JOIN collected_links AS cl
        ON dn."Company ID" = cl."Company ID"
    ORDER BY dn."Company ID", dn."Company Name"
""").to_df()




#Long name keywords
legal_suffixes = [' inc', ' llc', ' ltd', ' limited', ' plc', ' gmbh', ' corp', ' sa', ' ag']   #Leading space excludes names like 'zinc'
geographical_suffixes = ['international', 'holdings', 'global', 'technologies', 'solutions']
generic_suffixes = ['corporation', 'technologies', 'group', 'enterprise']

#Register as dataframe and duckdb table to use in name_categorization query
df_long_qualifiers = pd.DataFrame({"long_qualifiers": legal_suffixes + geographical_suffixes + generic_suffixes})
duckdb.register("long_name_qualifiers_table", df_long_qualifiers)


#Categorize all name variants
name_categorization = """
                              --Create normalized versions of the names to compare (for alternate spelling, short version, fka)
    WITH normalized_names AS (6
        SELECT 
            "Company ID", 
            "Company Name", 
            LOWER(REGEXP_REPLACE("Company Name", '[^A-Za-z0-9]', '', 'g')) AS normalized_name,
            LOWER(REGEXP_REPLACE("Company URL", '[^A-Za-z0-9]', '', 'g')) AS normalized_website,
            LOWER(REGEXP_REPLACE("Company LinkedIn page URL", '[^A-Za-z0-9]', '', 'g')) AS normalized_linkedin,
            LOWER(REGEXP_EXTRACT("Company Name", '^([A-Za-z0-9]+)')) AS first_word
        FROM aggregated_data
    ),

                                        -- If name doesn't match name in Linkedin URL --> FKA
    formerly_known_as AS (
        SELECT "Company ID", "Company Name", normalized_name
        FROM normalized_names
        WHERE normalized_linkedin NOT LIKE '%' || first_word || '%'
            AND normalized_linkedin IS NOT NULL
        AND normalized_website NOT LIKE '%' || first_word || '%'
            AND normalized_website IS NOT NULL
    ),

                                        -- If name has various suffixes --> Long Version
    has_long_qualifier AS (            
        SELECT DISTINCT ad."Company ID", ad."Company Name"
        FROM aggregated_data ad
        CROSS JOIN long_name_qualifiers_table lnq
        WHERE LOWER(ad."Company Name") LIKE '%' || lnq.long_qualifiers || '%'
    ),

                                        -- If name is a substring of another company name --> Short Version
    is_short AS (                   
        SELECT DISTINCT n1."Company ID", n1."Company Name"
        FROM  normalized_names n1
        INNER JOIN normalized_names n2
            ON n1."Company ID" = n2."Company ID"
            AND n1."Company Name" <> n2."Company Name"
            AND n2.normalized_name LIKE '%' || n1.normalized_name || '%'
            AND LENGTH(n2."Company Name") > Length(n1."Company Name")
    ),

                        -- If name is the same letters as another version with added punctuation/hyphens etc. --> Alternate Spelling 
    alternate_spelling AS (
        SELECT DISTINCT n1."Company ID", n1."Company Name"
        FROM  normalized_names n1
        INNER JOIN normalized_names n2
            ON n1."Company ID" = n2."Company ID"
            AND n1."Company Name" <> n2."Company Name"
            AND n1.normalized_name = n2.normalized_name
    )
    
    SELECT
        CASE
            WHEN ad."Company Name" = UPPER(ad."Company Name") THEN 'Ticker'     

            WHEN fka."Company Name" IS NOT NULL THEN 'FKA'

            WHEN hlq."Company Name" IS NOT NULL THEN 'Long Version'

            WHEN short."Company Name" IS NOT NULL THEN 'Short Version'

            WHEN alt."Company Name" IS NOT NULL THEN 'Alternate Spelling'

            ELSE 'Brand Name'

        END AS "Name Category"
    FROM aggregated_data ad
    LEFT JOIN formerly_known_as fka
        ON ad."Company ID" = fka."Company ID" AND ad."Company Name" = fka."Company Name"  
    LEFT JOIN has_long_qualifier hlq
        ON ad."Company ID" = hlq."Company ID" AND ad."Company Name" = hlq."Company Name"   
    LEFT JOIN is_short short
        ON ad."Company ID" = short."Company ID" AND ad."Company Name" = short."Company Name"   
    LEFT JOIN alternate_spelling alt
        ON ad."Company ID" = alt."Company ID" AND ad."Company Name" = alt."Company Name"      

    ORDER BY ad."Company ID", ad."Company Name"
"""

#Update data with name categorization
aggregated_data["Name Category"] = duckdb.query(name_categorization).to_df()


#Check for companies with different URL links
conflicting_links = """
    SELECT "Company ID",
    ARRAY_AGG(DISTINCT "Company URL") AS company_urls,
    ARRAY_AGG(DISTINCT "Company LinkedIn page URL") AS linkedin_urls
    FROM aggregated_data 
    GROUP BY "Company ID"
    HAVING COUNT(DISTINCT "Company URL") > 1
        OR COUNT(DISTINCT "Company LinkedIn page URL") > 1
    ORDER BY "Company ID"
"""

print("Conflicting Company URLs")
print(duckdb.query(conflicting_links).to_df())


#Format results
results = duckdb.query("""
    SELECT 
        "Company ID" AS "Company ID#",
        "Company Name" AS "Name Variant",
        "Company URL",
        "Company LinkedIn page URL" AS "LinkedIn URL",
        "Name Category"
    FROM aggregated_data
    ORDER BY "Company ID#", "Company ID#"
        
""").to_df()
results.to_excel('Company_Name_Normalization.xlsx')
