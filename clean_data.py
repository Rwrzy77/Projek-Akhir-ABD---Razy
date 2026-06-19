import pandas as pd
import re
import numpy as np

def clean_salary_value(val_str):
    if pd.isna(val_str) or not isinstance(val_str, str):
        return None, None
    
    val_str = val_str.lower().strip()
    
    # Check for empty/missing salary indicators
    missing_indicators = [
        "gaji tidak ditampilkan", "tidak dicantumkan", "tidak disebutkan", 
        "confidential", "dirahasiakan", "unknown", "tidak ada"
    ]
    if any(ind in val_str for ind in missing_indicators) or val_str == "":
        return None, None
    
    # Try to determine currency and multiplier
    currency_multiplier = 1.0
    if "$" in val_str or "sgd" in val_str or "usd" in val_str:
        currency_multiplier = 12000.0  # approximate to IDR
    elif "₫" in val_str or "vnd" in val_str:
        currency_multiplier = 0.65     # approximate to IDR
    
    # Normalize range numbers (e.g. "6,5 jt - 7,5 jt" or "3.000.000 - 4.000.000")
    
    # Split by range indicator
    parts = re.split(r'[-–—to]', val_str)
    
    def parse_part(part):
        part = part.strip()
        if not part:
            return None
        
        # Determine multiplier for jt (million) or rb/k (thousand)
        part_multiplier = 1.0
        if "jt" in part or "juta" in part or "million" in part or "m" in part:
            part_multiplier = 1000000.0
        elif "rb" in part or "ribu" in part or "thousand" in part or "k" in part:
            part_multiplier = 1000.0
            
        # Clean decimals and thousand separators (e.g. "3.000.000" -> "3000000", "6,5 jt" -> "6.5")
        
        # Handle commas and thousand separator dots
        if "," in part:
            part = part.replace(".", "") # remove thousand dots if any
            part = part.replace(",", ".") # convert comma to dot for decimal
        else:
            nums_only = re.findall(r'\d+', part)
            if len(nums_only) > 1 and all(len(x) == 3 for x in nums_only[1:]):
                part = "".join(nums_only)
        
        # Find the first float or integer number in the string
        nums = re.findall(r'\d+(?:\.\d+)?', part)
        if not nums:
            return None
        
        val = float(nums[0])
        return val * part_multiplier * currency_multiplier

    if len(parts) == 2:
        min_val = parse_part(parts[0])
        max_val = parse_part(parts[1])
        # Adjust mismatched multiplier ranges
        if min_val is not None and max_val is not None:
            if min_val > max_val:
                # Multiply max_val by multiplier if min_val has it
                if "jt" in parts[0] and "jt" not in parts[1]:
                    max_val *= 1000000.0
                elif "rb" in parts[0] and "rb" not in parts[1]:
                    max_val *= 1000.0
            elif max_val > min_val * 1000:
                if "jt" in parts[1] and "jt" not in parts[0]:
                    min_val *= 1000000.0
                elif "rb" in parts[1] and "rb" not in parts[0]:
                    min_val *= 1000.0
        return min_val, max_val
    elif len(parts) == 1:
        val = parse_part(parts[0])
        return val, val
    
    return None, None

def detect_country(location_str):
    if pd.isna(location_str) or not isinstance(location_str, str):
        return "Indonesia"
    
    location_lower = location_str.lower()
    
    countries = {
        "singapore": "Singapore",
        "malaysia": "Malaysia",
        "vietnam": "Vietnam",
        "india": "India",
        "united kingdom": "United Kingdom",
        "london": "United Kingdom",
        "united states": "United States",
        "usa": "United States",
        "philippines": "Philippines",
        "manila": "Philippines",
        "japan": "Japan",
        "tokyo": "Japan",
        "thailand": "Thailand",
        "bangkok": "Thailand",
        "kenya": "Kenya",
        "nigeria": "Nigeria",
        "peru": "Peru",
        "germany": "Germany",
        "australia": "Australia",
        "canada": "Canada"
    }
    
    for key, name in countries.items():
        if key in location_lower:
            return name
            
    return "Indonesia"

def clean_experience(exp_str):
    if pd.isna(exp_str) or not isinstance(exp_str, str):
        return "Tidak dicantumkan"
    exp_str = exp_str.strip()
    if exp_str.lower() in ["tidak diketahui", "tidak dicantumkan", "tidak disebutkan", "apa saja"]:
        return "Tidak dicantumkan"
    return exp_str

def clean_education(edu_str):
    if pd.isna(edu_str) or not isinstance(edu_str, str):
        return "Tidak dicantumkan"
    edu_str = edu_str.strip()
    if edu_str.lower() in ["tidak diketahui", "tidak dicantumkan", "tidak disebutkan", "apa saja"]:
        return "Tidak dicantumkan"
    return edu_str

def normalize_job_type(jt_str):
    if pd.isna(jt_str) or not isinstance(jt_str, str):
        return "Full-time"
    
    jt_lower = jt_str.lower()
    if "penuh waktu" in jt_lower or "full-time" in jt_lower or "fulltime" in jt_lower:
        return "Full-time"
    elif "kontrak" in jt_lower or "contract" in jt_lower:
        return "Contract"
    elif "magang" in jt_lower or "intern" in jt_lower:
        return "Internship"
    elif "freelance" in jt_lower:
        return "Freelance"
    elif "paruh waktu" in jt_lower or "part-time" in jt_lower:
        return "Part-time"
    elif "remote" in jt_lower:
        return "Remote"
    else:
        return "Full-time"

print("Starting Data Integration & Cleaning Process...")

# 1. Load Glints
print("Processing Glints Data...")
try:
    df_glints = pd.read_csv("data_loker_glints.csv", delimiter=";", encoding="utf-8-sig")
    df_glints_cleaned = pd.DataFrame()
    df_glints_cleaned['job_title'] = df_glints['Judul Posisi']
    df_glints_cleaned['company_name'] = df_glints['Nama Perusahaan']
    df_glints_cleaned['location'] = df_glints['Lokasi']
    df_glints_cleaned['country'] = df_glints['Lokasi'].apply(detect_country)
    df_glints_cleaned['job_type'] = df_glints['Tipe Pekerjaan'].apply(normalize_job_type)
    df_glints_cleaned['experience'] = df_glints['Pengalaman'].apply(clean_experience)
    df_glints_cleaned['education'] = df_glints['Pendidikan'].apply(clean_education)
    
    # Parse salaries
    salaries = df_glints['Gaji'].apply(clean_salary_value)
    df_glints_cleaned['salary_min'] = [s[0] for s in salaries]
    df_glints_cleaned['salary_max'] = [s[1] for s in salaries]
    
    df_glints_cleaned['skills'] = ""
    df_glints_cleaned['keyword'] = df_glints['Keyword']
    df_glints_cleaned['platform'] = "Glints"
    df_glints_cleaned['post_date'] = df_glints['Tanggal Posting'].fillna("Tidak dicantumkan")
    df_glints_cleaned['job_url'] = "Tidak dicantumkan"
    print(f"Glints parsed: {len(df_glints_cleaned)} records.")
except Exception as e:
    print(f"Error Glints: {e}")
    df_glints_cleaned = pd.DataFrame()

# 2. Load Karir.com
print("Processing Karir.com Data...")
try:
    df_karir = pd.read_csv("karir_dataset_master.csv", delimiter=";", encoding="utf-8-sig")
    df_karir_cleaned = pd.DataFrame()
    df_karir_cleaned['job_title'] = df_karir['job_title']
    df_karir_cleaned['company_name'] = df_karir['company_name']
    df_karir_cleaned['location'] = df_karir['location']
    df_karir_cleaned['country'] = df_karir['location'].apply(detect_country)
    df_karir_cleaned['job_type'] = df_karir['job_type'].apply(normalize_job_type)
    df_karir_cleaned['experience'] = df_karir['experience_level'].apply(clean_experience)
    df_karir_cleaned['education'] = df_karir['education_req'].apply(clean_education)
    
    # Parse salaries
    salaries = df_karir['salary_range'].apply(clean_salary_value)
    df_karir_cleaned['salary_min'] = [s[0] for s in salaries]
    df_karir_cleaned['salary_max'] = [s[1] for s in salaries]
    
    df_karir_cleaned['skills'] = df_karir['job_requirements'].fillna("")
    df_karir_cleaned['keyword'] = df_karir['job_title']
    df_karir_cleaned['platform'] = "Karir.com"
    df_karir_cleaned['post_date'] = df_karir['posted_date'].fillna("Tidak dicantumkan")
    df_karir_cleaned['job_url'] = "Tidak dicantumkan"
    print(f"Karir.com parsed: {len(df_karir_cleaned)} records.")
except Exception as e:
    print(f"Error Karir.com: {e}")
    df_karir_cleaned = pd.DataFrame()

# 3. Load LinkedIn
print("Processing LinkedIn Data...")
try:
    df_linkedin = pd.read_csv("linkedin_jobs_Ribuan.csv", delimiter=";", encoding="utf-8-sig")
    df_linkedin_cleaned = pd.DataFrame()
    df_linkedin_cleaned['job_title'] = df_linkedin['job_title']
    df_linkedin_cleaned['company_name'] = df_linkedin['company_name']
    df_linkedin_cleaned['location'] = df_linkedin['location']
    df_linkedin_cleaned['country'] = df_linkedin['location'].apply(detect_country)
    df_linkedin_cleaned['job_type'] = "Full-time"  # default
    df_linkedin_cleaned['experience'] = "Tidak dicantumkan"
    df_linkedin_cleaned['education'] = "Tidak dicantumkan"
    df_linkedin_cleaned['salary_min'] = np.nan
    df_linkedin_cleaned['salary_max'] = np.nan
    df_linkedin_cleaned['skills'] = ""
    df_linkedin_cleaned['keyword'] = df_linkedin['searched_keyword']
    df_linkedin_cleaned['platform'] = "LinkedIn"
    df_linkedin_cleaned['post_date'] = df_linkedin['posted_date'].fillna("Tidak dicantumkan")
    df_linkedin_cleaned['job_url'] = df_linkedin['job_link'].fillna("Tidak dicantumkan")
    print(f"LinkedIn parsed: {len(df_linkedin_cleaned)} records.")
except Exception as e:
    print(f"Error LinkedIn: {e}")
    df_linkedin_cleaned = pd.DataFrame()

# 4. Load Tech in Asia
print("Processing Tech in Asia Data...")
try:
    df_tia = pd.read_csv("techinasia_it_massive.csv", delimiter=";", encoding="utf-8-sig")
    df_tia_cleaned = pd.DataFrame()
    df_tia_cleaned['job_title'] = df_tia['job_title']
    df_tia_cleaned['company_name'] = df_tia['company_name']
    df_tia_cleaned['location'] = df_tia['location']
    df_tia_cleaned['country'] = df_tia['location'].apply(detect_country)
    df_tia_cleaned['job_type'] = df_tia['job_type'].apply(normalize_job_type)
    
    # Clean experience: "experience_years_min"
    def map_tia_exp(years):
        if pd.isna(years):
            return "Tidak dicantumkan"
        try:
            y = int(years)
            if y == 0:
                return "Fresh Graduate / Tanpa Pengalaman"
            return f"{y} tahun"
        except:
            return str(years)
            
    df_tia_cleaned['experience'] = df_tia['experience_years_min'].apply(map_tia_exp)
    df_tia_cleaned['education'] = "Tidak dicantumkan"
    df_tia_cleaned['salary_min'] = np.nan
    df_tia_cleaned['salary_max'] = np.nan
    df_tia_cleaned['skills'] = df_tia['skills_required'].fillna("")
    df_tia_cleaned['keyword'] = df_tia['job_title']
    df_tia_cleaned['platform'] = "Tech In Asia"
    df_tia_cleaned['post_date'] = df_tia['published_at'].fillna("Tidak dicantumkan")
    df_tia_cleaned['job_url'] = "Tidak dicantumkan"
    print(f"Tech in Asia parsed: {len(df_tia_cleaned)} records.")
except Exception as e:
    print(f"Error Tech in Asia: {e}")
    df_tia_cleaned = pd.DataFrame()

# 5. Load Indeed
print("Processing Indeed Data...")
try:
    df_indeed = pd.read_csv("data_indeed_it_jobs.csv", delimiter=";", encoding="utf-8-sig")
    df_indeed_cleaned = pd.DataFrame()
    df_indeed_cleaned['job_title'] = df_indeed['job_title']
    df_indeed_cleaned['company_name'] = df_indeed['company_name']
    df_indeed_cleaned['location'] = df_indeed['location']
    df_indeed_cleaned['country'] = df_indeed['location'].apply(detect_country)
    
    # Check salary_raw to determine job type
    def parse_indeed_job_type(row):
        raw = str(row.get('salary_raw', '')).lower()
        if pd.isna(row.get('salary_raw')) or raw.strip() == "" or raw == "nan":
            return normalize_job_type(row['job_type'])
        if 'contract' in raw:
            return 'Contract'
        elif 'internship' in raw:
            return 'Internship'
        elif 'part-time' in raw or 'paruh waktu' in raw:
            return 'Part-time'
        elif 'temporary' in raw:
            return 'Contract'
        elif 'freelance' in raw:
            return 'Freelance'
        return normalize_job_type(row['job_type'])
        
    df_indeed_cleaned['job_type'] = df_indeed.apply(parse_indeed_job_type, axis=1)
    df_indeed_cleaned['experience'] = "Tidak dicantumkan"
    df_indeed_cleaned['education'] = "Tidak dicantumkan"
    df_indeed_cleaned['salary_min'] = np.nan
    df_indeed_cleaned['salary_max'] = np.nan
    df_indeed_cleaned['skills'] = ""
    df_indeed_cleaned['keyword'] = df_indeed['keyword']
    df_indeed_cleaned['platform'] = "Indeed"
    df_indeed_cleaned['post_date'] = "Tidak dicantumkan"
    df_indeed_cleaned['job_url'] = df_indeed['job_url'].fillna("Tidak dicantumkan")
    print(f"Indeed parsed: {len(df_indeed_cleaned)} records.")
except Exception as e:
    print(f"Error Indeed: {e}")
    df_indeed_cleaned = pd.DataFrame()

# Merge all
dfs = [df_glints_cleaned, df_karir_cleaned, df_linkedin_cleaned, df_tia_cleaned, df_indeed_cleaned]
dfs = [df for df in dfs if not df.empty]

if dfs:
    df_merged = pd.concat(dfs, ignore_index=True)
    
    # Save cleaned merged dataset
    df_merged.to_csv("merged_it_jobs.csv", sep=";", index=False, encoding="utf-8-sig")
    print(f"\nIntegration Complete! Saved {len(df_merged)} records to merged_it_jobs.csv")
    
    # Print some stats
    print("\nRecord distribution by Platform:")
    print(df_merged['platform'].value_counts())
    
    print("\nRecord distribution by Country:")
    print(df_merged['country'].value_counts().head(5))
    
    print("\nSalary extraction stats:")
    salary_count = df_merged['salary_min'].notna().sum()
    print(f"Jobs with parsed salary: {salary_count} ({salary_count/len(df_merged)*100:.2f}%)")
else:
    print("No datasets were successfully merged.")
