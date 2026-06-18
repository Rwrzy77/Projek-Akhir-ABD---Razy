import csv

files = [
    "data_loker_glints.csv",
    "karir_dataset_master.csv",
    "linkedin_jobs_Ribuan.csv",
    "techinasia_it_massive.csv"
]

for file in files:
    try:
        with open(file, 'r', encoding='utf-8') as f:
            # Check for delimiter
            line = f.readline()
            f.seek(0)
            dialect = csv.Sniffer().sniff(line)
            reader = csv.DictReader(f, delimiter=dialect.delimiter)
            count = sum(1 for row in reader)
            print(f"{file}: {count} records (delimiter: '{dialect.delimiter}')")
    except Exception as e:
        print(f"Error reading {file}: {e}")
