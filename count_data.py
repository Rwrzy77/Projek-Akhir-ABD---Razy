import pandas as pd, os

base = r"c:\\Users\\Razy77\\OneDrive\\Desktop\\UB materi kelas\\sem 4\\Big data\\projek_akhir_ABD"
files = {
    "LinkedIn": "linkedin_jobs_Ribuan.csv",
    "Glints": "data_loker_glints.csv",
    "Tech in Asia": "techinasia_it_massive.csv",
    "Karir.com": "karir_dataset_master.csv",
    "Merged (All)": "merged_it_jobs.csv",
}

total = 0
print(f"{'Dataset':<15}\t{'Rows':>6}")
for name, fname in files.items():
    path = os.path.join(base, fname)
    if not os.path.exists(path):
        print(f"{name:<15}\tFILE NOT FOUND")
        continue
    df = pd.read_csv(path, low_memory=False)
    rows = len(df)
    print(f"{name:<15}\t{rows:,}")
    if name != "Merged (All)":
        total += rows
print(f"{'TOTAL (4 sources)':<15}\t{total:,}")
