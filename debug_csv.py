import pandas as pd

# Path to your CSV file
csv_path = "data/uploads/25_01_14 ALABAMA VS OLE MISS.csv"

# Load CSV
df = pd.read_csv(csv_path)

# Filter rows where "Row" is "Offense" and where Mark Sears has non-empty data
filtered_df = df[(df["Row"] == "Offense") & (df["#1 Mark Sears"].notna())]

# Show results
print("🔍 Debug: Rows where 'Row' is 'Offense' and Mark Sears has data:")
print(filtered_df[["Row", "Instance number", "#1 Mark Sears"]].to_string())
