import pandas as pd

# Load CSV
# Path relative to project root (one level up from webscraping/)
df = pd.read_csv('../kalsel_cash_nodes.csv')

# 1. Remove the 'category' column
df = df.drop(columns=['category'])

# 2. Rename duplicated location names with 1, 2, 3, ...
name_counts = {}
new_names = []
for name in df['name']:
    if name in name_counts:
        name_counts[name] += 1
        new_names.append(f"{name} {name_counts[name]}")
    else:
        # Check if this name appears more than once total
        total = (df['name'] == name).sum()
        if total > 1:
            name_counts[name] = 1
            new_names.append(f"{name} 1")
        else:
            name_counts[name] = 0
            new_names.append(name)

df['name'] = new_names

# 3. Add Google Maps link column
df['gmaps_link'] = df.apply(
    lambda row: f"https://www.google.com/maps?q={row['latitude']},{row['longitude']}",
    axis=1
)

# Save
df.to_csv('../kalsel_cash_nodes.csv', index=False)

print(f"Preprocessed {len(df)} rows.")
print(f"Columns: {list(df.columns)}")
print(f"\nSample duplicated names renamed:")
dupes = [n for n in df['name'] if any(c.isdigit() and n.endswith(c) for c in '123456789')]
for d in dupes[:10]:
    print(f"  - {d}")
print(f"\nSample gmaps links:")
for _, row in df.head(3).iterrows():
    print(f"  {row['name']}: {row['gmaps_link']}")
