import pandas as pd

# Read CSV with semicolon separator
df = pd.read_csv("stackoverflow_kafka.csv", sep=';')

# Convert date column to datetime
df['date'] = pd.to_datetime(df['date'])

# Split tags and explode
df['tags'] = df['tags'].str.split(',')
df_exploded = df.explode('tags')

# Extract year-month
df_exploded['month'] = df_exploded['date'].dt.to_period('M')

# Keep only tags that appear more than 50 times
tag_totals = df_exploded['tags'].value_counts()
frequent_tags = tag_totals[tag_totals > 50].index
df_filtered = df_exploded[df_exploded['tags'].isin(frequent_tags)]

# Outliers only
# outlier_tags = tag_totals[tag_totals <= 10].index
# df_filtered = df_exploded[df_exploded['tags'].isin(outlier_tags)]


# Count frequency of each tag per month
tag_counts = df_filtered.groupby(['month', 'tags']).size().reset_index(name='count')

# Pivot so that months are rows and tags are columns
tag_pivot = tag_counts.pivot(index='month', columns='tags', values='count').fillna(0).astype(int)

# Optional: sort columns alphabetically
tag_pivot = tag_pivot.sort_index(axis=1)

# tag_pivot.to_excel("tag_frequency_by_month.xlsx", sheet_name="Tag Frequency")
tag_pivot.to_csv("tag_frequency_by_month.csv", sep=';')