# Databricks notebook source


# Ingest library checkout data and print schema

file_paths = ["/Volumes/workspace/default/library_analysis_data/Checkouts_part_aa", "/Volumes/workspace/default/library_analysis_data/Checkouts_part_ab", "/Volumes/workspace/default/library_analysis_data/Checkouts_part_ac"]
df = spark.read.csv(file_paths, header=True, inferSchema=True)

print("Schema of CSV:")
df.printSchema()




# COMMAND ----------

# Filter for years 2005-2025 and create MaterialType groups
from pyspark.sql.functions import col, when, sum as _sum
import matplotlib.pyplot as plt

# Register the DataFrame as a SQL table
df.createOrReplaceTempView("library")

# Use SQL to create aggregated data with MaterialType grouping by Year
aggregated_df = spark.sql("""
    SELECT 
        CheckoutYear,
        CASE 
            WHEN MaterialType IN ('BOOK', 'VIDEODISC', 'EBOOK', 'AUDIOBOOK', 
                                  'SOUNDDISC', 'VIDEOCASS', 'SONG') 
            THEN MaterialType
            ELSE 'Other'
        END AS MaterialTypeGrouped,
        SUM(CAST(REPLACE(Checkouts, ',', '') AS INT)) AS TotalCheckouts
    FROM library
    WHERE CheckoutYear BETWEEN 2005 AND 2025
    GROUP BY CheckoutYear, MaterialTypeGrouped
    ORDER BY CheckoutYear, MaterialTypeGrouped
""")

# Use SQL to create the aggregated data with MaterialType grouping by Month
aggregated_df_monthly = spark.sql("""
    SELECT 
        CheckoutMonth,
        CASE 
            WHEN MaterialType IN ('BOOK', 'VIDEODISC', 'EBOOK', 'AUDIOBOOK', 
                                  'SOUNDDISC', 'VIDEOCASS', 'SONG') 
            THEN MaterialType
            ELSE 'Other'
        END AS MaterialTypeGrouped,
        SUM(CAST(REPLACE(Checkouts, ',', '') AS INT)) AS TotalCheckouts
    FROM library
    WHERE CheckoutYear BETWEEN 2005 AND 2025
    GROUP BY CheckoutMonth, MaterialTypeGrouped
    ORDER BY CheckoutMonth, MaterialTypeGrouped
""")
# Display the aggregated data
#display(aggregated_df)




# COMMAND ----------

# plot trends by year
import matplotlib.pyplot as plt
yearly_data_pd = aggregated_df.toPandas()

# custom label mapping
label_mapping = {
    'BOOK': 'Book',
    'EBOOK': 'E-Book',
    'AUDIOBOOK': 'Audiobook',
    'VIDEODISC': 'DVD',
    'SOUNDDISC': 'CD',
    'VIDEOCASS': 'VHS',
    'SONG': 'Song',
    'MAGAZINE': 'Magazine',
    'Other': 'Other'
}

# Create pie plots for each year
years = sorted(yearly_data_pd["CheckoutYear"].unique())

# Create subplots
n_years = len(years)
n_cols = 4
n_rows = (n_years + n_cols - 1) // n_cols

fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 5 * n_rows))
axes = axes.flatten() if n_years > 1 else [axes]

# Define colors for consistency
specific_types = ["BOOK", "VIDEODISC", "EBOOK", "AUDIOBOOK", "SOUNDDISC", "VIDEOCASS", "SONG"]
material_types = specific_types + ["Other"]
colors = plt.cm.Set3(range(len(material_types)))
color_map = dict(zip(material_types, colors))

for idx, year in enumerate(years):
    year_data = yearly_data_pd[yearly_data_pd["CheckoutYear"] == year]
    
    # Filter out zero or very small values (less than 0.1% of total)
    total = year_data["TotalCheckouts"].sum()
    year_data = year_data[year_data["TotalCheckouts"] > 0]
    year_data = year_data[year_data["TotalCheckouts"] / total >= 0.001]

    year_data = year_data.sort_values("MaterialTypeGrouped")
    custom_labels = [label_mapping.get(mt, mt) for mt in year_data["MaterialTypeGrouped"]]
    ax = axes[idx]

    wedges, texts, autotexts = ax.pie(
        year_data["TotalCheckouts"],
        labels=custom_labels,
        autopct='%1.1f%%',
        colors=[color_map[mt] for mt in year_data["MaterialTypeGrouped"]],
        startangle=90,
        textprops={'fontsize': 9}
    )
    
    for autotext in autotexts:
        autotext.set_color('black')
        autotext.set_fontsize(8)
        autotext.set_weight('bold')
    
    ax.set_title(f'Material Type Distribution - {int(year)}', fontsize=12, weight='bold')

# Hide empty subplots
for idx in range(n_years, len(axes)):
    axes[idx].axis('off')

plt.tight_layout()
plt.show()

print(f"\nCreated pie plots for {n_years} years (2005-2025)")




# COMMAND ----------

# plot trends by month (2005-2025)

import matplotlib.pyplot as plt

monthly_data_pd = aggregated_df_monthly.toPandas()
# Ensure CheckoutMonth is integer type for proper sorting
monthly_data_pd["CheckoutMonth"] = monthly_data_pd["CheckoutMonth"].astype(int)

# Month labels
month_names = {
    1: 'January (2005-2025)', 2: 'February (2005-2025)', 3: 'March (2005-2025)', 4: 'April (2005-2025)',
    5: 'May (2005-2025)', 6: 'June (2005-2025)', 7: 'July (2005-2025)', 8: 'August (2005-2025)',
    9: 'September (2005-2025)', 10: 'October (2005-2025)', 11: 'November (2005-2024)', 12: 'December (2005-2024)'
}

# Create pie plots for each month 
months = list(range(1, 13))

# Create subplots (12 months, 3 columns and 4 rows)
n_cols = 3
n_rows = 4
fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, 24))

# Adjust spacing between subplots
plt.subplots_adjust(hspace=0.3, wspace=0.3)

# Define colors for consistency
specific_types = ["BOOK", "VIDEODISC", "EBOOK", "AUDIOBOOK", "SOUNDDISC", "VIDEOCASS", "SONG"]
material_types = specific_types + ["Other"]
colors = plt.cm.Set3(range(len(material_types)))
color_map = dict(zip(material_types, colors))

for idx, month in enumerate(months):
    # Calculate row and column position
    row = idx // n_cols
    col = idx % n_cols
    ax = axes[row, col]
    
    month_data = monthly_data_pd[monthly_data_pd["CheckoutMonth"] == month]
    
    # Filter out zero or very small values (less than 0.1% of total)
    total = month_data["TotalCheckouts"].sum()
    month_data = month_data[month_data["TotalCheckouts"] > 0]
    month_data = month_data[month_data["TotalCheckouts"] / total >= 0.001]

    month_data = month_data.sort_values("MaterialTypeGrouped")
    custom_labels = [label_mapping.get(mt, mt) for mt in month_data["MaterialTypeGrouped"]]

    wedges, texts, autotexts = ax.pie(
        month_data["TotalCheckouts"],
        labels=custom_labels,
        autopct='%1.1f%%',
        colors=[color_map[mt] for mt in month_data["MaterialTypeGrouped"]],
        startangle=90,
        textprops={'fontsize': 11}
    )
    
    for autotext in autotexts:
        autotext.set_color('black')
        autotext.set_fontsize(8)
        autotext.set_weight('bold')
    
    ax.set_title(f'{month_names[int(month)]}', fontsize=12, weight='bold')

plt.tight_layout()
plt.show()

print(f"\nCreated pie plots for 12 months (2005-2025 combined)")
