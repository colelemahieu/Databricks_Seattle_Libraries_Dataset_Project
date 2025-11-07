# Databricks notebook source

# Ingest library checkout data and print schema
file_paths = ["/Volumes/workspace/default/library_analysis_data/Checkouts_part_aa", "/Volumes/workspace/default/library_analysis_data/Checkouts_part_ab", "/Volumes/workspace/default/library_analysis_data/Checkouts_part_ac"]
df = spark.read.csv(file_paths, header=True, inferSchema=True)

print("Schema of CSV:")
df.printSchema()





# COMMAND ----------

# save schema as image for github Images
import io
import matplotlib.pyplot as plt
# Capture schema as text 
schema_text = df._schema.simpleString() if hasattr(df, "_schema") else df.schema.simpleString()
# Prettify schema text
schema_lines = schema_text.replace("struct<", "").replace(">", "").replace(":", ": ").split(",")
# Create image
fig, ax = plt.subplots(figsize=(8, len(schema_lines) * 0.3))  # dynamic height based on number of lines
ax.text(0, 1, "\n".join(schema_lines), fontsize=10, family='monospace', va='top')
ax.axis('off')
# Save with tight bounding box and minimal padding
plt.savefig("schema_output.png", dpi=300, bbox_inches='tight', pad_inches=0.05)
plt.show()





# COMMAND ----------

# print out some basic info
from pyspark.sql.functions import col, min, max, countDistinct, regexp_replace, sum as _sum

print("Total number of rows:")
print(df.count())

print("Range of CheckoutYear:")
df.select(
    min(col("CheckoutYear")).alias("MinYear"),
    max(col("CheckoutYear")).alias("MaxYear")
).show()

print("Range of Checkouts (for BOOKs only):")
df.filter(col("MaterialType") == "BOOK") \
  .select(
      min(col("Checkouts")).alias("MinCheckouts_books"),
      max(col("Checkouts")).alias("MaxCheckouts_books")
  ) \
  .show()

print("Most Popular Checkouts (for BOOKs only):")
df.filter(col("MaterialType") == "BOOK") \
  .groupBy("Title") \
  .agg(_sum(regexp_replace(col("Checkouts"), ",", "").cast("int")).alias("total_checkouts")) \
  .orderBy(col("total_checkouts").desc()) \
  .show(5, truncate=False)





# COMMAND ----------

# Cast the Checkouts column to integers in order to total the Checkouts' values by MaterialType
# Print out all MaterialTypes to see what's there 
from pyspark.sql.functions import expr, sum as spark_sum, col

df_clean = df.withColumn(
    "CheckoutsInt",
    expr("try_cast(regexp_replace(Checkouts, ',', '') AS INT)")
)

df_total = (
    df_clean
    .groupBy("MaterialType")
    .agg(spark_sum(col("CheckoutsInt")).alias("TotalCheckouts"))
)

num_MaterialTypes = df.select("MaterialType").distinct().count()
df_total.show(num_MaterialTypes, truncate=False)





# COMMAND ----------

# save df_total table to an image for github Images
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from io import BytesIO
from IPython.display import Image

fig, ax = plt.subplots(figsize=(10, 6))
ax.axis("off")
table = plt.table(cellText=df_total.toPandas().values,
                  colLabels=df_total.columns,
                  cellLoc='center',
                  loc='center')

plt.tight_layout()
buf = BytesIO()
plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
buf.seek(0)
display(Image(data=buf.getvalue()))





# COMMAND ----------

# This cell plots a bar graph of total checkouts (2005-2025) for various material types 
import matplotlib.pyplot as plt

main_types = ["BOOK", "AUDIOBOOK", "SOUNDDISC", "SONG", "VIDEODISC", "EBOOK", "VIDEOCASS", "MAGAZINE"]
df_pandas = df_total.toPandas()
df_main = df_pandas[df_pandas["MaterialType"].isin(main_types)]
df_other = df_pandas[~df_pandas["MaterialType"].isin(main_types)]
other_sum = df_other["TotalCheckouts"].sum()
df_plot = df_main.copy()
df_plot.loc[len(df_plot)] = ["OTHER", other_sum]
df_plot = df_plot.sort_values(by="TotalCheckouts", ascending=False)

# Plot
plt.figure(figsize=(10,6))
bars = plt.bar(df_plot["MaterialType"], df_plot["TotalCheckouts"], color="skyblue")
plt.title("Total Library Checkouts: 2005-2025")
plt.xlabel("Material Type")
plt.ylabel("Total Checkouts")
plt.xticks(rotation=45)

for bar in bars:
    height = bar.get_height()
    plt.text(
        bar.get_x() + bar.get_width()/2,   # x position (center of bar)
        height,                            # y position (top of bar)
        f"{int(height):,}",                # format with commas
        ha="center", va="bottom", fontsize=9, color="black"
    )


plt.tight_layout()
plt.show()





# COMMAND ----------

# What percent of total checkouts are "Uncatalogued Folder or Bag..."?
from pyspark.sql.functions import col, regexp_replace, sum as _sum

df_cleaned = df.withColumn(
    "CheckoutsInt",
    regexp_replace(
        col("Checkouts"),
        ",",
        ""
    ).cast("int")
)

total_checkouts = df_cleaned.select(
    _sum("CheckoutsInt").alias("total")
).collect()[0]["total"]

uncataloged_checkouts = df_cleaned.filter(
    (col("Title") == "<Unknown Title>") | (col("Title").like("Uncataloged%"))
).select(
    _sum("CheckoutsInt").alias("uncat_total")
).collect()[0]["uncat_total"]

if uncataloged_checkouts is None:
    uncataloged_checkouts = 0

percent_uncataloged = __builtins__.round(
    uncataloged_checkouts / total_checkouts * 100,
    2
)

print (uncataloged_checkouts, " uncatalogued checkouts")
print (total_checkouts, " total checkouts")
print (percent_uncataloged, " percent of total checkouts are uncatalogued")
