# Databricks notebook source


# Ingest library checkout data and print schema
file_paths = ["/Volumes/workspace/default/library_analysis_data/Checkouts_part_aa", "/Volumes/workspace/default/library_analysis_data/Checkouts_part_ab", "/Volumes/workspace/default/library_analysis_data/Checkouts_part_ac"]
df = spark.read.csv(file_paths, header=True, inferSchema=True)
print("Schema of CSV:")
df.printSchema()




# COMMAND ----------
# Charlie Brown Holiday Analysis in PySpark
from pyspark.sql.functions import expr, sum as spark_sum, col
import matplotlib.pyplot as plt

# convert from string to int
df_ints = df.withColumn(
    "CheckoutsInt",
    expr("try_cast(regexp_replace(Checkouts, ',', '') AS INT)")
)

# filter for Charlie Brown Christmas books or DVDs
df_christmas = df_ints.filter(
    (col("Title").contains("A Charlie Brown Christmas")) &
    (col("MaterialType").isin("BOOK", "VIDEODISC"))
)
print("Rows after Christmas filter:", df_christmas.count())

# filter for Charlie Brown Halloween books or DVDs
df_halloween = df_ints.filter(
    (col("Title").contains("It's the Great Pumpkin, Charlie Brown")) &
    (col("MaterialType").isin("BOOK", "VIDEODISC"))
)
print("Rows after Halloween filter:", df_halloween.count())


# Group by month and sum the checkouts
df_CB_christmas_months = (
    df_christmas
    .groupBy("CheckoutMonth")
    .sum("CheckoutsInt")
    .orderBy("CheckoutMonth")
)

df_CB_halloween_months = (
    df_halloween
    .groupBy("CheckoutMonth")
    .sum("CheckoutsInt")
    .orderBy("CheckoutMonth")
)

# Christmas Plot
pdf = df_CB_christmas_months.toPandas()
pdf = pdf.rename(columns={"sum(CheckoutsInt)": "TotalCheckouts"})
pdf["CheckoutMonth"] = pdf["CheckoutMonth"].astype(int)
pdf["TotalCheckouts"] = pdf["TotalCheckouts"].astype(int)
plt.bar(pdf["CheckoutMonth"], pdf["TotalCheckouts"], color='darkgreen')
plt.xlabel("Month")
plt.ylabel("Number of Checkouts")
plt.title("A Charlie Brown Christmas: 2005-2025 Checkouts")
plt.xticks(range(1, 13))  # ensure ticks are 1–12
plt.show()

# Halloween Plot
pdf_halloween = df_CB_halloween_months.toPandas()
pdf_halloween = pdf_halloween.rename(columns={"sum(CheckoutsInt)": "TotalCheckouts"})
pdf_halloween["CheckoutMonth"] = pdf_halloween["CheckoutMonth"].astype(int)
pdf_halloween["TotalCheckouts"] = pdf_halloween["TotalCheckouts"].astype(int)
plt.bar(pdf_halloween["CheckoutMonth"], pdf_halloween["TotalCheckouts"], color='darkorange')
plt.xlabel("Month")
plt.ylabel("Number of Checkouts")
plt.title("It's the Great Pumpkin, CB: 2005-2025 Checkouts")
plt.xticks(range(1, 13))  # ensure ticks are 1–12
plt.show()




# COMMAND ----------
# Charlie Brown Holiday analysis in SQL

# Register the DataFrame as a SQL table
df.createOrReplaceTempView("library")

# filter for Charlie Brown Christmas books or DVDs
df_CB_christmas_months = spark.sql("""
    SELECT 
        CheckoutMonth,
        SUM(CAST(REPLACE(Checkouts, ',', '') AS INT)) AS TotalCheckouts
    FROM library
    WHERE Title LIKE '%A Charlie Brown Christmas%'
      AND MaterialType IN ('BOOK', 'VIDEODISC')
    GROUP BY CheckoutMonth
    ORDER BY CheckoutMonth
""")

# filter for Charlie Brown Halloween books or DVDs
df_CB_halloween_months = spark.sql("""
    SELECT 
        CheckoutMonth,
        SUM(CAST(REPLACE(Checkouts, ',', '') AS INT)) AS TotalCheckouts
    FROM library
    WHERE Title LIKE "%It's the Great Pumpkin, Charlie Brown%"
      AND MaterialType IN ('BOOK', 'VIDEODISC')
    GROUP BY CheckoutMonth
    ORDER BY CheckoutMonth
""")

# Christmas plot
pdf = df_CB_christmas_months.toPandas()
pdf["CheckoutMonth"] = pdf["CheckoutMonth"].astype(int)
pdf = pdf.sort_values("CheckoutMonth")
plt.bar(pdf["CheckoutMonth"], pdf["TotalCheckouts"], color='darkgreen')
plt.xlabel("Month")
plt.ylabel("Number of Checkouts")
plt.title("A Charlie Brown Christmas: 2005-2025 Checkouts")
plt.xticks(range(1, 13))
plt.show()

# Halloween Plot
pdf_halloween = df_CB_halloween_months.toPandas()
pdf_halloween["CheckoutMonth"] = pdf_halloween["CheckoutMonth"].astype(int)
pdf_halloween = pdf_halloween.sort_values("CheckoutMonth")
plt.bar(pdf_halloween["CheckoutMonth"], pdf_halloween["TotalCheckouts"], color='darkorange')
plt.xlabel("Month")
plt.ylabel("Number of Checkouts")
plt.title("It's the Great Pumpkin, CB: 2005-2025 Checkouts")
plt.xticks(range(1, 13))
plt.show()
