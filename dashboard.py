import streamlit as st
import plotly.express as px
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crearea unei sesiuni Spark
spark = SparkSession.builder.appName("Flight Delays Analysis").getOrCreate()

# Încărcarea fișierului CSV cu PySpark
file_path = "Airline_Delay_Cause.csv"  # Înlocuiește cu calea către fișierul tău CSV
df = spark.read.csv(file_path, header=True, inferSchema=True)

df_spark = df.filter(~df.year.isin(2020, 2021, 2022))


# Verificarea primelor linii din DataFrame
st.write("Primele 5 linii din dataset:", df_spark.limit(5).toPandas())

# Crearea unui grafic de întârzieri
df_spark_pd = df_spark.toPandas()  # Convertirea DataFrame-ului Spark într-un DataFrame Pandas
fig = px.histogram(df_spark_pd, x="arr_delay", nbins=50, title="Distribution of Arrival Delays")
st.plotly_chart(fig)

# Calcularea întârzierilor medii pe an
avg_delays = df_spark.groupBy("year").agg(
    {"arr_delay": "avg", "carrier_delay": "avg", "weather_delay": "avg", "nas_delay": "avg", 
     "security_delay": "avg", "late_aircraft_delay": "avg"}
).withColumnRenamed("avg(arr_delay)", "avg_arr_delay") \
 .withColumnRenamed("avg(carrier_delay)", "avg_carrier_delay") \
 .withColumnRenamed("avg(weather_delay)", "avg_weather_delay") \
 .withColumnRenamed("avg(nas_delay)", "avg_nas_delay") \
 .withColumnRenamed("avg(security_delay)", "avg_security_delay") \
 .withColumnRenamed("avg(late_aircraft_delay)", "avg_late_aircraft_delay")

# Convertirea rezultatelor la Pandas pentru a le vizualiza în Streamlit
avg_delays_pd = avg_delays.toPandas()
st.write("Average delays per year:", avg_delays_pd)

# Crearea unui grafic pentru întârzierile medii pe an
fig_avg_delays = px.bar(avg_delays_pd, x="year", y=["avg_arr_delay", "avg_carrier_delay", "avg_weather_delay", "avg_nas_delay", "avg_security_delay", "avg_late_aircraft_delay"], title="Average Delays Per Year")
st.plotly_chart(fig_avg_delays)

spark.stop()