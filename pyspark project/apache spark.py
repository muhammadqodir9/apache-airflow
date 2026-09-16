from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MyFirstSparkApp")
    .master("local[*]")
    .getOrCreate()
)

data = [
    ("Uzbekistan", "Laptop", 2, 800),
    ("Uzbekistan", "Mouse", 5, 25),
    ("Kazakhstan", "Keyboard", 3, 50),
    ("Kazakhstan", "Monitor", 2, 300),
    ("Uzbekistan", "Headphones", 4, 80),
]

df = spark.createDataFrame(
    data,
    ["country", "product", "quantity", "price"]
)

df.show()

print("Number of rows:", df.count())

result = (
    df.groupBy("country")
      .sum("price")
)

result.show()

input("Press Enter to stop Spark...")

spark.stop()