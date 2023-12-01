import findspark
findspark.init()
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType

# starts Spark session:
spark = SparkSession \
    .builder \
    .appName("Spark TaxCalc") \
    .getOrCreate()


# import financial data:
df_finData = pd.read_csv('Financial_Data.csv')
df_finData.head(20)

# transform data - remove blank/NaN rows, change 'Country' clumn value to upper case, remove redundant spaces in the 'Country' clumn values
df_finData.dropna(how='all')
df_finData['Country'] = df_finData['Country'].str.upper()
df_finData['Country'] = df_finData['Country'].str.strip()
df_finData['Country'] = df_finData['Country'].str.replace(r'\s+', ' ', regex=True)

# print the example after the data transfer:
df_finData.sort_values(by='Country', ascending=True).head(20)

# create Spark DataFrame:
sdf_finData = spark.createDataFrame(df_finData)


sdf_finData.printSchema()
sdf_finData.head(5)

# in order to run Spark SQL create temporary view:
sdf_finData.createTempView("sdf_newFinData")
spark.sql("SELECT Country, Sales FROM sdf_newFinData").show()

# create pandas_udf function - it is an example of vectorized operations in Pandas, leverages low-level C and Cython routines - makes code more efficient
@pandas_udf(FloatType())
def tax_calc(sales: pd.Series, country: pd.Series) -> pd.Series:
    # Create a Series for tax rate based on country
    tax_rate = pd.Series(0.10, index=sales.index)  # Default tax rate
    tax_rate[country.str.upper() == 'CANADA'] = 0.01
    tax_rate[country.str.upper() == 'GERMANY'] = 10
    tax_rate[country.str.upper() == 'MEXICO'] = 100

    # Calculate tax
    return sales * tax_rate

# in order to be able to use the UDF we need to create a register:
spark.udf.register("calculated_tax", tax_calc)

# run Spark SQL  using the UDF:
spark.sql("SELECT Country, Sales, ROUND(calculated_tax(Sales, Country), 2) as Tax FROM sdf_newFinData ORDER BY Country ASC").show()

# run Spark SQL using the UDF and also using the GROUP BY statement - this triggers the Shuffle that results in a new Stage (in Spark UI this is visualized in under the DAG Visualization):
spark.sql("SELECT Country, \
                  FORMAT_NUMBER(SUM(Sales), 2) as TotalSales, \
                  ROUND(SUM(calculated_tax(Sales, Country)), 2) AS TotalTax \
          FROM sdf_newFinData GROUP BY Country").show()


