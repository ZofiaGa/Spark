import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd

# starts Spark session:
spark = SparkSession.builder \
    .appName("Spark TaxCalc") \
    .getOrCreate()


# import financial data:
df = pd.read_csv('Financial_Data.csv')
df.head(10)


# create function to remove duplicates:
def df_no_duplicates(fun_df):
    return fun_df.drop_duplicates(subset='Country').sort_values(by='Country', ascending=True)


# clean data - change 'Country' clumn values to upper case, remove redundant spaces, shorten too long values
df['Country'] = df['Country'].str.upper()
df['Country'] = df['Country'].str.strip()
df['Country'] = df['Country'].str.replace(r'\s+', ' ', regex=True)
df['Country'] = df['Country'].replace('UNITED STATES OF AMERICA', 'USA')


# remove 'NaN' rows:
df_finData = df.dropna(how='all')

# create Spark DataFrame:
sdf_finData = spark.createDataFrame(df_finData)

# print schema
sdf_finData.printSchema()
sdf_finData.head(5)

# in order to run Spark SQL create temporary view:
sdf_finData.createTempView("sdf_newFinData")
spark.sql("SELECT Country, Sales FROM sdf_newFinData").show()

# import necessary libraries and create pandas_udf function - it is an example of vectorized operations in Pandas, leverages low-level C and Cython routines - makes code more efficient
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType

@pandas_udf(FloatType())
def tax_calc(sales: pd.Series, country: pd.Series) -> pd.Series:
    # Create a Series for tax rate based on country
    tax_rate = pd.Series(0.10, index=sales.index)  # Default tax rate
    tax_rate[country.str.upper() == 'CANADA'] = 0.20
    tax_rate[country.str.upper() == 'FRANCE'] = 0.25
    tax_rate[country.str.upper() == 'GERMANY'] = 0.30
    tax_rate[country.str.upper() == 'MEXICO'] = 0.45
    tax_rate[country.str.upper() == 'USA'] = 0.50
    
    # Disclaimer: tax rates are imaginary for demonstration purpose, any similarities to real world tax rates is purely coincidental

    # Calculate tax
    return sales * tax_rate

# Register the UDF
spark.udf.register("calculated_tax", tax_calc)

# in order to be able to use the UDF we need to create a register:
spark.udf.register("calculated_tax", tax_calc)

# run Spark SQL  using the UDF:
spark.sql("SELECT Country, FORMAT_NUMBER(Sales, 2) AS Sales, FORMAT_NUMBER(calculated_tax(Sales, Country), 2) as Tax \
            FROM sdf_newFinData").show()

# run Spark SQL using the UDF and also using the GROUP BY statement - this triggers the Shuffle that results in a new Stage (in Spark UI this is visualized in under the DAG Visualization):
spark.sql("SELECT Country, \
                  FORMAT_NUMBER(SUM(Sales), 2) as TotalSales, \
                  FORMAT_NUMBER(SUM(calculated_tax(Sales, Country)), 2) AS TotalTax \
          FROM sdf_newFinData GROUP BY Country").show()


