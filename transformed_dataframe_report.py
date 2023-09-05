import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql.functions import *

# Assuming df is your PySpark DataFrame
## transformed df will be the transformed table stored in snowflake
def dataframe_report(transformed_df,baseurl):
    masked_transformed_df = transformed_df.withColumn("avg").sha2(col("avg"),256)

    pandas_df = transformed_df.toPandas()


# Create a horizontal grouped bar chart with multiple value mapping
    bar_width = 0.4
    index = range(len(pandas_df['location']))
    bar_positions = [index, [i + bar_width for i in index], [i + 2 * bar_width for i in index]]

    plt.barh(bar_positions[0], pandas_df['min'], height=bar_width, label='Min')
    plt.barh(bar_positions[1], pandas_df['max'], height=bar_width, label='Max')
    # plt.barh(bar_positions[2], df['Value3'], height=bar_width, label='Value3')

    plt.yticks([i + bar_width for i in index], pandas_df['location'])
    plt.xlabel('Min and Max Score')
    plt.ylabel('location')
    plt.title('Location based Min and Max score for Universities available')
    plt.legend()

    # Save the chart as an image file (e.g., PNG)
    plt.savefig(baseurl+'barplot.png', bbox_inches='tight')