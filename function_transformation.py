from snowflakedata import query_processing
from datavalidation import validation
import configparser
def transform_func(dfs_dict_lst,spark,log,trans_df_lst,base_dir):
    configfilepath = base_dir + "config.ini"
    config = configparser.ConfigParser()
    config.read(configfilepath)
    url = config['SFConfig']['Sfurl']
    user = config['SFConfig']['Sfuser']
    password = config['SFConfig']['Sfpassword']
    database = config['SFConfig']['Sfdatabase']
    schema = config['SFConfig']['Sfschema']
    dbtable = config['SFConfig']['Dbtable']
    warehouse = config['SFConfig']['Sfwarehouse']
    # globals().update(dfs_dict_lst)
    a = [x for x in dfs_dict_lst.values()]
    b = a[0]
    result_df = a[0]  # Initialize with the first DataFrame
    for df in a[1:]:
        result_df = result_df.union(df)

    print(result_df.count(),"test1")

    result_df.write.format("net.snowflake.spark.snowflake") \
        .options(sfUrl=url,
                 sfUser=user,
                 sfPassword=password,
                 sfDatabase=database) \
        .option("dbtable", dbtable) \
        .mode("overwrite").options(header=True).save()
    log.info("Raw Data is successfully moved to stagging table ")
    query='select * from {};'
    stage_data = spark.read.format("net.snowflake.spark.snowflake") \
        .options(sfUrl=url,
                 sfUser=user,
                 sfPassword=password,
                 sfDatabase=database )\
        .option("query", query.format(dbtable)) \
        .option("sfWarehouse", "COMPUTE_WH") \
        .load()
    validation(stage_data,result_df,log)
    df=query_processing(spark,base_dir)




    return df










