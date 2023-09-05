from function_transformation import transform_func
import config_input_arguments
import json
from datetime import datetime
import pytz
from configobj import ConfigObj
from pyspark.sql import *
from pyspark import SparkConf
from log import logger
from transformation_report import transformation_graph
import  sys

# sys.path.append('C:/Users/dineshka/Desktop/frame')

# import function_transformation

class ingestion_func:
    def file_jdbc_ingestion(self,spark,inb_df_lst,base_dir,log):
        df_dict = {}
        for index in range(len(inb_df_lst)):
            index_dict = inb_df_lst[index]
            if index_dict['Inbound'] == True:
                if index_dict['Source Type'].lower()== 'file':
                    df_file_name = index_dict['Source File Name'].split(".",1)[0]
                elif index_dict['Source Type'].lower()== 'jdbc':
                    df_file_name = index_dict['Source Table']
                if index_dict['Source Type'].lower() == 'file':
                    df = spark.read.format(index_dict['Source File Type'])\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .load(base_dir + index_dict['Source Directory'] + '\\' + index_dict['Source File Name'])

                    log.info("Successfully ingested "+ index_dict['Source File Name'])
                    log.info(df._jdf.schema().treeString())
                    log.info("************DataFrame Content***********")
                    for record in df.limit(5).collect():
                        log.info(record)
                    log.info("********************END*********************")
                elif index_dict['Source Type'].lower() == 'jdbc':

                    df = spark.read.format("jdbc").option('driver',"com.mysql.jdbc.Driver")\
                    .option('hostname',index_dict['Source Hostname'])\
                    .option('url',index_dict['Source URL'])\
                    .option('dbtable',index_dict['Source Table'])\
                    .option('user',index_dict['Source Username'])\
                    .option('password',index_dict['Source Password'])\
                    .option("inferSchema", "true")\
                    .load()
                    log.info("Successfully ingested " + index_dict['Source Table'])
                    log.info(df._jdf.schema().treeString())
                    log.info("************DataFrame Content***********")
                    for record in df.limit(5).collect():
                        log.info(record)
                    log.info("*******************END*********************")

                df_dict[df_file_name] = df

        return df_dict


    def file_jdbc_outbound(self,spark,index_dict,df,base_dir,log):
        df_out_name = index_dict['Target File Name']
        if index_dict['Target Type'].lower() == 'file':
            df.write.format(index_dict['Stored As']).mode('overwrite').option("header", True)\
                .save(base_dir + index_dict['Target Directory'] + '\\' + df_out_name)
        elif index_dict['Target Type'].lower() == 'jdbc':
            df.write.format(index_dict['Target Type']).option('url',index_dict['Target URL'])\
                .option('driver',"com.mysql.jdbc.Driver")\
                .option('dbtable', index_dict['Target Table'])\
                .option('user', index_dict['Target Username'])\
                .option('password',index_dict['Target Password']).save()
        elif index_dict['Target Type'].lower() == 'cloud':
            df.write.format("net.snowflake.spark.snowflake") \
            .options(sfUrl=index_dict['Target URL'],
                     sfUser=index_dict['Target Username'],
                     sfPassword=index_dict['Target Password'],
                     sfDatabase=index_dict['Target Database']) \
            .option("dbtable", index_dict['Target Table']) \
            .mode("overwrite").options(header=True).save()
        transformation_graph(df)

        log.info("Successfully outbounded the transformation data to  " + index_dict['Target Type'])

        return True

    def transformation(self, dfs_dict_lst, base_dir,log,spark,trans_df_lst):
        transformed_df=transform_func(dfs_dict_lst,spark,log,trans_df_lst,base_dir)
        print(transformed_df.count())



        # transformed_df=function_transformation.transform_func(dfs_dict_lst,spark,log,trans_df_lst)
        # print(transformed_df)

        return transformed_df


class main:
    global base_dir

    base_dir  = 'C:/Users/dineshka/PycharmProjects/pythonProject2/updated_ingstn_fw/'

    @staticmethod
    def main_function ():
        global ingstd_dfs, spark, bol_value, log

        current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')
        log_filename = base_dir + "spark_logs" + "/Spark_logs" + current_date + ".log"
        config = ConfigObj(
            'C:/Users/dineshka/PycharmProjects/pythonProject2/updated_ingstn_fw/resources/log4j.properties',
            configspec=False)
        # update a value in the configparser object
        config['value']['log4j.appender.file.File'] = log_filename
        # write the updated configuration to the log4j.properties file
        config.write()
        conf = SparkConf().setAppName('Framework') \
            .set("spark.driver.extraJavaOptions",
                 "-Dlog4j.configuration=file:C:/Users/dineshka/PycharmProjects/pythonProject2/updated_ingstn_fw/resources/log4j.properties")
        # sc = SparkContext(conf = conf)
        spark = SparkSession.builder.master("local[2]").config(conf=conf).appName('Framework').config("spark.jars",'file:///C:/Users/dineshka/PycharmProjects/pythonProject2/updated_ingstn_fw/jar/mysql-connector-java-8.0.13.jar,file:///C:/Users/dineshka/PycharmProjects/pythonProject/Updated_RealTimeSnowflakeIngestion/Spark/jar/snowflake-jdbc-3.13.4.jar,file:///C:/Users/dineshka/PycharmProjects/pythonProject/Updated_RealTimeSnowflakeIngestion/Spark/jar/spark-snowflake_2.12-2.9.0-spark_3.1.jar') \
            .config("spark.master", "local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.1")\
        .config("spark.scheduler.mode", "FAIR") \
 \
        .getOrCreate()
        # .set("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.1") \
        # .config('spark.jars.packages', "org.apache.spark:spark-avro_2.12:3.3.2") \

        # log = logger_init(spark)
        log = logger(base_dir)

        log.info('********** Framework execution started and Spark Session is created **********')

        input_job_type, inb_df, trans_df, out_df = config_input_arguments.input_param_file_parser(base_dir, log)
        print(out_df)

        if input_job_type == []:
            log.info('********** Framework execution ended since there are no proper inputs in input file **********')
            spark.stop()
        else:
            log.info('********** Spark Session is initiated **********')
            if not inb_df.empty:
                inb_df_lst = json.loads(inb_df.to_json(orient='records'))
                outb_df_lst = json.loads(out_df.to_json(orient='records'))
                trans_df_lst = json.loads(trans_df.to_json(orient='records'))
                func_object = ingestion_func()
                ingstd_dfs_dict = func_object.file_jdbc_ingestion(spark, inb_df_lst, base_dir, log)
                log.info("")
                trans_opt = input_job_type[1]
                outb_opt = input_job_type[2]
                if trans_opt['Job Type'] == "Transform" and trans_opt['Value'] is True:
                    transform_output = func_object.transformation(ingstd_dfs_dict, base_dir, log, spark,trans_df_lst)
                    print(transform_output.count(),"final")
                    if outb_opt['Job Type']=="Outbound" and  outb_opt['Value'] is True and transform_output is not None:
                        for trans_df, out_lst in zip(ingstd_dfs_dict.values(), outb_df_lst):

                            func_object.file_jdbc_outbound(spark,out_lst,transform_output,base_dir,log)
                elif outb_opt['Job Type']=="Outbound" and  outb_opt['Value'] is True:
                    for inb_df,out_lst in zip(ingstd_dfs_dict.values(),outb_df_lst):
                        func_object.file_jdbc_outbound(spark,out_lst,inb_df,base_dir,log)


        spark.stop()





