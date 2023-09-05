
from datavalidation_report import validation_report
from pyspark.sql.functions import *






def validation(stage_data,source_df,log):



    ##### getting count of dataframe
    log.info("getting count of datafram")
    src_count_df = source_df.count()
    stage_count_df=stage_data.count()
    if src_count_df==stage_count_df:
        validation_count_df="PASS"
    else:
        validation_count_df="FAIL"


    #### dropping the null in the dataframe
    log.info("dropping the null in the dataframe")

    src_drop_null_df = source_df.na.drop()
    src_drop_null_df_count = src_drop_null_df.count()
    stage_drop_null_df = stage_data.na.drop()
    stage_drop_null_df_count = stage_drop_null_df.count()

    if src_drop_null_df_count==stage_drop_null_df_count:
        validation_drop_null_df_count="PASS"
    else:
        validation_drop_null_df_count="FAIL"

    #### to show the null columns
    log.info("")

    src_null_rows = source_df.exceptAll(src_drop_null_df)
    src_null_rows_count = src_null_rows.count()
    stage_null_rows = stage_data.exceptAll(stage_drop_null_df)
    stage_null_rows_count = stage_null_rows.count()

    if src_null_rows_count==stage_null_rows_count:
        validation_null_rows_count="PASS"
    else:
        validation_null_rows_count="FAIL"

    # Find and show duplicate rows
    # drop_null_df.count()
    src_unique_rows = src_drop_null_df.dropDuplicates()
    src_unique_rows_count = src_unique_rows.count()
    # unique_rows.count()
    src_duplicate_rows = src_drop_null_df.exceptAll(src_drop_null_df.dropDuplicates())
    src_duplicate_rows_count = src_duplicate_rows.count()


    # Find and show duplicate rows
    # drop_null_df.count()
    stage_unique_rows = stage_drop_null_df.dropDuplicates()
    stage_unique_rows_count = stage_unique_rows.count()
    # unique_rows.count()
    stage_duplicate_rows = stage_drop_null_df.exceptAll(stage_drop_null_df.dropDuplicates())
    stage_duplicate_rows_count = stage_duplicate_rows.count()

    if src_unique_rows_count==stage_unique_rows_count:
        validation_unique_rows_count="PASS"
    else:
        validation_unique_rows_count="FAIL"

    if src_duplicate_rows_count==stage_duplicate_rows_count:
        validation_duplicate_rows_count="PASS"
    else:
        validation_duplicate_rows_count="FAIL"


    src_data = {"Total Rows": src_count_df, "Non-NULL Rows": src_drop_null_df_count, "NULL Rows": src_null_rows_count,
                "Unique Records": src_unique_rows_count, "Duplicate Records": src_duplicate_rows_count}
    stage_data = {"Total Rows": stage_count_df, "Non-NULL Rows": stage_drop_null_df_count,
                  "NULL Rows": stage_null_rows_count,
                  "Unique Records": stage_unique_rows_count, "Duplicate Records": stage_duplicate_rows_count}



    validation_data = {"Total Rows": validation_count_df, "Non-NULL Rows": validation_drop_null_df_count,
                  "NULL Rows": validation_null_rows_count,
                  "Unique Records": validation_unique_rows_count, "Duplicate Records": validation_duplicate_rows_count}

    validation_report(src_data,stage_data,validation_data,log)






