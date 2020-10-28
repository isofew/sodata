from pyspark.sql import SparkSession

def get_spark(appName, mem, cores):
    return (
        SparkSession
        .builder
        .config('spark.driver.memory', f'{mem}g') 
        .config('spark.sql.session.timeZone', 'UTC')
        .appName('count_per_user_bin')
        .master(f'local[{cores}]')
        .getOrCreate()
    )
