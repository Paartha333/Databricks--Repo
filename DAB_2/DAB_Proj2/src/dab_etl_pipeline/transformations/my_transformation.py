from pyspark import pipelines as dp

def bronze_data():
    df1 = spark.createDateFrame([1, 'a'], [2, 'b'],[3, 'c'],[4, 'd'])
    df2 = df1.toDf('id', 'name')
    return df2