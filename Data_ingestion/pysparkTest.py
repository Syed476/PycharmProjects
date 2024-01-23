import unittest
import pyspark


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[2]").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
class SimpleTestCase(PySparkTestCase):

    def test_with_rdd(self):
        test_input = [
            ' hello spark ',
            ' hello again spark spark'
        ]

        input_rdd = self.sc.parallelize(test_input, 1)

        from operator import add

        results = input_rdd.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(add).collect()
        self.assertEqual(results, [('hello', 2), ('spark', 3), ('again', 1)])

    def test_with_df(self):
        df = self.spark.createDataFrame(data=[[1, 'a'], [2, 'b']],
                                        schema=['c1', 'c2'])
        self.assertEqual(df.count(), 2)