from jobs.spark_create_data_class import SparkCreateData

if __name__ == "__main__":
    # converter = SparkConvertMatToCSV()
    # converter.run()
    data_creator = SparkCreateData()
    data_creator.run()
