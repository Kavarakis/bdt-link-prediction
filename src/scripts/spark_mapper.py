import os
import glob
from scipy.io import loadmat
import networkx as nx
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import csv


spark = SparkSession.builder.appName("Graph Processing").getOrCreate()

s3 = boto3.client("s3")

# Assumes your S3 bucket and key structure
bucket_name = "hadoop-project-burch"
input_prefix = "dataset/facebook100/"
output_prefix = "output/"


def list_s3_files(prefix):
    """List files in an S3 bucket under a prefix"""
    files = []
    resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    for obj in resp["Contents"]:
        files.append(obj["Key"])
    return files


def read_mat_file_s3(key):
    """Read a .mat file from S3"""
    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = response["Body"].read()
    return loadmat(BytesIO(content))


def write_df_to_s3(df, key):
    """Write a DataFrame to a CSV on S3"""
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer)
    s3.put_object(Bucket=bucket_name, Body=csv_buffer.getvalue(), Key=key)


"""
A parser for the Facebook100 dataset.
Data available on https://archive.org/details/oxford-2005-facebook-matrix.

Each of the school .mat files has an A matrix (sparse) and a
"local_info" variable, one row per node: a student/faculty status
flag, gender, major, second major/minor (if applicable), dorm/house,
year, and high school. Missing data is coded 0.

"""

PATH = "./dataset/facebook100"


node_attributes = [
    "student_fac",
    "gender",
    "major_index",
    "second_major",
    "dorm",
    "year",
    "high_school",
]

attribute_dict = {
    "student_fac": 0,
    "gender": 1,
    "major_index": 2,
    "second_major": 3,
    "dorm": 4,
    "year": 5,
    "high_school": 6,
}


def get_attribute_partition(matlab_object, attribute):
    attribute_rows = matlab_object["local_info"]

    try:
        index = attribute_dict[attribute]
    except KeyError:
        raise KeyError(
            "Given attribute "
            + attribute
            + " is not a valid choice.\nValid choices include\n"
            + str(attribute_dict.keys())
        )

    current_id = 0
    values = dict()
    for row in attribute_rows:
        if not (len(row) == 7):
            raise ValueError(
                "Row "
                + str(current_id)
                + " has "
                + str(len(row))
                + " rather than the expected 7 rows!"
            )

        val = row[index]
        values[current_id] = int(val)
        current_id += 1

    return values


def graph_to_dataframes(G, node_attributes):
    print("Starting conversion process...")

    if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
        print("Warning: The graph is empty.")
        return None, None

    print("Converting nodes to DataFrame...")
    nodes = []
    for node in G.nodes():
        node_data = {"node_id": str(node)}
        for attr in node_attributes:
            if attr not in G.nodes[node]:
                print(f"Attribute '{attr}' not found for node {node}. Setting as None.")
            node_data[attr] = G.nodes[node].get(attr, None)

        nodes.append(node_data)

    nodes_schema = StructType(
        [StructField("node_id", StringType(), True)]
        + [StructField(attr, IntegerType(), True) for attr in node_attributes]
    )
    nodes_df = spark.createDataFrame(nodes, schema=nodes_schema)

    print("Converting edges to DataFrame...")
    edges = []
    for edge in G.edges():
        edges.append({"source": str(edge[0]), "target": str(edge[1])})

    edges_schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("target", StringType(), True),
        ]
    )
    edges_df = spark.createDataFrame(edges, schema=edges_schema)

    return nodes_df, edges_df


# NETWORKS_NUMBER = 2
if __name__ == "__main__":
    matlab_files = list_s3_files(bucket_name, input_prefix)

    for counter, matlab_filename in enumerate(matlab_files):
        network_name = (
            matlab_filename.strip(".").strip("/").split("/")[-1].split(".")[0]
        )
        print("//::", network_name)
        print("Now parsing:", network_name)
        matlab_object = loadmat(matlab_filename)
        scipy_sparse_graph = matlab_object["A"]
        G = nx.from_scipy_sparse_array(scipy_sparse_graph)
        print("Number of nodes:", G.number_of_nodes())
        print("Number of edges:", G.number_of_edges())

        for attribute in attribute_dict:
            values = get_attribute_partition(matlab_object, attribute)
            for node in values:
                G.nodes[node][attribute] = values[node]

        nodes_df, edges_df = graph_to_dataframes(G, node_attributes)
        write_df_to_s3(nodes_df, "./dataset/data/" + network_name + "_nodes")
        write_df_to_s3(edges_df, "./dataset/data/" + network_name + "_edges")
