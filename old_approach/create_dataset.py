from pyspark.sql import SparkSession
import random
import numpy as np
import networkx as nx
import os
import datetime
import pandas as pd

SEED = 21
attribute_dict = {
    "student_fac": 0,
    "gender": 1,
    "major_index": 2,
    "second_major": 3,
    "dorm": 4,
    "year": 5,
    "high_school": 6,
}
PERCENTAGE = 0.01

spark = SparkSession.builder.appName("bdt-link-prediction").getOrCreate()


def read_graph(file_path):
    # Read graph from .net file
    G = nx.read_pajek(file_path + ".net")
    G = nx.Graph(nx.to_undirected(G))

    G = nx.convert_node_labels_to_integers(G)

    # Read attibutes from .nodelist file
    f = open(file_path + ".nodelist", "r")
    f.readline()
    for line in f.read().split("\n"):
        if line == "":
            break

        attributes = list(map(int, line.split("\t")))
        node_id = attributes[0]
        attributes = attributes[1:]

        for key in attribute_dict:
            index = attribute_dict[key]
            G.nodes[node_id][index] = attributes[index]

    f.close()
    return G


def set_baseline_features(from_dict, to_dict, mean_year, label):
    dorms = [from_dict[attribute_dict["dorm"]], to_dict[attribute_dict["dorm"]]]
    is_dorm = int(dorms[0] == dorms[1])
    if min(dorms) == 0:
        is_dorm = 0
        # is_year, year_diff
    if (from_dict[attribute_dict["year"]]) == 0:
        from_dict[attribute_dict["year"]] = mean_year
    if (to_dict[attribute_dict["year"]]) == 0:
        to_dict[attribute_dict["year"]] = mean_year
    years = [from_dict[attribute_dict["year"]], to_dict[attribute_dict["year"]]]
    is_year = int(years[0] == years[1])
    year_diff = abs(int(years[0] - years[1]))

    # from_high_school, to_high_school
    high_schools = [
        from_dict[attribute_dict["high_school"]],
        to_dict[attribute_dict["high_school"]],
    ]
    from_high_school = min(high_schools)
    to_high_school = max(high_schools)

    # from_major, to_major
    majors = [
        from_dict[attribute_dict["major_index"]],
        to_dict[attribute_dict["major_index"]],
    ]
    from_major = min(majors)
    to_major = max(majors)

    # is_faculty
    faculties = [
        from_dict[attribute_dict["student_fac"]],
        to_dict[attribute_dict["student_fac"]],
    ]
    is_faculty = int(faculties[0] == faculties[1])
    if min(faculties) == 0:
        is_faculty = 0

    # is_gender
    genders = [
        from_dict[attribute_dict["gender"]],
        to_dict[attribute_dict["gender"]],
    ]
    is_gender = int(genders[0] == genders[1])
    if min(genders) == 0:
        is_gender = 0
    return (
        from_dict["id"],
        to_dict["id"],
        is_dorm,
        is_year,
        year_diff,
        from_high_school,
        to_high_school,
        from_major,
        to_major,
        is_faculty,
        is_gender,
        label,
    )


def create_test_links_spark(G, percentage):
    graph_dict = nx.to_dict_of_lists(G)
    nodes = list(G.nodes())
    edges = list(G.edges())
    test_edges = int(G.number_of_edges() * percentage)
    mean_year = []
    for n in nodes:
        year = G.nodes[n][attribute_dict["year"]]
        if year != 0:
            mean_year.append(year)
    mean_year = np.mean(mean_year)

    def baseline_mapper(x, y, label):
        nodes = G.nodes()
        return set_baseline_features(nodes[x], nodes[y], mean_year, label)

    data = map(
        lambda x: baseline_mapper(x[0], x[1], True),
        random.sample(list(G.edges()), test_edges),
    )
    positive_rdd = spark.sparkContext.parallelize(data)
    del data
    negative = set()
    while len(negative) < test_edges:
        x, y = random.sample(nodes, 2)
        if (x not in graph_dict[y] or y not in graph_dict[x]) and (
            (y, x) not in negative or (x, y) not in negative
        ):
            negative.add(baseline_mapper(x, y, False))
    negative_rdd = spark.sparkContext.parallelize(negative)
    del negative
    del nodes
    del edges
    del graph_dict
    feature_names = [
        "from_id",
        "to_id",
        "is_dorm",
        "is_year",
        "year_diff",
        "from_high_school",
        "to_high_school",
        "from_major",
        "to_major",
        "is_faculty",
        "is_gender",
        "label",
    ]
    df1 = negative_rdd.toDF(feature_names)
    df2 = positive_rdd.toDF(feature_names)
    df = df1.union(df2)
    negative_rdd.unpersist()
    positive_rdd.unpersist()
    return df


if __name__ == "__main__":
    output_path = "./dataset/output"
    performance = []
    for input_path in os.listdir("./dataset/data"):
        print(f"reading for {input_path}")
        print("Reading graph ...")
        start = datetime.datetime.now()

        G = read_graph("./dataset/data/" + input_path + "/" + input_path)
        print("Creating test links...")

        df = create_test_links_spark(G, PERCENTAGE)
        df.write.csv(output_path + "/" + input_path, header=True)
        end = datetime.datetime.now()
        result = (end - start).total_seconds() * 1000
        print("Time elapsed:", result)
        df.unpersist()
        print(f"Finished for {input_path}")
        performance.append((input_path, result))
    pd.DataFrame(performance, columns=["network", "time"]).to_csv("results-old.csv")
