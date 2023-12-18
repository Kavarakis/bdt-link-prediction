# bdt-link-prediction

Repository for Big Data Techologies project (DataScience Master Programme @BurchUniversity)
with Hadoop and Link Prediction on facebook100 dataset

## Data

Extract data to folder dataset.

Mat files should be on path `./dataset/facebook100/facebook100/.mat`
Data used is obtained from this [paper](https://archive.org/details/oxford-2005-facebook-matrix).
Since data is available in MATLAB matrix format and recorded as graph, it is required to:

- use `src/scripts/dataset_parser.py` to parse .mat files to .net and .nodelist formats native to network graphs and py (standardized networkx package for working with networks)
- parse it to dataset (Pandas readable) with baseline feature and topology. Use `src/scripts/create_dataset.py`

**NOTE**: in `src/scripts/dataset_parser.py` is defined `NETWORKS_NUMBER = 2`, which is a constant for parsing user defined number of networks. Facebook100, has **100** networks.
