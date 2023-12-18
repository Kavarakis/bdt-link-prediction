# bdt-link-prediction

Repository for Big Data Techologies project (DataScience Master Programme @BurchUniversity)
with Hadoop and Link Prediction on facebook100 dataset

## Data

used data is obtained from this [paper](https://archive.org/details/oxford-2005-facebook-matrix).
Since data is available in MATLAB matrix format and recorded as graph, it is required to:

- use dataset_parser to parse .mat files to .net and .nodelist formats native to network graphs and py (standardized networkx package for working with networks)
- parse it to dataset (Pandas readable) with baseline feature and topology. Use create_dataset.py
