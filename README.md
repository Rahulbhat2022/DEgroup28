# DEgroup28 — Reddit Word par Markov Chain
**Data Engineering I (1TD169) | Uppsala University | Spring 2026**

## Project Overview
Scalable word pair Markov chain language model built on 3.8M Reddit posts
using a 4-node Hadoop/Spark cluster on OpenStack. The pipeline ingests
raw JSON data, converts it to Parquet, extracts bigram frequencies across
the distributed dataset, and benchmarks horizontal and vertical scaling.

## Cluster Architecture
- 1 Master node — HDFS NameNode, YARN ResourceManager, Spark Master
- 3 Worker nodes — HDFS DataNode, YARN NodeManager, Spark Worker
  - worker 1, worker2 , worker3 
- Software: Hadoop 3.3.6, Spark 3.5.8, Python 3.10, Java OpenJDK 11

## Repository Structure
```
DEgroup28/
├── src/
│   ├── convert.py       — JSON to Parquet conversion (run once)
│   ├── analysis.py      — Bigram extraction and model building
│   ├── benchmark.py     — Automated horizontal + vertical scaling experiments
│   └── config.py        — Central configuration (paths, constants)
├── scripts/
│   ├── run_benchmark.sh — Shell script to run and time scaling experiments
│   └── deploy.sh        — Copy code to cluster master
├── notebooks/
│   └── exploration.ipynb — Data exploration and schema analysis
├── data/                — Sample data only (full dataset not included)
├── results/             — Benchmark timing results
├── docs/
│   └── architecture.png — System design diagram
├── requirements.txt
└── .gitignore
```

## Setup & Execution

### Prerequisites
- SSH access to master node
- Hadoop 3.3.6 and Spark 3.5.8 installed on all nodes
- Reddit TLDR dataset uploaded to HDFS at /reddit/slim/

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run Pipeline

**Step 1 — Convert JSON to Parquet (run once):**
```bash
spark-submit --master yarn --num-executors 3 \
  --executor-memory 1g --driver-memory 1g \
  src/convert.py
```

**Step 2 — Run main analysis:**
```bash
spark-submit --master yarn --num-executors 3 \
  --executor-memory 1g --driver-memory 1g \
  src/analysis.py
```

**Step 3 — Run scaling benchmark (horizontal + vertical):**
```bash
python src/benchmark.py
# OR
bash scripts/run_benchmark.sh
```

## Scaling Experiments
benchmark.py runs the analysis job across all combinations of:
- Executors: 1, 2, 3 (horizontal scaling)
- Cores per executor: 1, 2 (vertical scaling)

Results are printed to stdout and saved to results/benchmark_results.txt

## Dataset
Reddit TLDR corpus (Webis-TLDR-17) — 3.8M post-summary pairs.
Not included in repo due to size (19.6GB raw JSON).
Source: https://zenodo.org/records/1043504

Ingest instructions:
```bash
wget -P ~/data/ https://zenodo.org/records/1043504/files/corpus-webis-tldr-17.zip
hdfs dfs -mkdir -p /reddit/raw
hdfs dfs -put ~/data/corpus-webis-tldr-17.json /reddit/raw/
```
