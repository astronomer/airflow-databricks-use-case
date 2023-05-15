Astro Databricks provider tutorial
==================================

This repository contains the DAG code used in the Astronomer Learn tutorial [Orchestrate Databricks jobs with Airflow](https://docs.astronomer.io/learn/airflow-databricks). The DAG uses both the Astro Databricks provider as well as the Astro Python SDK.

# How to use this repository

This section explains how to run this repository with Airflow. Note that you will need to define extra connections (AWS, Databricks and a connection to a relational database). See the tutorial  for instructions. The code used in the Databricks notebooks is available in the `databricks_notebook_code` folder.

## Option 1: Use GitHub Codespaces

Run this Airflow project without installing anything locally.

1. Fork this repository.
2. Create a new GitHub codespaces project on your fork. Make sure it uses at least 4 cores!
3. After creating the codespaces project the Astro CLI will automatically start up all necessary Airflow components. This can take a few minutes. 
4. Once the Airflow project has started, access the Airflow UI by clicking on the **Ports** tab and opening the forward URL for port 8080.

## Option 2: Use the Astro CLI

Download the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) to run Airflow locally in Docker. `astro` is the only package you will need to install.

1. Run `git clone https://github.com/astronomer/2-6-example-dags.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.

## Resources

- [Orchestrate Databricks jobs with Airflow tutorial](https://docs.astronomer.io/learn/airflow-databricks).
- [Databricks 14 day free trial](https://www.databricks.com/try-databricks).
- [Astro Databricks provider repository](https://github.com/astronomer/astro-provider-databricks/).
- [Astro Databricks provider documentation](https://astronomer.github.io/astro-provider-databricks/).
- [Astro Python SDK tutorial](https://docs.astronomer.io/learn/astro-python-sdk).
- [Astro Python SDK documentation](https://astro-sdk-python.readthedocs.io/en/stable/index.html).
- [Astro Python SDK repository](https://github.com/astronomer/astro-sdk).