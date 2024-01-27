# Friend Recommendations Based on Mutual Friends

This Databricks notebook implements a friend recommendation system based on mutual friends using Apache Spark.

## Overview

The notebook performs the following steps:

1. **Data Loading**: Loads the dataset containing user-friend relationships from a text file.
2. **Data Preprocessing**: Processes the data to create a structure representing each user and their friends.
3. **Mutual Friend Structure Creation**: Creates a structure to represent mutual friends between users.
4. **Recommendation Generation**: Generates friend recommendations for users based on mutual friends.
5. **Output**: Stores the recommendations in a text file.

## Usage

1. Ensure that the dataset containing user-friend relationships is available in the specified location.
2. Run each cell in the notebook sequentially to execute the code.
3. Modify the input and output file paths as needed.
4. Check the output file for the generated recommendations.

## Requirements

- Apache Spark
- Databricks environment or Apache Spark setup
- Python

## Credits

This notebook is inspired by various tutorials and resources on Apache Spark and Databricks.

- [Databricks Documentation](https://docs.databricks.com/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/index.html)
