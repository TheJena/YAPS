import sys


import argparse
import pandas as pd
from graph.logger import CustomLogger
import numpy as np


def stratified_sample(df, frac):
    """
    Perform stratified sampling on a DataFrame.
    """
    if frac > 0.0 and frac < 1.0:
        # Infer categorical columns for stratification
        stratify_columns = df.select_dtypes(
            include=["object"]
        ).columns.tolist()

        # Check if any class in stratify columns has fewer than 2 members
        for col in stratify_columns:
            value_counts = df[col].value_counts()
            if value_counts.min() >= 2:
                # Perform stratified sampling for this column
                stratified_df = df.groupby(col, group_keys=False).apply(
                    lambda x: x.sample(frac=frac)
                )
                return stratified_df.reset_index(drop=True)

        # If no suitable stratification column is found, fall back to random sampling
        sampled_df = df.sample(frac=frac).reset_index(drop=True)
    else:
        sampled_df = df
    return sampled_df


def run_pipeline(tracker) -> None:

    df = pd.DataFrame(
        {
            "key1": [1, 2, 3, 4, 5],
            "key2": [0, np.nan, 2, 1, 1],
            "A": [0, 1, 0, 3, 0],
            "D": [0, 1, np.nan, 2, 3],
            "B": [0, 1, 0, 3, 0],
        }
    )
    logger = CustomLogger("Provenancetracker")

    # Subscribe dataframe
    # Subscribe the dataframe to the tracker
    df = tracker.subscribe(df)
    tracker.analyze_changes(df)

    # Imputation
    # Replace missing values with 0
    df = df.fillna(0)
    tracker.analyze_changes(df)

    # Feature rename
    # Rename column 'key1' to 'chiave01'
    df = df.rename({"key1": "chiave1"}, axis=1)
    df = df.rename({"chiave1": "chiave01"}, axis=1)
    tracker.analyze_changes(df)

    # Drop column 'key2'
    df = df.drop(["key2"], axis=1)
    tracker.analyze_changes(df)

    # Add new column 'C'
    df["C"] = [0, 1, 2, 3, 4]
    tracker.analyze_changes(df)

    # Feature transformation of column D
    # Multiply values in column 'D' by 2
    df["D"] = df["D"].apply(lambda x: x * 2)
    tracker.analyze_changes(df)

    # Groupby and sum
    # Group the dataframe by column 'A' and perform a sum operation
    df = df.groupby("A").sum()
    tracker.analyze_changes(df)

    # Imputation 2
    # Replace missing values with 10
    df = df.fillna(10)
    tracker.analyze_changes(df)

    # Space transformation 1
    # Create dummy variables for column 'D'
    c = "D"
    dummies = pd.get_dummies(df[c])
    df_dummies = dummies.add_prefix(c + "_")
    df = df.join(df_dummies)
    df = df.drop([c], axis=1)
    tracker.analyze_changes(df)

    # Drop column 'B'
    df = df.drop(["B"], axis=1)
    tracker.analyze_changes(df)
