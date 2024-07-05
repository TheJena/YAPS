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
    df = tracker.subscribe(df)

    # Imputation
    df = df.fillna(0)

    # Feature rename
    df = df.rename({"key1": "chiave1"}, axis=1)
    df = df.rename({"chiave1": "chiave01"}, axis=1)

    df = df.drop(["key2"], axis=1)

    df["C"] = [0, 1, 2, 3, 4]

    # Feature transformation of column D
    df["D"] = df["D"].apply(lambda x: x * 2)

    df = df.groupby("A").sum()

    # Imputation 2
    df = df.fillna(10)

    # Space transformation 1
    c = "D"
    dummies = pd.get_dummies(df[c])
    df_dummies = dummies.add_prefix(c + "_")
    df = df.join(df_dummies)
    df = df.drop([c], axis=1)

    df = df.drop(["B"], axis=1)
