import sys


import argparse
import pandas as pd
from graph.logger import CustomLogger
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder


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


def run_pipeline(tracker, frac) -> None:

    input_path = "datasets/generated_dataset.csv"

    # Read the input CSV file into a pandas DataFrame
    df = pd.read_csv(input_path)

    # Sample the DataFrame based on the provided fraction
    if frac > 0.0 and frac < 1.0:
        df = df.sample(frac=frac)
    elif frac > 1.0:
        df = pd.concat([df] * int(frac), ignore_index=True)

    # Subscribe the DataFrame to the tracker
    df = tracker.subscribe(df)

    tracker.analyze_changes(df)

    # Drop rows with missing values
    # Remove rows that contain any NaN values
    df = df.dropna()

    tracker.analyze_changes(df)

    # Separate features and target variable
    # Split the DataFrame into features (all columns except the last) and target variable (last column)
    df = df.iloc[:, :-1]
    y = df.iloc[:, -1]

    tracker.analyze_changes(df)

    # Impute missing values in the numerical columns
    # Replace NaN values in columns 1 and 2 with the mean of the respective columns
    imputer = SimpleImputer(missing_values=np.nan, strategy="mean")
    df.iloc[:, 1:3] = imputer.fit_transform(df.iloc[:, 1:3])

    tracker.analyze_changes(df)

    # Apply OneHotEncoder to the first column
    # Convert the first column into a one-hot encoded format
    ct = ColumnTransformer(
        transformers=[("encoder", OneHotEncoder(), [0])],
        remainder="passthrough",
    )
    df = pd.DataFrame(ct.fit_transform(df))

    tracker.analyze_changes(df)

    # Ensure column names are maintained or regenerated after transformation
    # Assign new column names to the transformed DataFrame
    df.columns = [f"feature_{i}" for i in range(df.shape[1])]

    tracker.analyze_changes(df)
