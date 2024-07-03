import sys

import argparse
import pandas as pd
import numpy as np
import subprocess
from misc.logger import CustomLogger

from prov_acquisition.prov_libraries.tracker import ProvenanceTracker
from prov_acquisition.prov_libraries.pipeline_standardizer import (
    PipelineStandardizer,
)


def get_args() -> argparse.Namespace:
    """
    Parses command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Real worlds pipelines - Census Pipeline"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="../../demos/real_world_pipelines/datasets/census.csv",
        help="Relative path to the dataset file",
    )
    parser.add_argument(
        "--frac", type=float, default=0.0, help="Sampling fraction [0.0 - 1.0]"
    )

    return parser.parse_args()


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


def run_pipeline(args) -> None:
    logger = CustomLogger("ProvenanceTracker")

    input_path = args.dataset

    df = pd.read_csv(input_path)

    # Assign names to columns
    if len(df.columns) == 15:
        names = [
            "age",
            "workclass",
            "fnlwgt",
            "education",
            "education-num",
            "marital-status",
            "occupation",
            "relationship",
            "race",
            "sex",
            "capital-gain",
            "capital-loss",
            "hours-per-week",
            "native-country",
            "label",
        ]
        df.columns = names

    if args.frac > 0.0 and args.frac < 1.0:
        df = stratified_sample(df, args.frac)
        logger.info(
            f"The dataframe was stratified and sampled ({args.frac * 100}%)"
        )
    elif args.frac > 1.0:
        df = pd.concat([df] * int(args.frac), ignore_index=True)
        logger.info(
            f"The dataframe has been enlarged by ({int(args.frac)} times"
        )

    # Create provenance tracker
    tracker = ProvenanceTracker(save_on_neo4j=True)

    # Subscribe dataframe
    df = tracker.subscribe(df)

    logger.info(f" OPERATION C0")

    # Strip whitespace from object columns
    object_columns = df.select_dtypes(include=["object"]).columns.tolist()
    df[object_columns] = df[object_columns].applymap(str.strip)
    tracker.analyze_changes(df)
    logger.info(f" OPERATION C1 ")

    # Replace '?' with NaN
    df = df.replace("?", np.nan)
    tracker.analyze_changes(df)
    logger.info(f" OPERATION C2 ")

    # One-hot encoding for categorical columns
    tracker.dataframe_tracking = False  # to have the missing link for now
    categorical_columns = df.select_dtypes(include=["object"]).columns.tolist()
    for i, col in enumerate(categorical_columns):
        dummies = pd.get_dummies(df[col])
        df_dummies = dummies.add_prefix(col + "_")
        df = df.join(df_dummies)
        if i == len(categorical_columns) - 1:
            tracker.dataframe_tracking = True
        df = df.drop([col], axis=1)
    tracker.analyze_changes(df)
    logger.info(f" OPERATION C3 - ")

    # Replace specific values in 'sex' and 'label' columns
    if "sex" in df.columns and "label" in df.columns:
        df = df.replace(
            {"sex": {"Male": 1, "Female": 0}, "label": {"<=50K": 0, ">50K": 1}}
        )
    tracker.analyze_changes(df)
    logger.info(f" OPERATION C4 -")

    # Drop 'fnlwgt' and 'age' columns
    if "fnlwgt" in df.columns:
        df = df.drop(["fnlwgt", "age"], axis=1)
    tracker.analyze_changes(df)


if __name__ == "__main__":
    run_pipeline(get_args())
