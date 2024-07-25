import pandas as pd
import sys


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


def run_pipeline(args, tracker) -> None:

    input_path = args.dataset
    df = pd.read_csv(input_path)

    # Assign names to columns
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

    if args.frac != 0.0:
        df = df.sample(frac=args.frac)

    # Subscribe dataframe
    df = tracker.subscribe(df)
    tracker.analyze_changes(df)

    # Strip whitespace from categorical columns
    columns = [
        "workclass",
        "education",
        "marital-status",
        "occupation",
        "relationship",
        "race",
        "sex",
        "native-country",
        "label",
    ]
    df[columns] = df[columns].applymap(str.strip)
    tracker.analyze_changes(df)

    # Replace '?' with 0
    df = df.replace("?", 0)
    tracker.analyze_changes(df)

    # One-hot encode 'education' column
    columns = ["education"]
    for i, col in enumerate(columns):
        dummies = pd.get_dummies(df[col])
        df_dummies = dummies.add_prefix(col + "_")
        df = df.join(df_dummies)
        df = df.drop([col], axis=1)
    tracker.analyze_changes(df)

    # Replace categorical values with numerical values
    df = df.replace(
        {"sex": {"Male": 1, "Female": 0}, "label": {"<=50K": 0, ">50K": 1}}
    )
    tracker.analyze_changes(df)

    # Drop 'fnlwgt' column
    df = df.drop(["fnlwgt"], axis=1)
    tracker.analyze_changes(df)

    # Rename 'hours-per-week' column to 'hw'
    df = df.rename(columns={"hours-per-week": "hw"})
    tracker.analyze_changes(df)
