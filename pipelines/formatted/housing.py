from sklearn import preprocessing
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def stratified_sample(df, frac):
    """
    Perform stratified sampling on a DataFrame.
    """
    if frac > 0.0 and frac < 1.0:
        # Infer categorical columns for stratification
        stratify_columns = df.select_dtypes(
            include=["object"],
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

        # If no suitable stratification column is found, fall back to
        # random sampling
        sampled_df = df.sample(frac=frac).reset_index(drop=True)
    else:
        sampled_df = df
    return sampled_df


def run_pipeline(args, tracker) -> None:
    input_path = args.dataset  # "datasets/housing.csv"

    df = pd.read_csv(input_path)

    if args.frac > 0.0 and args.frac < 1.0:
        df = stratified_sample(df, args.frac)
    elif args.frac > 1.0:
        df = pd.concat([df] * int(args.frac), ignore_index=True)

    # Subscribe dataframe
    df = tracker.subscribe(df)

    # Plot scatterplot of longitude vs latitude with hue as median_house_value
    sns.scatterplot(
        data=df,
        x="longitude",
        y="latitude",
        hue="median_house_value",
    )
    plt.show()

    # Split data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(
        df[["latitude", "longitude"]],
        df[["median_house_value"]],
        test_size=0.33,
        random_state=0,
    )

    # Normalize the training data using preprocessing.normalize()
    X_train_norm = preprocessing.normalize(X_train)

    # Fit KMeans model to normalized training data
    kmeans = KMeans(n_clusters=3, random_state=0)
    kmeans.fit(X_train_norm)

    # Plot scatterplot of longitude vs latitude with hue as cluster labels
    sns.scatterplot(
        data=X_train,
        x="longitude",
        y="latitude",
        hue=kmeans.labels_,
    )
    plt.show()

    # Add cluster labels to the original DataFrame
    df["cluster"] = kmeans.predict(
        preprocessing.normalize(df[["latitude", "longitude"]])
    )
    tracker.analyze_changes(df)

    # Sample 50 points from each cluster
    sampled_df_list = []
    for cluster_id in df["cluster"].unique():
        cluster_data = df[df["cluster"] == cluster_id]
        sampled_data = (
            cluster_data.sample(n=50, random_state=0)
            if len(cluster_data) > 50
            else cluster_data
        )
        sampled_df_list.append(sampled_data)

    # Concatenate all sampled data and update the original DataFrame
    sampled_df = pd.concat(sampled_df_list, ignore_index=True)

    # Find indices of rows in df that are not present in sampled_df
    indices_to_drop = []
    for index, row in df.iterrows():
        if not ((sampled_df == row).all(axis=1)).any():
            indices_to_drop.append(index)

    # Remove rows in df that are not present in sampled_df using drop
    df = df.drop(indices_to_drop)
    tracker.analyze_changes(df)

    # Plot scatterplot of longitude vs latitude with hue as cluster labels
    sns.scatterplot(data=df, x="longitude", y="latitude", hue="cluster")
    plt.show()

    return df
