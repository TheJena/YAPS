operations = {
    "Plot scatterplot of longitude vs latitude with hue as median_house_value": (
        "Create a scatterplot of longitude vs latitude with hue as median_house_value using seaborn",
        'sns.scatterplot(data=df, x="longitude", y="latitude", hue="median_house_value")',
    ),
    "Split data into training and test sets": (
        "Split the data into training and test sets using train_test_split from sklearn.model_selection",
        'X_train, X_test, y_train, y_test = train_test_split(df[["latitude", "longitude"]], df[["median_house_value"]], test_size=0.33, random_state=0)',
    ),
    "Normalize the training data": (
        "Normalize the training data using normalize from sklearn.preprocessing",
        "X_train_norm = preprocessing.normalize(X_train)",
    ),
    "Fit KMeans model to normalized training data": (
        "Fit a KMeans model to the normalized training data using KMeans from sklearn.cluster",
        "kmeans = KMeans(n_clusters=3, random_state=0); kmeans.fit(X_train_norm)",
    ),
    "Plot scatterplot of longitude vs latitude with hue as cluster labels": (
        "Create a scatterplot of longitude vs latitude with hue as cluster labels using seaborn",
        'sns.scatterplot(data=X_train, x="longitude", y="latitude", hue=kmeans.labels_)',
    ),
    "Add cluster labels to the original DataFrame": (
        "Add cluster labels to the original DataFrame by predicting on the normalized data using the KMeans model",
        'df["cluster"] = kmeans.predict(preprocessing.normalize(df[["latitude", "longitude"]])))',
    ),
    "Sample 50 points from each cluster": (
        "Sample 50 points from each cluster and concatenate them into a new DataFrame",
        'sampled_df_list = []; for cluster_id in df["cluster"].unique(): cluster_data = df[df["cluster"] == cluster_id]; sampled_data = (cluster_data.sample(n=50, random_state=0) if len(cluster_data) > 50 else cluster_data); sampled_df_list.append(sampled_data); sampled_df = pd.concat(sampled_df_list, ignore_index=True)',
    ),
    "Remove rows in df that are not present in sampled_df": (
        "Find indices of rows in df that are not present in sampled_df and remove them using drop",
        "indices_to_drop = []; for index, row in df.iterrows(): if not ((sampled_df == row).all(axis=1)).any(): indices_to_drop.append(index); df = df.drop(indices_to_drop)",
    ),
    "Plot scatterplot of longitude vs latitude with hue as cluster labels": (
        "Create a scatterplot of longitude vs latitude with hue as cluster labels using seaborn",
        'sns.scatterplot(data=df, x="longitude", y="latitude", hue="cluster")',
    ),
}
