operations = {
    "Strip whitespace from categorical columns and replace ? with 0": (
        "Remove leading and trailing whitespaces from categorical columns and replace '?' values with '0'",
        "df[columns] = df[columns].applymap(str.strip)\ndf = df.replace('?', 0)",
    ),
    "One-hot encode education column": (
        "Perform one-hot encoding on the 'education' column to transform it into numerical features",
        "dummies = pd.get_dummies(df[col])\ndf_dummies = dummies.add_prefix(col + '_')\ndf = df.join(df_dummies)\ndf = df.drop([col], axis=1)",
    ),
    "Replace sex and label columns with numerical values": (
        "Map 'sex' and 'label' columns to numerical values for further processing",
        "df = df.replace({'sex': {'Male': 1, 'Female': 0}, 'label': {'<=50K': 0, '>50K': 1}})",
    ),
    "Drop fnlwgt column": (
        "Remove the 'fnlwgt' column as it's not necessary for the analysis",
        "df = df.drop(['fnlwgt'], axis=1)",
    ),
    "Rename hours-per-week column to hw": (
        "Rename the 'hours-per-week' column to a shorter name 'hw' for simplicity",
        "df = df.rename(columns={'hours-per-week': 'hw'}, inplace=True)",
    ),
}
