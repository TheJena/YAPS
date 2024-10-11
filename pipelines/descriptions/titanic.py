operations = {
    "Drop unnecessary columns": (
        "Remove specified columns from the DataFrame",
        "df = df.drop(cols, axis=1)",
    ),
    "Remove rows with missing values": (
        "Delete rows containing missing values from the DataFrame",
        "df = df.dropna()",
    ),
    "One-hot encode categorical variables and drop original columns": (
        "Perform one-hot encoding on categorical columns and remove the original columns",
        """for i, col in enumerate(cols):
    dummies = pd.get_dummies(df[col])
    df_dummies = dummies.add_prefix(col + \"_\")
    df = df.join(df_dummies)
    df = df.drop([col], axis=1)""",
    ),
}
