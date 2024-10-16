operations = {
    "Drop rows with missing values": (
        "Remove rows from the dataframe that contain missing values.",
        "df = df.dropna()",
    ),
    "Impute missing values in numerical columns": (
        "Replace missing values in numerical columns with the mean value of that column.",
        'imputer = SimpleImputer(missing_values=np.nan, strategy="mean"); df.iloc[:, 1:3] = imputer.fit_transform(df.iloc[:, 1:3])',
    ),
    "Apply OneHotEncoder to categorical column": (
        "Convert a categorical column into numerical columns using one-hot encoding.",
        'ct = ColumnTransformer(transformers=[("encoder", OneHotEncoder(), [0])], remainder="passthrough"); df = pd.DataFrame(ct.fit_transform(df))',
    ),
    "Ensure column names are maintained or regenerated": (
        "Update column names after transformation to maintain consistency and readability.",
        'df.columns = [f"feature_{i}" for i in range(df.shape[1])]',
    ),
}
