operations = {
    "Replace categorical values": (
        "Replace specific categorical values in the dataframe.",
        """df = df.replace(
            {
                "checking": {
                    "A11": "check_low",
                    "A12": "check_mid",
                    "A13": "check_high",
                    "A14": "check_none",
                },
                # ...
            }
        )""",
    ),
    "Create a new column for marital status": (
        "Create a new column 'status' based on the 'personal_status' column.",
        """df["status"] = (
            df["personal_status"]
            .map(
                {
                    "A91": "divorced",
                    "A92": "divorced",
                    "A93": "single",
                    "A95": "single",
                }
            )
            .fillna("married")
        )""",
    ),
    "Translate gender": (
        "Translate the 'personal_status' column to a binary value.",
        """df["personal_status"] = np.where(
            df.personal_status == "A92",
            0,
            np.where(df.personal_status == "A95", 0, 1),
        )""",
    ),
    "Drop 'personal_status' column": (
        "Drop the 'personal_status' column from the dataframe.",
        """df = df.drop(["personal_status"], axis=1)""",
    ),
    "One-hot encoding for categorical features": (
        "Perform one-hot encoding on specific categorical columns.",
        """columns = [
            "checking",
            "credit_history",
            # ...
        ]
        for col in columns:
            df_dummies = pd.get_dummies(df[col], prefix=f"{col}_")
            df = df.join(df_dummies)
            df = df.drop([col], axis=1)""",
    ),
}
