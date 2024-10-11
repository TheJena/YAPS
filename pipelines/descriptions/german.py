operations = [
    {
        "name": "Replace categorical values",
        "description": "Replace specific categorical values in the dataframe.",
        "code": """df = df.replace(
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
    },
    {
        "name": "Create a new column for marital status",
        "description": "Create a new column 'status' based on the 'personal_status' column.",
        "code": """df["status"] = (
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
    },
    {
        "name": "Translate gender",
        "description": "Translate the 'personal_status' column to a binary value.",
        "code": """df["personal_status"] = np.where(
            df.personal_status == "A92",
            0,
            np.where(df.personal_status == "A95", 0, 1),
        )""",
    },
    {
        "name": "Drop 'personal_status' column",
        "description": "Drop the 'personal_status' column from the dataframe.",
        "code": """df = df.drop(["personal_status"], axis=1)""",
    },
    {
        "name": "One-hot encoding for categorical features",
        "description": "Perform one-hot encoding on specific categorical columns.",
        "code": """columns = [
            "checking",
            "credit_history",
            # ...
        ]
        for col in columns:
            df_dummies = pd.get_dummies(df[col], prefix=f"{col}_")
            df = df.join(df_dummies)
            df = df.drop([col], axis=1)""",
    },
]
