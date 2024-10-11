operations = {
    "Format car price and mileage columns": (
        "Add 'k' suffix to values greater than or equal to 1000 in 'car_price' and 'car_mileage' columns",
        """df[col] = df[col].apply(
            lambda x: "{:.1f}k".format(x / 1000) if x >= 1000 else x
        )""",
    ),
    "Drop unnecessary columns": (
        "Remove specified columns from the dataframe",
        """df = df.drop(
        [
            "car_drive",
            "car_engine_capacity",
            "car_engine_hp",
            "car_transmission",
        ],
        axis=1,
    )""",
    ),
    "Rename first column to 'car_id'": (
        "Rename the first column of the dataframe to 'car_id'",
        """df.rename(columns={df.columns[0]: "car_id"}, inplace=True)""",
    ),
    "Strip leading and trailing whitespaces from specified columns": (
        "Remove leading and trailing whitespaces from specified columns",
        """df[cols] = df[cols].applymap(str.strip)""",
    ),
    "Create new column 'car_age_category' based on 'car_age'": (
        "Create a new column 'car_age_category' based on the value of 'car_age'",
        """df["car_age_category"] = df["car_age"].apply(
        lambda age: "New" if age <= 3 else ("Middle" if age <= 9 else "Old")
    )""",
    ),
}
