operations = {
    "Selection of relevant columns": (
        "Select specific columns from the dataframe",
        "columns = [\n"
        + '    "age",\n'
        + '    "c_charge_degree",\n'
        + '    "c_jail_in",\n'
        + '    "c_jail_out",\n'
        + '    "days_b_screening_arrest",\n'
        + '    "priors_count",\n'
        + '    "race",\n'
        + '    "sex",\n'
        + '    "two_year_recid"\n'
        + "]\ndf = df.drop(df.columns.difference(columns), axis=1)",
    ),
    "Removal of rows with missing values": (
        "Remove rows from the dataframe that contain missing values",
        "df = df.dropna()",
    ),
    "Mapping of race to binary variable": (
        "Map the 'race' column to a binary variable (0 for non-Caucasian, 1 for Caucasian)",
        'df["race"] = [0 if r != "Caucasian" else 1 for r in df["race"]]',
    ),
    "Renaming of two_year_recid column": (
        "Rename the 'two_year_recid' column to 'label'",
        'df = df.rename({"two_year_recid": "label"}, axis=1)',
    ),
    "Reversal of label for consistency": (
        "Reverse the 'label' column for consistency with function definitions (1 means no recid, 0 means recid)",
        'df["label"] = [0 if lab == 1 else 1 for lab in df["label"]]',
    ),
    "Calculation of jailtime in days": (
        "Calculate the 'jailtime' column as the difference between 'c_jail_out' and 'c_jail_in' in days",
        'df["jailtime"] = (pd.to_datetime(df.c_jail_out) - pd.to_datetime(df.c_jail_in)).dt.days',
    ),
    "Dropping of c_jail_in and c_jail_out columns": (
        "Drop the 'c_jail_in' and 'c_jail_out' columns from the dataframe",
        'df = df.drop(["c_jail_in", "c_jail_out"], axis=1)',
    ),
    "Mapping of c_charge_degree to binary variable": (
        "Map the 'c_charge_degree' column to a binary variable (0 for misconduct, 1 for felony)",
        'df["c_charge_degree"] = [0 if s == "M" else 1 for s in df["c_charge_degree"]]',
    ),
}
