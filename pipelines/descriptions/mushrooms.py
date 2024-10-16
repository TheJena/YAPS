operations = {
    "Drop unnecessary columns from the dataframe": (
        "Drops specified columns from the dataframe",
        'df = df.drop([\n            "does-bruise-or-bleed",\n            "gill-attachment",\n            "gill-color",\n            "gill-spacing",\n            "habitat",\n            "has-ring",\n            "ring-type",\n            "spore-print-color",\n            "stem-color",\n            "stem-root",\n            "stem-surface",\n            "veil-color",\n            "veil-type",\n        ],\n        axis=1,\n    )',
    ),
    "Convert the class column to binary values (0/1)": (
        "Replaces specified string values in the 'class' column with numerical values",
        'df["class"] = df["class"].replace({"e": 1, "p": 0})',
    ),
    "Drop rows with missing values": (
        "Drops rows from the dataframe where any value is missing or null",
        "df = df.dropna()",
    ),
    "Replace categorical values in specified columns with numerical values": (
        "Replaces string values in specified columns with numerical values based on a predefined mapping",
        'df = df.replace({\n        "cap-color": {\n            "n": 1,\n            "b": 2,\n            "g": 3,\n            "r": 3,\n            "p": 4,\n            "u": 5,\n            "e": 6,\n            "w": 7,\n            "y": 8,\n            "l": 9,\n            "o": 10,\n            "k": 11,\n        },\n        "cap-shape": {\n            "b": 1,\n            "c": 2,\n            "x": 3,\n            "f": 4,\n            "s": 5,\n            "p": 6,\n            "o": 7,\n        },\n        "cap-surface": {\n            "i": 1,\n            "g": 2,\n            "y": 3,\n            "s": 4,\n            "h": 5,\n            "l": 6,\n            "k": 7,\n            "t": 8,\n            "w": 9,\n            "e": 10,\n        },\n        "season": {\n            "s": 1,\n            "u": 2,\n            "a": 3,\n            "w": 4,\n        },\n    })',
    ),
}
