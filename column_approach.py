from graph.structure import *
from graph.constants import *


def column_vision(changes, current_activities, args):
    derivations_column = []
    current_relations_column = []
    current_columns = {}

    for act in changes.keys():
        generated_columns = []
        used_columns = []
        invalidated_columns = []
        if act == 0:
            continue
        activity = current_activities[act - 1]
        df1 = changes[act]["before"]
        df2 = changes[act]["after"]
        # Approach working when the number of rows is the same and the number of columns increase or is the same
        if len(df1.columns) <= len(df2.columns):
            unique_rows_in_df1 = set(df1.index) - set(df2.index)
            # Iterate over the columns and rows to find differences
            for col in df2.columns:
                new_column = None
                for idx in df2.index:
                    old_col_name = col
                    if idx in df1.index and col in df1.columns:
                        old_value = df1.at[idx, col]
                    elif (
                        idx in df1.index
                        and df2.columns.get_loc(col) < len(df1.columns)
                        and (
                            list(df1.iloc[:, df2.columns.get_loc(col)])
                            == list(df2[col])
                        )
                    ):
                        old_value = df1.iloc[
                            list(df1.index).index(idx),
                            df2.columns.get_loc(col),
                        ]
                        old_col_name = df1.columns[df2.columns.get_loc(col)]
                    else:
                        old_value = "Not exist"
                    new_value = df2.at[idx, col]
                    # gen = False
                    if (
                        old_value != new_value
                        or col != df1.columns[df2.columns.get_loc(col)]
                    ):
                        # control the column already exist or create it
                        val_col = str(df2[col].tolist())
                        idx_col = str(df2.index.tolist())
                        if (
                            val_col,
                            idx_col,
                            col,
                        ) not in current_columns.keys():
                            new_column = create_column(val_col, idx_col, col)
                            generated_columns.append(new_column["id"])
                            current_columns[(val_col, idx_col, col)] = (
                                new_column
                            )
                        if old_value != "Not exist":
                            # same control but for the before df, to get the used columns
                            old_column = None
                            val_old_col = str(df1[old_col_name].tolist())
                            idx_old_col = str(df1.index.tolist())
                            if (
                                val_old_col,
                                idx_old_col,
                                old_col_name,
                            ) not in current_columns.keys():
                                old_column = create_column(
                                    val_old_col, idx_old_col, old_col_name
                                )
                                current_columns[
                                    (val_old_col, idx_old_col, old_col_name)
                                ] = old_column
                            else:
                                old_column = current_columns[
                                    (val_old_col, idx_old_col, old_col_name)
                                ]
                            if (
                                new_column
                                and new_column["id"] != old_column["id"]
                            ):
                                derivations_column.append(
                                    {
                                        "gen": str(new_column["id"]),
                                        "used": str(old_column["id"]),
                                    }
                                )
                            used_columns.append(old_column["id"])
                            invalidated_columns.append(old_column["id"])
                            break
                for idx in unique_rows_in_df1:
                    if idx in df1.index and col in df1.columns:
                        # the old column that with the unique row
                        val_col = str(df1[col].tolist())
                        idx_col = str(df1.index.tolist())
                        if (
                            val_col,
                            idx_col,
                            col,
                        ) not in current_columns.keys():
                            old_column = create_column(val_col, idx_col, col)
                            current_columns[(val_col, idx_col, col)] = (
                                old_column
                            )
                        else:
                            old_column = current_columns[
                                (val_col, idx_col, col)
                            ]
                        current_columns[(val_col, idx_col, col)] = old_column
                        used_columns.append(old_column["id"])
                        invalidated_columns.append(old_column["id"])
                        # the new column without the unique row
                        val_new_col = str(df2[col].tolist())
                        idx_new_col = str(df2.index.tolist())
                        if (
                            val_new_col,
                            idx_new_col,
                            col,
                        ) not in current_columns.keys():
                            new_column = create_column(
                                val_new_col, idx_new_col, col
                            )
                            current_columns[
                                (val_new_col, idx_new_col, col)
                            ] = new_column
                        else:
                            new_column = current_columns[
                                (val_new_col, idx_new_col, col)
                            ]
                        current_columns[(val_new_col, idx_new_col, col)] = (
                            new_column
                        )
                        generated_columns.append(new_column["id"])
                        if new_column and new_column["id"] != old_column["id"]:
                            derivations_column.append(
                                {
                                    "gen": str(new_column["id"]),
                                    "used": str(old_column["id"]),
                                }
                            )
                        break

        # if the number of columns decrease but the number of rows is still the same
        elif len(df1.columns) > len(df2.columns):
            # Iterate over the columns and rows to find differences
            unique_col_in_df1 = set(df1.columns) - set(df2.columns)
            unique_col_in_df2 = set(df2.columns) - set(df1.columns)
            for col in unique_col_in_df1:
                # control il the column already exist or create it
                val_col = str(df1[col].tolist())
                idx_col = str(df1.index.tolist())
                new_column = None
                if (val_col, idx_col, col) not in current_columns.keys():
                    new_column = create_column(val_col, idx_col, col)
                    current_columns[(val_col, idx_col, col)] = new_column
                else:
                    new_column = current_columns[(val_col, idx_col, col)]
                used_columns.append(new_column["id"])
                invalidated_columns.append(new_column["id"])
            # if the column is exclusively in the "after" dataframe
            for col in unique_col_in_df2:
                # control il the column already exist or create it
                val_col = str(df2[col].tolist())
                idx_col = str(df2.index.tolist())
                if (val_col, idx_col, col) not in current_columns.keys():
                    new_column = create_column(val_col, idx_col, col)
                    generated_columns.append(new_column["id"])
                    current_columns[(val_col, idx_col, col)] = new_column

            common_col = set(df1.columns).intersection(set(df2.columns))
            for col in common_col:
                new_column = None
                for idx in df2.index:
                    if idx in df1.index:
                        old_value = df1.at[idx, col]
                    else:
                        old_value = "Not exist"
                    new_value = df2.at[idx, col]
                    if old_value != new_value:
                        # control if the column already exist or create it
                        val_col = str(df2[col].tolist())
                        idx_col = str(df2.index.tolist())
                        if (
                            val_col,
                            idx_col,
                            col,
                        ) not in current_columns.keys():
                            new_column = create_column(val_col, idx_col, col)
                            generated_columns.append(new_column["id"])
                            current_columns[(val_col, idx_col, col)] = (
                                new_column
                            )
                        if old_value != "Not exist":
                            # same control but for the before df, to get the used columns
                            old_column = None
                            val_old_col = str(df1[col].tolist())
                            idx_old_col = str(df1.index.tolist())
                            if (
                                val_old_col,
                                idx_old_col,
                                col,
                            ) not in current_columns.keys():
                                old_column = create_column(
                                    val_old_col, idx_old_col, col
                                )
                                current_columns[
                                    (val_old_col, idx_old_col, col)
                                ] = old_column
                            else:
                                old_column = current_columns[
                                    (val_old_col, idx_old_col, col)
                                ]
                            if (
                                new_column
                                and new_column["id"] != old_column["id"]
                            ):
                                derivations_column.append(
                                    {
                                        "gen": str(new_column["id"]),
                                        "used": str(old_column["id"]),
                                    }
                                )
                            used_columns.append(old_column["id"])
                            invalidated_columns.append(old_column["id"])
                            break

        current_relations_column.append(
            create_relation_column(
                activity["id"],
                generated_columns,
                used_columns,
                invalidated_columns,
                same=False,
            )
        )
    return current_relations_column, current_columns, derivations_column
