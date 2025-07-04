#!/usr/bin/env python3
# coding: utf-8
#
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Copyright (C) 2024-2025 Federico Motta            <federico.motta@unimore.it>
#                         Pasquale Leonardo Lazzaro <pas.lazzaro@stud.uniroma3.it>
#                         Marialaura Lazzaro        <mar.lazzaro1@stud.uniroma3.it>
# Copyright (C) 2022-2024 Luca Gregori              <luca.gregori@uniroma3.it>
# Copyright (C) 2021-2022 Luca Lauro                <luca.lauro@uniroma3.it>
#
# This file is part of YAPS, a provenance capturing suite
#
# YAPS is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# YAPS is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
# License for more details.
#
# You should have received a copy of the GNU General Public License
# along with YAPS.  If not, see <https://www.gnu.org/licenses/>.

from sklearn import preprocessing
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def stratified_sample(df, frac):
    """
    Perform stratified sampling on a DataFrame.
    """
    if frac > 0.0 and frac < 1.0:
        # Infer categorical columns for stratification
        stratify_columns = df.select_dtypes(
            include=["object"],
        ).columns.tolist()

        # Check if any class in stratify columns has fewer than 2 members
        for col in stratify_columns:
            value_counts = df[col].value_counts()
            if value_counts.min() >= 2:
                # Perform stratified sampling for this column
                stratified_df = df.groupby(col, group_keys=False).apply(
                    lambda x: x.sample(frac=frac)
                )
                return stratified_df.reset_index(drop=True)

        # If no suitable stratification column is found, fall back to
        # random sampling
        sampled_df = df.sample(frac=frac).reset_index(drop=True)
    else:
        sampled_df = df
    return sampled_df


def run_pipeline(args, tracker) -> None:
    input_path = args.dataset  # "datasets/housing.csv"

    df = pd.read_csv(input_path)

    if args.frac > 0.0 and args.frac < 1.0:
        df = stratified_sample(df, args.frac)
    elif args.frac > 1.0:
        df = pd.concat([df] * int(args.frac), ignore_index=True)

    # Subscribe dataframe
    df = tracker.subscribe(df)

    sns.scatterplot(
        data=df,
        x="longitude",
        y="latitude",
        hue="median_house_value",
    )
    plt.show()
    X_train, X_test, y_train, y_test = train_test_split(
        df[["latitude", "longitude"]],
        df[["median_house_value"]],
        test_size=0.33,
        random_state=0,
    )
    # normalize the training and test data using the
    # preprocessing.normalize() method from sklearn
    X_train_norm = preprocessing.normalize(X_train)
    # X_test_norm = preprocessing.normalize(X_test)

    kmeans = KMeans(n_clusters=3, random_state=0)
    kmeans.fit(X_train_norm)

    sns.scatterplot(
        data=X_train,
        x="longitude",
        y="latitude",
        hue=kmeans.labels_,
    )
    plt.show()

    # Add cluster labels to the original DataFrame
    df["cluster"] = kmeans.predict(
        preprocessing.normalize(df[["latitude", "longitude"]])
    )

    # Sample 50 points from each cluster
    sampled_df_list = []
    for cluster_id in df["cluster"].unique():
        cluster_data = df[df["cluster"] == cluster_id]
        sampled_data = (
            cluster_data.sample(n=50, random_state=0)
            if len(cluster_data) > 50
            else cluster_data
        )
        sampled_df_list.append(sampled_data)

    # Concatenate all sampled data and update the original DataFrame
    # df = pd.concat(sampled_df_list, ignore_index=True)
    # Concatenate the sampled data
    sampled_df = pd.concat(sampled_df_list, ignore_index=True)

    # Trovare gli indici delle righe in df che non sono presenti in sampled_df
    indices_to_drop = []
    for index, row in df.iterrows():
        if not ((sampled_df == row).all(axis=1)).any():
            indices_to_drop.append(index)

    # Rimuovere le righe in df che non sono presenti in sampled_df usando drop
    df = df.drop(indices_to_drop)

    sns.scatterplot(data=df, x="longitude", y="latitude", hue="cluster")

    plt.show()

    return df
