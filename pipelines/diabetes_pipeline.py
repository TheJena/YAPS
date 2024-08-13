#!/usr/bin/env python3
# coding: utf-8
#
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Copyright (C) 2024      Federico Motta            <federico.motta@unimore.it>
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

import pandas as pd
import sys

from sklearn.tree import plot_tree
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.tree import (
    DecisionTreeClassifier,
)  # Import Decision Tree Classifier
from sklearn.model_selection import (
    train_test_split,
)  # Import train_test_split function
from sklearn import (
    metrics,
)  # Import scikit-learn metrics module for accuracy calculation


def run_pipeline(args, tracker) -> None:

    input_path = args.dataset

    col_names = [
        "pregnant",
        "glucose",
        "bp",
        "skin",
        "insulin",
        "bmi",
        "pedigree",
        "age",
        "label",
    ]
    # load dataset
    p = pd.read_csv(input_path, header=0, names=col_names)

    # split dataset in features and target variable
    feature_cols = [
        "pregnant",
        "insulin",
        "bmi",
        "age",
        "glucose",
        "bp",
        "pedigree",
    ]
    X = p[feature_cols]  # Features
    X_train = tracker.subscribe(X)

    Y = p.label  # Target variable

    # Subscribe dataframe

    # Split dataset into training set and test set
    X_train, X_test, y_train, y_test = train_test_split(
        X, Y, test_size=0.3, random_state=1
    )  # 70% training and 30% test
    clf = DecisionTreeClassifier()
    clf = clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)

    # Model Accuracy
    print("Accuracy:", metrics.accuracy_score(y_test, y_pred))
    # Visualize the decision tree
    plt.figure(figsize=(20, 10))
    plot_tree(
        clf, feature_names=feature_cols, class_names=["0", "1"], filled=True
    )
    plt.show()

    # Create Decision Tree classifer object, Data Pruning
    clf = DecisionTreeClassifier(criterion="entropy", max_depth=3)
    clf = clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)

    # Model Accuracy
    print("Accuracy:", metrics.accuracy_score(y_test, y_pred))
    # Visualize the decision tree
    plt.figure(figsize=(20, 10))
    plot_tree(
        clf, feature_names=feature_cols, class_names=["0", "1"], filled=True
    )
    plt.show()
