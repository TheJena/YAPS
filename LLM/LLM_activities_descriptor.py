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

from langchain.prompts import PromptTemplate
from logging import debug, info, warning
from LLM.LLM_formatter import ChatLLM, Groq, Ollama
from re import DOTALL, search
from textwrap import dedent
from utils import black, parsed_args, yaml_dump


class LLM_activities_descriptor:
    def __init__(self, io_obj):
        # Template per descrivere il grafo e suggerire miglioramenti
        # alla pipeline di pulizia dei dati
        PIPELINE_DESCRIPTOR_TEMPLATE = (
            dedent(
                """\
                You are an expert in data preprocessing
                pipelines. Return a list of name + (description of the
                single operations in the pipeline, python code of the
                operation), consider just the content of the
                run_pipeline function. Do not include sampling
                subscription and column name assignment and
                identification of the objects of the columns
                activities. Consider just the code after the
                subscription of the dataframe (df.subscribe()). Be the
                most detailed as possible.
                
                Return the result as a python dictionary with the
                operation name as key and the description as value.
                
                Do not refer to comments. Do not include sampling
                operations. Consider just the operations appearing
                after the tracker.subscribe line and with a
                tracker.analyze_changes(df) in their block. All the
                operations followed by tracker.analyze_changes have to
                appear in the map.  Do not consider the sampling as an
                operation Give me the result as a map between ``` ```
                """
            )
            + dedent(
                """\
                Example:
                pipeline:
                from sklearn.compose import ColumnTransformer
                from sklearn.ensemble import RandomForestRegressor
                from sklearn.feature_selection import RFE
                from sklearn.impute import SimpleImputer
                from sklearn.metrics import mean_squared_error
                from sklearn.model_selection import train_test_split
                from sklearn.pipeline import Pipeline
                from sklearn.preprocessing import StandardScaler, OneHotEncoder
                import pandas as pd
                
                # Load dataset
                data = pd.read_csv("data.csv")
                
                # Identify features and target
                X = data.drop("target", axis=1)
                y = data["target"]
                
                # Subscribe dataframe
                df = tracker.subscribe(data)
                tracker.analyze_changes(df)
                
                # Identify numerical and categorical columns
                num_features = X.select_dtypes(
                    include=["int64", "float64"]
                ).columns
                cat_features = X.select_dtypes(include=["object"]).columns
                
                # Define preprocessing for numerical data
                num_transformer = Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="mean")),
                        ("scaler", StandardScaler()),
                    ]
                )
                
                # Define preprocessing for categorical data
                cat_transformer = Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                )
                
                # Combine preprocessing steps
                preprocessor = ColumnTransformer(
                    transformers=[
                        ("num", num_transformer, num_features),
                        ("cat", cat_transformer, cat_features),
                    ]
                )
                
                # Define the model
                model = RandomForestRegressor()
                
                # Create and evaluate the full pipeline
                pipeline = Pipeline(
                    steps=[
                        ("preprocessor", preprocessor),
                        (
                            "feature_selection",
                            RFE(model, n_features_to_select=10),
                        ),
                        ("model", model),
                    ]
                )
                
                # Split data into train and test sets
                X_train, X_test, y_train, y_test = train_test_split(
                    X,
                    y,
                    test_size=0.2,
                    random_state=42,
                )
                
                # Fit the model
                pipeline.fit(X_train, y_train)
                
                # Make predictions
                y_pred = pipeline.predict(X_test)
                
                # Evaluate the model
                mse = mean_squared_error(y_test, y_pred)
                response:
                [
                    "Identification of numerical and categorical columns": (
                        "Determine which columns are numerical "
                        "and which are categorical using select_dtypes",
                        "    num_features = X.select_dtypes(",
                        "        include=['int64', 'float64',]",
                        "    ).columns",
                        "    cat_features = X.select_dtypes(",
                        "        include=['object']",
                        "    ).columns",
                    )
                ]
                
            Cleaning Pipeline: {pipeline_content}
            """
            )
        )

        self.prompt = PromptTemplate(
            input_variables=["pipeline_content", "question"],
            template=PIPELINE_DESCRIPTOR_TEMPLATE,
        )
        )

        self.chat_llm = (
            Ollama(self.prompt)
            if parsed_args().use_ollama
            else (Groq(self.prompt) if parsed_args().use_groq else ChatLLM())
        )

        # cleaning pipeline in text format
        self.pipeline_content = black(io_obj.read())

    def descript(self, io_obj=None) -> str:
        response = _descript_llm_invokation(
            {
                "pipeline_content": self.pipeline_content,
            },
            io_obj,
        )
        # Use regular expression to find text between triple quotes
        extracted_text = search("```(.*?)```", response, DOTALL)

        if extracted_text:
            descr_to_write = (
                extracted_text.group(1)
                .removeprefix("python\n")
                .removeprefix("pipeline_")
            )
            info(descr_to_write)
            descr_to_write = black(descr_to_write).removeprefix(
                "operations = "
            )

            if io_obj is None:
                io_obj = open("described_activities.yaml", "w")

            if io_obj.seekable:
                io_obj.seek(0)  # truncate file
            yaml_dump(code=descr_to_write, stream=io_obj)
            io_obj.close()

            debug(
                f"Description has been successfully written to {io_obj.name}"
            )
            return descr_to_write
        else:
            warning("No triple-quoted text found.")


@black_magic
def _descript_llm_invokation(context_dict, io_obj):
    return LLM_activities_descriptor(io_obj).chat_llm.invoke(context_dict)
