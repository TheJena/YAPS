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

from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain_groq import ChatGroq
import os
import re


class LLM_formatter:
    def __init__(
        self,
        file_pipeline,
        api_key: str,
        temperature: float = 0,
        model_name: str = "llama3-70b-8192",
    ):
        self.chat = ChatGroq(
            temperature=temperature,
            groq_api_key=api_key,
            model_name=model_name,
        )

        # cleaning pipeline in text format
        self.pipeline_content = self.file_to_text(file_pipeline)

        # Template per descrivere il grafo e suggerire miglioramenti
        # alla pipeline di pulizia dei dati
        PIPELINE_STANDARDIZER_TEMPLATE = """

        Add python comments describing the single existing operations
        in the pipeline, be the most detailed as possible.  Return
        your response as a complete python file, including both
        changed and not changed functions, import and ..

        Do not write new lines of code, just add python comments and
        empty lines related to the code that you read.

        Instructions:
        1. Each cleaning operation on the data frame should be
           conatined in the same block of code without empty lines.
        2. Do not write new lines of code, just add comments and empty
           lines.
        3. Each operation on the data frame should be separated by a
           single empty line.
        4. Consider just the code contained in the run_pipeline
           function.
        5. Exclusively after the blocks after the subscribe dataframe
           block for each identified block add at the end, after leaving
           an empty line a line containing "tracker.analyze_changes(df)"
        6. Do not comment "tracker.analyze_changes(df)" lines

        example:
        pipeline:
        X_train, X_test, y_train, y_test = train_test_split(
            df[["latitude", "longitude"]],
            df[["median_house_value"]],
            test_size=0.33,
            random_state=0,
        )
        # normalize the training and test data using the
        # preprocessing.normalize() method from sklearn
        X_train_norm = preprocessing.normalize(X_train)
        X_test_norm = preprocessing.normalize(X_test)

        kmeans = KMeans(n_clusters = 3, random_state = 0)
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
            preprocessing.normalize(
                df[["latitude", "longitude"]]
            )
        )

        columns = [
            "checking",
            "credit_history",
            "purpose",
            "savings",
            "employment",
            "other_debtors",
            "property",
            "other_inst",
            "housing",
            "job",
        ]
        for i, col in enumerate(columns):
            dummies = pd.get_dummies(df[col])
            df_dummies = dummies.add_prefix(col + '_')
            df = df.join(df_dummies)
            df = df.drop([col], axis=1)


        response:
         # Split data into training and testing sets
            X_train, X_test, y_train, y_test = train_test_split(
                df[['latitude', 'longitude']],
                df[['median_house_value']],
                test_size=0.33,
                random_state=0
            )
            tracker.analyze_changes(df)

            # Normalize the training and test data
            X_train_norm = preprocessing.normalize(X_train)
            X_test_norm = preprocessing.normalize(X_test)
            tracker.analyze_changes(df)

            # Fit KMeans clustering model to the normalized training data
            kmeans = KMeans(n_clusters = 3, random_state = 0)
            kmeans.fit(X_train_norm)
            tracker.analyze_changes(df)

            # Scatter plot of longitude vs latitude with hue as cluster labels
            sns.scatterplot(
                data=X_train,
                x="longitude",
                y="latitude",
                hue=kmeans.labels_,
            )
            plt.show()
            tracker.analyze_changes(df)

            # Add cluster labels to the original DataFrame
            df['cluster'] = kmeans.predict(
                preprocessing.normalize(df[['latitude', 'longitude']])
            )
            tracker.analyze_changes(df)

            #One-hot encode categorical variables
            columns = [
                "checking",
                "credit_history",
                "purpose",
                "savings",
                "employment",
                "other_debtors",
                "property",
                "other_inst",
                "housing",
                "job",
            ]
            for i, col in enumerate(columns):
                dummies = pd.get_dummies(df[col])
                df_dummies = dummies.add_prefix(col + '_')
                df = df.join(df_dummies)
                df = df.drop([col], axis=1)
             tracker.analyze_changes(df)


        Cleaning Pipeline:{pipeline_content}

        Question: {question}
        """

        self.prompt = PromptTemplate(
            template=PIPELINE_STANDARDIZER_TEMPLATE,
            input_variables=["pipeline_content", "question"],
        )

        self.chat_chain = LLMChain(
            llm=self.chat,
            prompt=self.prompt,
            verbose=False,
        )

    def file_to_text(self, file):
        # Leggi il contenuto del file e convertilo in testo
        content = None
        try:
            with open(file, "r") as file:
                content = file.read()

        except Exception as e:
            print(f"An error occurred: {e}")
        return content

    def standardize(self) -> str:
        response = self.chat_chain.invoke(
            {
                "pipeline_content": self.pipeline_content,
                "question": "Description and Suggestions:",
            }
        )
        # Use regular expression to find text between triple quotes
        extracted_text = re.search("```(.*?)```", response["text"], re.DOTALL)

        if extracted_text:
            # Get the matched group from the search
            code_to_write = extracted_text.group(1)
            # Specify the filename
            filename = "extracted_code.py"

            # Write the extracted text to a file
            with open(filename, "w") as file:
                file.write(code_to_write)
            print(f"Code has been successfully written to {filename}")
            print(os.path.abspath(filename))
            return str(os.path.abspath(filename))
        else:
            print("No triple-quoted text found.")
