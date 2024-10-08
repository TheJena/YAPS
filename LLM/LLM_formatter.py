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

from constants import LLM_NAME, USE_GROQ, USE_OLLAMA
from langchain.chains import LLMChain
from langchain.prompts import ChatPromptTemplate, PromptTemplate
from langchain_community.chat_models import ChatOllama
from langchain_community.llms.ollama import OllamaEndpointNotFoundError
from langchain_core.output_parsers import StrOutputParser
from langchain_groq import ChatGroq
from os.path import abspath, isfile
from re import DOTALL, search
from SECRET import MY_API_KEY
from textwrap import dedent
from utils import black


class ChatLLM(object):
    def invoke(self, runnable_input):
        raise NotImplementedError("Please override this inherited method")


class Groq(ChatLLM):
    def __init__(
        self,
        prompt,
        groq_api_key: str = MY_API_KEY,
        model_name: str = LLM_NAME,
        temperature: float = 0,
    ):
        self.prompt = prompt
        self.chat = ChatGroq(
            groq_api_key=groq_api_key,
            model_name=model_name,
            temperature=temperature,
        )
        self.chat_chain = LLMChain(
            llm=self.chat,
            prompt=self.prompt,
            verbose=False,
        )

    def invoke(self, runnable_input):
        response = self.chat_chain.invoke(runnable_input)
        return response["text"]


class Ollama(ChatLLM):
    def __init__(self, prompt, model=LLM_NAME):
        self.prompt = prompt
        self.chat = ChatOllama(
            # be aware that adding parameters here will probably
            # invalidate
            model=model
        )
        self.parser = StrOutputParser()

        try:
            response = (
                ChatPromptTemplate.from_messages([("user", "ping")])
                | self.chat
                | self.parser
            ).invoke({"user": "ping"})
        except OllamaEndpointNotFoundError as e:
            if "404" in str(e):
                raise SystemExit(
                    "The model was not found; please check for typos "
                    "by running:\n\tdocker exec --interactive --tty ollama "
                    f"ollama pull {LLM_NAME}"
                )
            raise e
        else:
            assert response.lower() == "pong", f"{response=}"

        self.chain = self.prompt | self.chat | self.parser

    def invoke(self, runnable_input):
        return self.chain.invoke(runnable_input)


class LLM_formatter:
    def __init__(self, file_pipeline):
        # Template per descrivere il grafo e suggerire miglioramenti
        # alla pipeline di pulizia dei dati
        PIPELINE_STANDARDIZER_TEMPLATE = (
            # Do not drop the shebang sequence, the encoding declaration and
            # the copyright and license notices if present.
            dedent(
                """\
                Add python comments describing the single existing operations
                in the pipeline, be the most detailed as possible.  Return your
                response as a complete python file, including both changed and
                not changed functions, import and the first commented lines.
                
                Do not write new lines of code, just add python comments and
                empty lines related to the code that you read.
                
                Instructions:"""
            )
            + "\n".join(
                f"{i}. {' '.join(step.split())}."
                for i, step in enumerate(
                    """
                    Each cleaning operation on the data frame should be
                    contained in the same block of code without empty lines
                    #
                    Do not write new lines of code, just add comments and empty
                    lines
                    #
                    Each operation on the data frame should be separated by a
                    single empty line
                    #
                    Consider just the code contained in the run_pipeline
                    function
                    #
                    Exclusively after the blocks after the subscribe dataframe
                    block for each identified block add at the end, after
                    leaving an empty line a line containing
                    "tracker.analyze_changes(df)"
                    #
                    Do not comment "tracker.analyze_changes(df)" lines
                    """.split(
                        "#"
                    )
                )
            )
            + dedent(
                """\
                example:
                pipeline:
                X_train, X_test, y_train, y_test = train_test_split(
                    df[["latitude", "longitude"]],
                    df[["median_house_value"]],
                    random_state=0,
                    test_size=0.33,
                )
                # normalize the training and test data using the
                # preprocessing.normalize() method from sklearn
                X_train_norm = preprocessing.normalize(X_train)
                X_test_norm = preprocessing.normalize(X_test)
                
                kmeans = KMeans(n_clusters=3, random_state=0)
                kmeans.fit(X_train_norm)
                
                sns.scatterplot(
                    data=X_train,
                    hue=kmeans.labels_,
                    x="longitude",
                    y="latitude",
                )
                plt.show()
                
                # Add cluster labels to the original DataFrame
                df["cluster"] = kmeans.predict(
                    preprocessing.normalize(df[["latitude", "longitude"]])
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
                    df_dummies = dummies.add_prefix(col + "_")
                    df = df.join(df_dummies)
                    df = df.drop([col], axis=1)
                
                response:
                # Split data into training and testing sets
                X_train, X_test, y_train, y_test = train_test_split(
                    df[["latitude", "longitude"]],
                    df[["median_house_value"]],
                    random_state=0,
                    test_size=0.33,
                )
                tracker.analyze_changes(df)
                
                # Normalize the training and test data
                X_train_norm = preprocessing.normalize(X_train)
                X_test_norm = preprocessing.normalize(X_test)
                tracker.analyze_changes(df)
                
                # Fit KMeans clustering model to the normalized training data
                kmeans = KMeans(n_clusters=3, random_state=0)
                kmeans.fit(X_train_norm)
                tracker.analyze_changes(df)
                
                # Scatter plot of longitude vs latitude with hue as cluster labels
                sns.scatterplot(
                    data=X_train,
                    hue=kmeans.labels_,
                    x="longitude",
                    y="latitude",
                )
                plt.show()
                tracker.analyze_changes(df)
                
                # Add cluster labels to the original DataFrame
                df["cluster"] = kmeans.predict(
                    preprocessing.normalize(df[["latitude", "longitude"]])
                )
                tracker.analyze_changes(df)
                
                # One-hot encode categorical variables
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
                    df_dummies = dummies.add_prefix(col + "_")
                    df = df.join(df_dummies)
                    df = df.drop([col], axis=1)
                tracker.analyze_changes(df)
                
                Cleaning Pipeline: {pipeline_content}
                
                Question: {question}
                """
            )
        )

        self.prompt = PromptTemplate(
            input_variables=["pipeline_content", "question"],
            template=PIPELINE_STANDARDIZER_TEMPLATE,
        )

        self.chat_llm = (
            Ollama(self.prompt)
            if USE_OLLAMA
            else (Groq(self.prompt) if USE_GROQ else ChatLLM())
        )

        # cleaning pipeline in text format
        self.pipeline_content = self.file_to_text(file_pipeline)

    def file_to_text(self, path):
        path = abspath(path)
        assert isfile(path), f"{path=}"
        return black(open(path).read())

    def standardize(self) -> str:
        response = self.chat_llm.invoke(
            {
                "pipeline_content": self.pipeline_content,
                "question": "Description and Suggestions:",
            }
        )
        # Use regular expression to find text between triple quotes
        extracted_text = search("```(.*?)```", response, DOTALL)

        if extracted_text:
            # Get the matched group from the search
            code_to_write = extracted_text.group(1).removeprefix("python\n")
            # Specify the filename
            filename = "extracted_code.py"

            # Write the extracted text to a file
            with open(filename, "w") as file:
                file.write(black(code_to_write))
            print(f"Code has been successfully written to {filename}")
            print(abspath(filename))
            return str(abspath(filename))
        else:
            print("No triple-quoted text found.")
