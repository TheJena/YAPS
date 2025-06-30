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

from groq import APIStatusError
from httpx import ConnectError
from langchain.chains import LLMChain
from langchain.prompts import ChatPromptTemplate, PromptTemplate
from langchain_community.llms.ollama import OllamaEndpointNotFoundError
from langchain_core.output_parsers import StrOutputParser
from langchain_groq import ChatGroq
from langchain_ollama.chat_models import ChatOllama
from logging import debug, info, warning
from os.path import abspath, getsize, isfile
from re import DOTALL, search
from SECRET import black_magic  # from functools import lru_cache
from SECRET import MY_API_KEY
from textwrap import dedent
from traceback import format_exception
from utils import black, parsed_args


class ChatLLM(object):
    def invoke(self, runnable_input):
        raise NotImplementedError("Please override this inherited method")


class Groq(ChatLLM):
    def __init__(
        self,
        prompt,
        groq_api_key: str = MY_API_KEY,
        model_name: str = parsed_args().llm_name,
        temperature: float = 0,
    ):
        self.prompt = prompt
        self.chat = ChatGroq(
            groq_api_key=groq_api_key,
            model_name=model_name,
            temperature=temperature,
        )
        self.parser = StrOutputParser()
        self.chain = self.prompt | self.chat | self.parser

    def invoke(self, runnable_input):
        try:
            return self.chain.invoke(runnable_input)
        except APIStatusError as e:
            warning(format_exception(e))
            raise RuntimeWarning("Groq failed, have a look at the logs")


class Ollama(ChatLLM):
    def __init__(
        self,
        prompt,
        context_windows_size=parsed_args().num_ctx,
        keep_alive=parsed_args().keep_alive,
        model=parsed_args().llm_name,
    ):
        self.prompt = prompt
        self.chat = ChatOllama(
            # be aware that adding parameters here will probably
            # invalidate
            keep_alive=keep_alive,
            model=model,
            num_ctx=context_windows_size,
        )
        self.parser = StrOutputParser()

        try:
            response = (
                ChatPromptTemplate.from_messages([("user", "ping")])
                | self.chat
                | self.parser
            ).invoke({"user": "ping"})
        except (ConnectError, OllamaEndpointNotFoundError) as e:
            if "111" in str(e) or "404" in str(e):
                raise SystemExit(
                    "The model was not found; please check for typos "
                    "by running:\n\tdocker exec --interactive --tty ollama "
                    f"ollama pull {parsed_args().llm_name}"
                )
            raise e
        else:
            assert "pong" in response.lower(), f"{response=}"

        self.chain = self.prompt | self.chat | self.parser

    def invoke(self, runnable_input):
        return self.chain.invoke(runnable_input)


class LLM_formatter:
    def __init__(self, input_file_object):
        # Template per descrivere il grafo e suggerire miglioramenti
        # alla pipeline di pulizia dei dati
        PIPELINE_FORMATTER_TEMPLATE = (
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
                
                Cleaning Pipeline: {pipeline_content}
                
                Question: {question}
                """
            )
        )

        self.prompt = PromptTemplate(
            input_variables=["pipeline_content", "question"],
            template=PIPELINE_FORMATTER_TEMPLATE,
        )

        self.chat_llm = (
            Ollama(self.prompt)
            if parsed_args().use_ollama
            else (Groq(self.prompt) if parsed_args().use_groq else ChatLLM())
        )

        self.pipeline_content = "\n".join(
            line.split("#")[0].rstrip()
            + str("  # fmt: skip" if "#" in line else "")  # strip comments
            for line in black(input_file_object.read()).split("\n")
        )
        self.__raw_pipeline_path = abspath(input_file_object.name)

    def standardize(self, output_path) -> str:
        response = _standardize_llm_invokation(
            {
                "pipeline_content": self.pipeline_content,
                "question": "Description and Suggestions:",
            },
            input_path=open(self.__raw_pipeline_path),
        )
        # Use regular expression to find text between triple quotes
        extracted_text = search("```(.*?)```", response, DOTALL)

        if extracted_text:
            # Get the matched group from the search
            code_to_write = extracted_text.group(1).removeprefix("python\n")
            debug(code_to_write)

            with open(output_path, "w") as f:
                f.write(black(code_to_write))

            output_path = abspath(output_path)
            debug(f"Code has been successfully written to {output_path}")
            return str(output_path)
        else:
            warning("No triple-quoted text found.")


@black_magic
def _standardize_llm_invokation(context_dict, input_path):
    debug(
        f"{_standardize_llm_invokation.__name__}(context_dict=\n{'#'*80}\n"
        + PIPELINE_FORMATTER_TEMPLATE.format(**context_dict)
        + f"\n{'#'*80}\n)"
    )
    return LLM_formatter(input_path).chat_llm.invoke(context_dict)
