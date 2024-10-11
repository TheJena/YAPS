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

from constants import USE_GROQ, USE_OLLAMA
from langchain.prompts import PromptTemplate
from LLM.LLM_formatter import ChatLLM, Groq, Ollama
from re import DOTALL, search


class LLM_activities_used_columns:
    def __init__(self):
        # Template per descrivere il grafo e suggerire miglioramenti
        # alla pipeline di pulizia dei dati
        PIPELINE_STANDARDIZER_TEMPLATE = """
            You are receiving the dataframe before and after the
            operation, the code and the description of the operation.

            Return me a python list with the name of the columns in
            the dataframe before used by the operation. Limit your
            observations just to this inputs and the code of the
            operation. Do not make assumptions.  Return the python
            list between `[]`.

            For example if a column is dropped only the dropped column
            is used.
            Example of answer: ```["column1", "column2", "column3"]```
            Write the list inside ``` ```

            dataframe before: {df_before}
            dataframe after: {df_after}

            code: {code}
            description: {description}
        """

        self.prompt = PromptTemplate(
            input_variables=["code", "description", "df_after", "df_before"],
            template=PIPELINE_STANDARDIZER_TEMPLATE,
        )

        self.chat_llm = (
            Ollama(self.prompt)
            if USE_OLLAMA
            else (Groq(self.prompt) if USE_GROQ else ChatLLM())
        )

    def give_columns(self, df_before, df_after, code, description) -> str:
        response = self.chat_llm.invoke(
            {
                "code": code,
                "description": description,
                "df_after": df_after,
                "df_before": df_before,
            }
        )
        # Use regular expression to find text between triple quotes
        extracted_text = search("```(.*?)```", response, DOTALL)

        if extracted_text:
            return extracted_text.group(1).removeprefix("python\n")
        else:
            print("No triple-quoted text found.")
