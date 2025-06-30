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

from langchain.prompts import PromptTemplate
from logging import debug, warning
from LLM.LLM_formatter import ChatLLM, Groq, Ollama
from SECRET import black_magic  # from functools import lru_cache
from re import DOTALL, search
from utils import parsed_args


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
        )

        self.ollama_chat = Ollama(self.prompt)
        self.groq_chat = Groq(self.prompt)

    def give_columns(self, df_before, df_after, code, description) -> str:
        debug(
            f"activity_code={code!r},\t{description=}\n"
            f"df_before=\n{df_before.to_string()}\n"
            f"df_after=\n{df_after.to_string()}"
        )
        debug(f"{df_before.shape=}\t{df_after.shape=}")
        response = _give_columns_llm_invokation(
            {
                "code": code,
                "description": description,
                "df_after": df_after.to_string(),
                "df_before": df_before.to_string(),
            }
        )
        # Use regular expression to find text between triple quotes
        extracted_text = search("```(.*?)```", response, DOTALL)

        if extracted_text:
            extracted_text = extracted_text.group(1).removeprefix("python\n")
            debug(extracted_text)
            return extracted_text
        else:
            warning("No triple-quoted text found.")


@black_magic
def _give_columns_llm_invokation(context_dict):
    debug(
        f"{_give_columns_llm_invokation.__name__}(context_dict=\n{'#'*80}\n"
        + PIPELINE_STANDARDIZER_TEMPLATE.format(**context_dict).strip()
        + f"\n{'#'*80}\n)"
    )
    failed = False
    if parsed_args().use_groq:
        try:
            ret = LLM_activities_used_columns().groq_chat.invoke(context_dict)
        except RuntimeWarning as e:
            debug("Groq invocation failed")
            debug(str(e))
            failed = True
        else:
            return ret
    if parsed_args().use_ollama or failed:
        return LLM_activities_used_columns().ollama_chat.invoke(context_dict)
    else:
        return ChatLLM().invoke(context_dict)
