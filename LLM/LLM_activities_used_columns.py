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

from langchain_groq import ChatGroq
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
import re
import os


class LLM_activities_used_columns:

    def __init__(
        self,
        api_key: str,
        temperature: float = 0,
        model_name: str = "llama3-70b-8192",
    ):
        self.chat = ChatGroq(
            temperature=temperature,
            groq_api_key=api_key,
            model_name=model_name,
        )

        # Template per descrivere il grafo e suggerire miglioramenti alla pipeline di pulizia dei dati
        PIPELINE_STANDARDIZER_TEMPLATE = """
            You are receiving the dataframe before and after the operation, the code and the description of the operation.
            Return me a python list with the name of the columns in the dataframe before used by the operation. Limit your observations just to this inputs and the code of the operation. Do not make assumptions.
            Return the python list between `[]`.


            For example if a column is dropped only the dropped column is used.
            Example of answer: ```["column1", "column2", "column3"]```
            Write the list inside ``` ```

            dataframe before:{df_before}
            dataframe after:{df_after}

            code:{code}
            description:{description}
        """

        self.prompt = PromptTemplate(
            template=PIPELINE_STANDARDIZER_TEMPLATE,
            input_variables=["df_before", "df_after", "code", "description"],
        )

        self.chat_chain = LLMChain(
            llm=self.chat, prompt=self.prompt, verbose=False
        )

    def give_columns(self, df_before, df_after, code, description) -> str:

        response = self.chat_chain.invoke(
            {
                "df_before": df_before,
                "df_after": df_after,
                "code": code,
                "description": description,
            }
        )
        # Use regular expression to find text between triple quotes
        extracted_text = re.search("```(.*?)```", response["text"], re.DOTALL)

        if extracted_text:
            return extracted_text.group(1)
