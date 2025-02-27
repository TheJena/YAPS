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

from langchain_groq import ChatGroq
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate


class ChatBot(object):
    def __init__(
        self,
        api_key: str,
        temperature: float = 0.2,
        model_name: str = "llama3-70b-8192",
    ):
        self.chat = ChatGroq(
            temperature=temperature,
            groq_api_key=api_key,
            model_name=model_name,
        )

    def generate_prompt(self, element: str) -> PromptTemplate:
        if element == "column":
            template = """
            Use three sentences maximum and keep the answer concise.
            Do not respond in a negative way like the code doesn't.
            The input is a dictionary that rappresent a column,
            'feature_name' is the name of the column, 'value' are the
            values of the element in the column and 'index' are the
            indexes of the values of the column.
            Look at the 'feature_name', 'value' and 'index' of the
            dictionary and use the code to expalain what appened.
            If the output column is present in the question look at
            the difference of the 'feature_name', 'value' and 'index'
            of the two dictionary and use the code given trough the
            context to give an explenation.

            Context: {context}
            Question: {question}
            """
        else:
            template = """
            Use three sentences maximum and keep the answer concise.
            Do not respond in a negative way like the code doesn't.
            The input is a dictionary that rappresent an entity of the
            dataFrame, 'feature_name' is the name of the column,
            'value' are the value of the element in the dataframe and
            'index' are the index of the value in the dataframe.
            Look at the 'feature_name', 'value' and 'index' of the
            dictionary and use the code to expalain what appened.
            If the output entity is present in the question look at
            the difference of the 'feature_name', 'value' and 'index'
            of the two dictionary and use the code given trough the
            context to give an explenation.

            Context: {context}
            Question: {question}
            """
        return PromptTemplate(template=template)

    def ask_question(
        self, context: str, input, type, output="optional", element="column"
    ) -> str:

        prompt = self.generate_prompt(element)
        self.chat_chain = LLMChain(llm=self.chat, prompt=prompt, verbose=False)

        if type == "WAS_INVALIDATED_BY":
            question = f"Why this code invalidate this {input}?"
        elif type == "WAS_GENERATED_BY":
            question = f"Why this code get {output} from {input}?"
        elif type == "USED":
            question = f"Why this code use this {input}?"

        response = self.chat_chain.invoke({"context": context, "question": question})
        return response["text"]
