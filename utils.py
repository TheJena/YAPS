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

from argparse import (
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    FileType,
    Namespace,
)
from black import __path__ as BLACK_PATH, __version__ as BLACK_VERSION
from logging import warning
from os.path import join as join_path, isdir, isfile
from subprocess import check_output
import random
import yaml


try:
    from yaml import CSafeDumper as SafeDumper, CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeDumper, SafeLoader


_PARSE_ARGS = None


def black(pycode, line_length=79, target_version="py312"):
    """The uncompromising code formatter"""
    assert isdir(BLACK_PATH[0]) and isfile(
        join_path(BLACK_PATH[0], "__main__.py")
    ), f"{BLACK_PATH=}"
    assert int(BLACK_VERSION.split(".")[0]) >= 24, f"{BLACK_VERSION=}"

    try:
        return check_output(
            [
                "black",
                "--code",
                pycode,
                f"--line-length={line_length!s}",
                f"--required-version={BLACK_VERSION}",
                "--safe",
                f"--target-version={target_version}",
            ],
            text=True,
        )
    except Exception as e:
        warning(f"Black failed ({e!s})")
        return pycode


def dump(code=None, data=None, io_obj=None):
    assert io_obj is not None, repr(io_obj)
    assert code is not None or data is not None, f"{code!r}\n{data!r}"

    if code is not None:
        data = i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
            code
        )
    yaml.dump(data, default_flow_style=False, Dumper=SafeDumper, stream=io_obj)


def i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
    *args, **kwargs
):
    raise NotImplementedError()


def keep_random_element_in_place(lst):
    if not lst or len(lst) == 0:
        return list()  # None (?)
    random_element = random.choice(lst)
    lst.clear()
    lst.append(random_element)
    return random_element


def load(io_obj):
    return yaml.load(io_obj, loader=SafeLoader)


def parsed_args() -> Namespace:
    """
    Parses command line arguments
    """
    global _PARSE_ARGS
    if _PARSE_ARGS is not None:
        return _PARSE_ARGS

    parser = ArgumentParser(
        allow_abbrev=True,
        description="TODO",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--input-data",
        help="datasets/car.csv",
        dest="dataset",
        metavar="file.csv",
        type=FileType("r"),
    )
    parser.add_argument(
        "-r",
        "--raw-pipeline",
        help="pipelines/raw/car.py",
        dest="raw_pipeline",
        metavar="file.py",
        type=FileType("r"),
    )
    parser.add_argument(
        "-f",
        "--formatted-pipeline",
        dest="formatted_pipeline",
        help="LLM_formatter($raw_pipeline)",
        metavar="file.py",
        type=FileType("w+"),
    )
    parser.add_argument(
        "-d",
        "--pipeline-description",
        dest="pipeline_description",
        help="LLM_activities_descriptor(LLM_formatter($raw_pipeline))",
        metavar="file.py",
        type=FileType("w+"),
    )

    parser.add_argument(
        "-m",
        "--model",
        default="llama3.1:70b",
        dest="llm_name",
        help="model name, e.g., llama3-70b-8192",
        type=str,
    )
    llm_backend = parser.add_mutually_exclusive_group(required=True)
    llm_backend.add_argument(
        "-q", "--groq", action="store_true", dest="use_groq"
    )
    llm_backend.add_argument(
        "-o", "--ollama", action="store_true", dest="use_ollama"
    )

    parser.add_argument(
        "-s",
        "--frac",
        default=0.1,
        help="Sampling fraction [0.0 - 1.0]",
        type=float,
    )
    parser.add_argument(
        "-g",
        "--granularity_level",
        default=3,
        help="Granularity level: 1, 2 or 3",
        type=int,
    )
    prov_lvl = parser.add_mutually_exclusive_group(required=True)
    prov_lvl.add_argument(
        "-e",
        "--entity-type-level",
        action="store_true",
        dest="prov_entity_level",
    )
    prov_lvl.add_argument(
        "-c",
        "--column-type-level",
        action="store_true",
        dest="prov_column_level",
    )

    _PARSE_ARGS = parser.parse_args()
    return _PARSE_ARGS
