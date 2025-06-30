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

from argparse import (
    RawDescriptionHelpFormatter,
    ArgumentParser,
    FileType,
    Namespace,
)
from black import __path__ as BLACK_PATH, __version__ as BLACK_VERSION
from collections import defaultdict
from datetime import datetime
from itertools import chain
from logging import (
    DEBUG,
    Formatter,
    INFO,
    StreamHandler,
    WARNING,
    basicConfig,
    debug,
    getLogger,
    info,
    warning,
)
from os import makedirs
from os.path import abspath, join as join_path, isdir, isfile
from queue import Queue as FIFO
from SECRET import black_magic  # from functools import lru_cache
from subprocess import check_output
from tempfile import NamedTemporaryFile
from textwrap import dedent, fill, indent
from time import time
import gzip
import logging
import pandas as pd
import pickle
import random
import yaml


try:
    from yaml import CSafeDumper as SafeDumper, CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeDumper, SafeLoader


_PARSE_ARGS = None


class CustomLogFormatter(Formatter):
    """
    Colorize logging output

    https://stackoverflow.com/a/56944256
    """

    ANSI_escape_color_codes = dict(
        black="\033[0;30m",
        blue="\033[0;34m",
        brown="\033[0;33m",
        cyan="\033[0;36m",
        green="\033[0;32m",
        purple="\033[0;35m",
        red="\033[0;31m",
        reset="\033[0m",
        white="\033[37m",
        yellow="\033[1;33m",
    )

    lvl2color = dict(
        DEBUG="white",
        INFO="green",
        WARNING="red",
        ERROR="yellow",
        CRITICAL="purple",
    )

    def format(self, record) -> str:
        record.levelname = "".join(
            (
                self.ANSI_escape_color_codes[
                    self.lvl2color.get(record.levelname, "reset")
                ],
                record.levelname,
                self.ANSI_escape_color_codes["reset"],
            )
        )
        return super(CustomLogFormatter, self).format(record)


class Singleton(object):
    """
    A decorator for implementing the Singleton design pattern.

    :param callable cls: The class to be turned into a Singleton.
    """

    def __init__(self, cls) -> None:
        self.cls = cls
        self.instance = None

    def __call__(self, *args, **kwargs):
        """
        Overrides the call method to create a Singleton instance of the class.

        :param args: Positional arguments for the class constructor.
        :param kwargs: Keyword arguments for the class constructor.

        :return: The Singleton instance of the class.
        """
        if self.instance is None:
            self.instance = self.cls(*args, **kwargs)
        return self.instance


def black(pycode, line_length=79, target_version="py312"):
    """The uncompromising code formatter"""
    assert isdir(BLACK_PATH[0]) and isfile(
        join_path(BLACK_PATH[0], "__main__.py")
    ), f"{BLACK_PATH=}"
    assert int(BLACK_VERSION.split(".")[0]) >= 24, f"{BLACK_VERSION=}"

    try:
        ret = check_output(
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
        warning(f"Black failed ({e!s})\n\n{pycode!s}")
        return pycode
    else:
        if kwargs.pop("__eval", False):
            return i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
                ret
            )
    return ret


def foreign_modules():
    return sorted(
        set(
            getLogger(name).name.split(".")[0]
            for name in logging.root.manager.loggerDict
        ),
        key=str.lower,
    )


def initialize_logging(suffix_filename, level=INFO, debug_mode=False):
    logfile = NamedTemporaryFile(
        prefix=datetime.now().strftime("%Y_%m_%d__%H_%M_%S____"),
        suffix="____"
        + str(
            suffix_filename.strip("_")
            + str(".txt" if "." not in suffix_filename else "")
        ),
        delete=False,
    )
    basicConfig(
        filename=logfile.name,
        force=True,
        format="\t".join(
            (
                "[{levelname: ^9s}| {module}+L{lineno} | PID-{process}]",
                "{message}",
            )
        ),
        level=DEBUG,  # use always the most verbose level for log file
        style="{",
    )

    root_logger = getLogger()
    stderr_handle = StreamHandler()
    stderr_handle.setLevel(level if not debug_mode else DEBUG)
    stderr_handle.setFormatter(
        CustomLogFormatter(
            "{levelname}: {message}",
            style="{",
        )
    )
    root_logger.addHandler(stderr_handle)

    # make foreign modules quiet in logfile
    for module_name in (
        "aiohttp",
        "asyncio",
        "blib2to3",
        "charset_normalizer",
        "concurrent",
        "httpcore",
        "httpx",
        "langchain",
        "langchain_community",
        "langchain_core",
        "langchain_groq",
        "langchain_ollama",
        "langsmith",
        "neo4j",
        "packaging",
        "requests",
        "sklearn",
        "urllib3",
    ):
        getLogger(module_name).setLevel(WARNING)

    info("Temporary file with debugging log will be available here:")
    info(f"{' ' * 4}{logfile.name}")
    debug(f"{'~' * 120}")
    debug("")
    debug("Logging initialized and temporary file created")
    debug("")


@black_magic
def i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
    *args, **kwargs
):
    for i, a in enumerate(args):
        warning(f"{i:02d}-th\t\t{a!r}")
    for k, v in sorted(kwargs.items(), key=lambda t: t[0].lower()):
        warning(f"{k:<32}\t\t{v!r}")
    raise NotImplementedError(
        r"""return eval(*args, **kwargs)"""
    )


def keep_random_element_in_place(lst):
    if not lst or len(lst) == 0:
        return list()  # None (?)
    random_element = random.choice(lst)
    lst.clear()
    lst.append(random_element)
    return random_element


def parsed_args() -> Namespace:
    """
    Parses command line arguments
    """
    global _PARSE_ARGS
    if _PARSE_ARGS is not None:
        return _PARSE_ARGS

    parser = ArgumentParser(
        allow_abbrev=True,
        description=indent(
            "\n\n".join(
                fill(" ".join(p.split()), width=80 - 8)
                for p in (
                    """The sinking of the Titanic is an infamous
                       shipwreck happened on April 15, 1912.  During
                       her maiden voyage, the widely considered
                       `unsinkable' RMS Titanic sank after colliding
                       with an iceberg.  Unfortunately, there weren't
                       enough lifeboats for everyone on board,
                       resulting in the death of 1'502 out of 2'224
                       passengers and crew.""",
                    """While there was some element of luck involved
                       in surviving, it seems some groups of people
                       were more likely to survive than others.""",
                    """The chosen default data-engineering pipeline is
                       supposed to pre-process/prepare a dataset for a
                       predictive model able to answers the question:
                       'Who was more likely to survive?'""",
                )
            )
            + dedent(
                """

                Given feature list:
                - Passenger ID
                - Survived    (0 = No, 1 = Yes)
                - Pclass      (ticket class: 1 = 1st, 2 = 2nd, 3 = 3rd)
                - Name        (passenger name)
                - Sex         (gender)
                - Age         (in years)
                - SibSp       (No. of siblings / spouses aboard the Titanic)
                - Parch       (No. of parents / children aboard the Titanic)
                - Ticket      (ticket number)
                - Fare        (passenger fare)
                - Cabin       (cabin number)
                - Embarked    (port of embarkation: C = Cherbourg, Q =
                               Queenstown, S = Southampton)"""
            ),
            prefix="\t",
        ),
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--input-data",
        help="datasets/raw/titanic.csv",
        dest="dataset",
        metavar="file.csv",
        type=FileType("r"),
    )
    parser.add_argument(
        "-r",
        "--raw-pipeline",
        help="pipelines/raw/titanic.py",
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
        type=str,
    )
    parser.add_argument(
        "-d",
        "--pipeline-description",
        dest="pipeline_description",
        help="LLM_activities_descriptor(LLM_formatter($raw_pipeline))",
        metavar="file.yaml",
        type=str,
    )
    parser.add_argument(
        "-o",
        "--output-data",
        help="datasets/clean/titanic.csv",
        dest="output",
        metavar="file.csv",
        type=FileType("w"),
    )

    parser.add_argument(
        "-m",
        "--model",
        default="llama3.3:70b",
        dest="llm_name",
        help="model name, e.g., llama-3.3-70b-versatile",
        type=str,
    )
    parser.add_argument(
        "-k",
        "--keep-alive",
        default="6h",
        dest="keep_alive",
        help="how long the model will stay loaded into memory",
        type=str,
    )
    parser.add_argument(
        "-x",
        "--num-ctx",
        default=8192,
        dest="num_ctx",
        help="size of the context window used to generate the next token",
        type=int,
    )
    llm_backend = parser.add_mutually_exclusive_group(required=True)
    llm_backend.add_argument("--groq", action="store_true", dest="use_groq")
    llm_backend.add_argument(
        "--ollama",
        action="store_true",
        dest="use_ollama",
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

    parser.add_argument(
        "-v",
        "--debug",
        "--verbose",
        help="increase logging level",
        action="store_true",
        dest="verbose",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        "--silent",
        help="decrease logging level",
        action="store_true",
        dest="quiet",
    )

    _PARSE_ARGS = parser.parse_args()
    for attr in ("formatted_pipeline", "pipeline_description"):
        path = getattr(_PARSE_ARGS, attr)
        if path is not None:
            setattr(_PARSE_ARGS, attr, abspath(path))
    return _PARSE_ARGS


def serialize(data, io_obj):
    assert io_obj is not None
    debug(f"{io_obj!r}")
    if not hasattr(io_obj, "name"):
        assert isinstance(
            io_obj, str
        ), "Please provide io_obj|str, got {io_obj!r} instead"
        if "." not in io_obj:
            io_obj += ".pkl.gz"
        io_obj = open(io_obj, "w")
    debug(f"{io_obj!r}")
    if (
        (io_obj.name.endswith(".csv") or "stdout" in io_obj.name)
        and hasattr(data, "to_csv")
        and callable(getattr(data, "to_csv"))
    ):
        debug("detected CSV output format")
        data.loc[
            :,
            sorted(data.columns, key=str.lower),
        ].sort_index().to_csv(
            io_obj if "stdout" in io_obj.name else io_obj.name,
        )
    elif (
        io_obj.name.endswith(".xlsx")
        and hasattr(data, "to_excel")
        and callable(getattr(data, "to_excel"))
    ):
        debug("detected EXCEL output format")
        data.loc[
            :,
            sorted(data.columns, key=str.lower),
        ].sort_index().to_excel(io_obj.name)
    else:
        debug("falling back to default GZIPPED PICKLE output format")
        pickle.dump(
            data,
            (
                gzip.open(
                    ".".join(io_obj.name.split(".")[:-2] + ["pkl", "gz"]),
                    "wb",
                )
                if io_obj.name.endswith(".gz")
                else open(
                    ".".join(io_obj.name.split(".")[:-1] + ["pkl"]),
                    "wb",
                )
            ),
        )
    debug(f"serialized data to {io_obj!r}")


def yaml_dump(code=None, data=None, io_obj=None):
    assert io_obj is not None, repr(io_obj)
    assert code is not None or data is not None, f"{code!r}\n{data!r}"

    if isinstance(io_obj, str):
        assert io_obj.endswith(".yaml"), repr(io_obj)
        io_obj = open(io_obj, "w")

    if code is not None:
        data = i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
            code
        )

    if io_obj.seekable:
        io_obj.seek(0)  # truncate file
    debug(f"{io_obj!r}")
    yaml.dump(data, default_flow_style=False, Dumper=SafeDumper, stream=io_obj)
    io_obj.close()


def yaml_load(io_obj):
    debug(f"{io_obj=}")
    if isinstance(io_obj, str):
        assert io_obj.endswith(".yaml"), repr(io_obj)
        io_obj = open(io_obj, "r")
    return yaml.load(io_obj, Loader=SafeLoader)
