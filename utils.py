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

from os.path import join as join_path, isdir, isfile
from subprocess import check_output
from black import __path__ as BLACK_PATH, __version__ as BLACK_VERSION
import random


def black(pycode, line_length=79, target_version="py312"):
    """The uncompromising code formatter"""
    assert isdir(BLACK_PATH[0]) and isfile(
        join_path(BLACK_PATH[0], "__main__.py")
    ), f"{BLACK_PATH=}"
    assert int(BLACK_VERSION.split(".")[0]) >= 24, f"{BLACK_VERSION=}"

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


def i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
    *args, **kwargs
):
    raise NotImplementedError()


def keep_random_element_in_place(lst):
    if not lst or len(lst) == 0:
        return []  # Return None if the list is empty
    random_element = random.choice(lst)
    lst.clear()
    lst.append(random_element)
    return random_element
