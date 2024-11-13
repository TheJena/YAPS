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

from datetime import timedelta
import time


def timing(log_file: str = None):
    """
    Measures the execution time of a function and appends it to a log file.

    :param str log_file: The name of the log file to append execution
                         times (optional).
    """

    def decorator(f):
        """
        A decorator for measuring the execution time of a function.

        :param callable f: The function to be measured.

        :return: The wrapped function.
        """

        # @wraps(f)
        def wrap(*args, **kwargs):
            """
            Wrapper function that measures execution time and logs it.

            :param args: Positional arguments for the function.
            :param kwargs: Keyword arguments for the function.

            :return: The result of the wrapped function.
            """
            tracker = args[0]
            code = tracker.global_state.code

            start_time = time.time()
            result = f(*args, **kwargs)
            elapsed_time = time.time() - start_time

            tracker.logger.info(
                msg=f"{f.__name__} function took "
                f"{str(timedelta(seconds=elapsed_time))}"
            )

            if log_file is not None:
                with open(log_file, "a") as file:
                    file.write(  #
                        f"{f.__module__};{f.__name__};{code};{elapsed_time}\n"
                    )

            return result

        return wrap

    return decorator


def suppress_tracking(f):
    """
    Disables the tracker tracking before executing the function.

    :param callable f: The function to be executed with tracking disabled.

    :return: The wrapped function.
    """

    def wrap(*args, **kwargs):
        """
        Wrapper function that temporarily disables tracking and
        then reverts it.

        :param args: Positional arguments for the function.
        :param kwargs: Keyword arguments for the function.

        :return: The result of the wrapped function.

        """
        tracker = args[0]
        temp1, temp2 = (
            tracker.enable_dataframe_warning_msg,
            tracker.dataframe_tracking,
        )
        tracker.enable_dataframe_warning_msg, tracker.dataframe_tracking = (
            False,
            False,
        )
        result = f(*args, **kwargs)
        tracker.dataframe_tracking, tracker.enable_dataframe_warning_msg = (
            temp1,
            temp2,
        )

        return result

    return wrap
