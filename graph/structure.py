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

from typing import Dict, List
from graph.constants import (
    NAMESPACE_ACTIVITY,
    NAMESPACE_TRACKER,
    NAMESPACE_ENTITY,
    NAMESPACE_COLUMN,
)
import uuid


def create_activity(
    function_name: str,
    context: str = None,
    used_features: List[any] = None,
    description: str = None,
    other_attributes: Dict[str, any] = None,
    generated_features: List[any] = None,
    generated_records: List[any] = None,
    deleted_used_features: List[any] = None,
    deleted_records: List[any] = None,
    code: str = None,
    code_line: str = None,
    tracker_id: str = None,
    exception_text: str = None,
) -> str:
    """
    Create a provenance activity and add it to the current activities list.
    Return the ID of the new provenance activity.

    :param function_name: The name of the function.
    :param used_features: The list of used features.
    :param description: The description of the activity.
    :param other_attributes: Other attributes of the activity.
    :param generated_features: The list of generated features.
    :param generated_records: The list of generated records.
    :param deleted_used_features: The list of deleted used features.
    :param deleted_records: The list of deleted records.
    :param code: The code of the activity.
    :param code_line: The code line of the activity.
    :param tracker_id: The tracker ID.
    :return: The ID of the new provenance activity.
    """

    act_id = NAMESPACE_ACTIVITY + str(uuid.uuid4())

    attributes = {
        "code": code,
        "code_line": code_line,
        "context": context,
        "deleted_used_features": deleted_used_features,
        "deleted_records": deleted_records,
        "description": description,
        "function_name": function_name,
        "generated_features": generated_features,
        "generated_records": generated_records,
        "id": act_id,
        "runtime_exceptions": "An exception occured here or before "
        + "("
        + exception_text
        + ")",
        "tracker_id": (
            NAMESPACE_TRACKER + tracker_id  #
            if tracker_id is not None  #
            else None
        ),
        "used_features": used_features or list(),
    }

    if other_attributes is not None:
        attributes.update(other_attributes)

    return attributes


def create_entity(
    value, feature_name: str, index: int, instance: str = None
) -> Dict[str, any]:
    """
    Create a provenance entity.
    Return a dictionary with the ID and the record ID of the entity.

    :param value: The value of the entity.
    :param feature_name: The feature name of the entity.
    :param index: The index of the entity.
    :param instance: The instance of the entity.
    :return: A dictionary with the ID and the record ID of the entity.
    """

    entity = {
        "id": NAMESPACE_ENTITY + str(uuid.uuid4()),
        "value": value,
        "type": type(value).__name__,
        "feature_name": feature_name,
        "index": index,
        "name": instance or list(),
    }

    return entity


def create_column(value, index, instance: str = None) -> Dict[str, any]:
    """
    Create a provenance entity.
    Return a dictionary with the ID and the record ID of the entity.

    :param value: The value of the entity.
    :return: A dictionary with the ID and the record ID of the entity.
    """

    column = {
        "id": NAMESPACE_COLUMN + str(uuid.uuid4()),
        "value": value,
        "index": index,
        "instance": instance or list(),
    }

    return column


def create_relation(
    act_id: str,
    generated: List[any] = None,
    used: List[any] = None,
    invalidated: List[any] = None,
    same: bool = False,
) -> None:
    """
    Create a provenance relation and add it to the current relations list.

    :param act_id: The ID of the activity.
    :param generated: The list of generated entities.
    :param used: The list of used entities.
    :param invalidated: The list of invalidated entities.
    :param same: A boolean indicating whether the generated and used
                 entities are the same.
    :return: None
    """

    generated = generated or list()
    used = used or list()
    invalidated = invalidated or list()

    if same:
        invalidated = list()

    return (generated, used, invalidated, same, act_id)


def create_relation_column(
    act_id: str,
    generated: List[any] = None,
    used: List[any] = None,
    invalidated: List[any] = None,
    same: bool = False,
) -> None:
    """
    Create a provenance relation and add it to the current relations list.

    :param act_id: The ID of the activity.
    :param generated: The list of generated columns.
    :param used: The list of used columns.
    :param invalidated: The list of invalidated columns.
    :param same: A boolean indicating whether the generated and used
                 entities are the same.
    :return: None
    """

    generated = generated or list()
    used = used or list()
    invalidated = invalidated or list()

    if same:
        invalidated = list()

    return (generated, used, invalidated, same, act_id)
