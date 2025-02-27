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

# Constants:
NAMESPACE_ACTIVITY = "activity:"
NAMESPACE_COLUMN = "column:"
NAMESPACE_ENTITY = "entity:"
NAMESPACE_TRACKER = "tracker:"

# Neo4j CONSTANTS
ACTIVITY_CONSTRAINT = "constraint_activity_id"
ACTIVITY_LABEL = "Activity"
BELONGS_RELATION = "BELONGS_TO"
COLUMN_CONSTRAINT = "constraint_column_id"
COLUMN_LABEL = "Column"
DERIVATION_RELATION = "WAS_DERIVED_FROM"
ENTITY_CONSTRAINT = "constraint_entity_id"
ENTITY_LABEL = "Entity"
GENERATION_RELATION = "WAS_GENERATED_BY"
INVALIDATION_RELATION = "WAS_INVALIDATED_BY"
NEXT_RELATION = "NEXT"
USED_RELATION = "USED"

FUNCTION_EXECUTION_TIMES = "function_execution_times.log"
NEO4j_QUERY_EXECUTION_TIMES = "neo4j_query_execution_times.log"
