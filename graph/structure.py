from typing import Dict, List
import uuid
import graph.constants as constants


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

    act_id = constants.NAMESPACE_ACTIVITY + str(uuid.uuid4())

    attributes = {
        "id": act_id,
        "function_name": function_name,
        "context": context,
        "used_features": used_features or [],
        "description": description,
        "generated_features": generated_features,
        "generated_records": generated_records,
        "deleted_used_features": deleted_used_features,
        "deleted_records": deleted_records,
        "code": code,
        "code_line": code_line,
        "tracker_id": (
            constants.NAMESPACE_TRACKER + tracker_id
            if tracker_id is not None
            else None
        ),
    }

    if other_attributes is not None:
        attributes.update(other_attributes)

    return attributes