from LLM.LLM_activities_descriptor import LLM_activities_descriptor
from LLM.LLM_formatter import LLM_formatter


formatter = LLM_formatter("pipelines/census_pipeline.py", api_key="MY_APY_KEY")

extracted_file = formatter.standardize()

descriptor = LLM_activities_descriptor(extracted_file, api_key="MY_APY_KEY")

activities_description = descriptor.descript()

activities_description_dict = (
    i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
        activities_description.replace("pipeline = ", "")
    )
)
