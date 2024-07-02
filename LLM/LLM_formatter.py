from langchain_groq import ChatGroq
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
import re
import os


class LLM_formatter:

    def __init__(
        self,
        file_pipeline,
        api_key: str,
        temperature: float = 0,
        model_name: str = "llama3-70b-8192",
    ):
        self.chat = ChatGroq(
            temperature=temperature,
            groq_api_key=api_key,
            model_name=model_name,
        )

        # cleaning pipeline in text format
        self.pipeline_content = self.file_to_text(file_pipeline)

        # Template per descrivere il grafo e suggerire miglioramenti alla pipeline di pulizia dei dati
        PIPELINE_STANDARDIZER_TEMPLATE = """
        You are an expert in data preprocessing pipelines. Add comments describing the single operations in the pipeline, be the most detailed as possible.Return your response as a complete python file, including both changed and not changed functions, import and ..

        Instructions:
        1. Each cleaning operation on the data frame should be conatined in the same block of code without empty lines.
        2. Each operation on the data frame should be separated by a single empty line.
        3. Modify just the code contained in the run_pipeline function.
        4. uncomment run_pipeline(get_args()) and delete the rest in the main block.

        Cleaning Pipeline:{pipeline_content}

        Question: {question}
        """

        self.prompt = PromptTemplate(
            template=PIPELINE_STANDARDIZER_TEMPLATE,
            input_variables=["pipeline_content", "question"],
        )

        self.chat_chain = LLMChain(
            llm=self.chat, prompt=self.prompt, verbose=False
        )

    def file_to_text(self, file):
        # Leggi il contenuto del file e convertilo in testo
        content = None
        try:
            with open(file, "r") as file:
                content = file.read()

        except Exception as e:
            print(f"An error occurred: {e}")
        return content

    def standardize(self) -> str:
        response = self.chat_chain.invoke(
            {
                "pipeline_content": self.pipeline_content,
                "question": "Description and Suggestions:",
            }
        )
        # Use regular expression to find text between triple quotes
        extracted_text = re.search("```(.*?)```", response["text"], re.DOTALL)

        if extracted_text:
            # Get the matched group from the search
            code_to_write = extracted_text.group(1)
            # Specify the filename
            filename = "extracted_code.py"

            # Write the extracted text to a file
            with open(filename, "w") as file:
                file.write(code_to_write)
            print(f"Code has been successfully written to {filename}")
            print(os.path.abspath(filename))
            return str(os.path.abspath(filename))
        else:
            print("No triple-quoted text found.")
