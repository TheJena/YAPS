from langchain_groq import ChatGroq
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
import re
import os


class LLM_activities_descriptor:

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
            You are an expert in data preprocessing pipelines. Return a list of name + (description of the single operations in the pipeline, python code of the operation), consider just the content of the run_pipeline function. Do not include sampling subscription and column name assignment and identification of the objects of the columns activities. Consider just the code after the subscription of the dataframe (df.subscribe()). Be the most detailed as possible.
            Return the result as a python dictionary with the operation name as key and the description as value.
            Do not refer to comments.

            Example:
            pipeline:
            import pandas as pd
            from sklearn.model_selection import train_test_split
            from sklearn.impute import SimpleImputer
            from sklearn.preprocessing import StandardScaler, OneHotEncoder
            from sklearn.compose import ColumnTransformer
            from sklearn.pipeline import Pipeline
            from sklearn.ensemble import RandomForestRegressor
            from sklearn.feature_selection import RFE
            from sklearn.metrics import mean_squared_error

            # Load dataset
            data = pd.read_csv('data.csv')

            # Identify features and target
            X = data.drop('target', axis=1)
            y = data['target']

            # Identify numerical and categorical columns
            num_features = X.select_dtypes(include=['int64', 'float64']).columns
            cat_features = X.select_dtypes(include=['object']).columns

            # Define preprocessing for numerical data
            num_transformer = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='mean')),
                ('scaler', StandardScaler())
            ])

            # Define preprocessing for categorical data
            cat_transformer = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='most_frequent')),
                ('onehot', OneHotEncoder(handle_unknown='ignore'))
            ])

            # Combine preprocessing steps
            preprocessor = ColumnTransformer(
                transformers=[
                    ('num', num_transformer, num_features),
                    ('cat', cat_transformer, cat_features)
                ])

            # Define the model
            model = RandomForestRegressor()

            # Create and evaluate the full pipeline
            pipeline = Pipeline(steps=[
                ('preprocessor', preprocessor),

                ('feature_selection', RFE(model, n_features_to_select=10)),

                ('model', model)
            ])

            # Split data into train and test sets
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            # Fit the model
            pipeline.fit(X_train, y_train)

            # Make predictions
            y_pred = pipeline.predict(X_test)

            # Evaluate the model
            mse = mean_squared_error(y_test, y_pred)
            response:
            ["Identify Features and Target": ("Separation of  the target variable from the feature variables by dropping the target column from the DataFrame", "X = data.drop('target', axis=1)
            y = data['target']")", "Identifiction of numerical and categorical columns": ("Determine which columns are numerical and which are categorical using select_dtypes", "    num_features = X.select_dtypes(include=['int64', 'float64']).columns
            cat_features = X.select_dtypes(include=['object']).columns") ]


            Cleaning Pipeline:{pipeline_content}
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
        try:
            with open(file, "r") as file:
                content = file.read()

        except Exception as e:
            print(f"An error occurred: {e}")
        return content

    def descript(self) -> str:
        response = self.chat_chain.invoke(
            {"pipeline_content": self.pipeline_content}
        )
        # Use regular expression to find text between triple quotes
        extracted_text = re.search("```(.*?)```", response["text"], re.DOTALL)

        if extracted_text:
            return extracted_text.group(1)
