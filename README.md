## Installation

To use the tool, follow these steps:

1. **Clone the repository**:
   ```
   git clone https://github.com/pasqualeleonardolazzaro/DPDS_LLM.git
   ```

2. **Navigate to the project directory**:
   ```
   cd DPDS_LLM
   ```

3. **Create and activate a virtual environment (optional but recommended)**:
   On Unix or MacOS:
   ```
   python3 -m venv venv
   source venv/bin/activate
   ```

   On Windows:
   ```
   python -m venv venv
   .\venv\Scripts\activate
   ```

4. **Install the requirements**:
   ```
   pip install -r requirements.txt
   ```

## Neo4j (Docker)


- **Start Neo4j in the background by executing the following command:**:
    
      cd neo4j
      docker compose up -d

- **To stop Neo4j, run the following command:**:

      cd neo4j
      docker compose down -v

### Access the Neo4j Web Interface

To access the Neo4j web interface, open the following URL in your web browser:

http://localhost:7474/browser/

#### Default Credentials

- **User**: `MY_NEO4J_USERNAME`
- **Password**: `MY_NEO4J_PASSWORD`



