# OceanIQ Phase3 API

OceanIQ Phase3 API is a sophisticated backend service designed to democratize access to complex oceanographic data. By leveraging Google's Gemini AI and a robust RAG (Retrieval-Augmented Generation) pipeline, it enables users to query ocean data using natural language, automatically converting questions into optimized SQL queries.

## 🚀 Key Features

-   **Natural Language to SQL**: Powered by Gemini, converting plain English questions into precise SQL queries.
-   **RAG-Enhanced Accuracy**: "Always-on" Retrieval-Augmented Generation pipeline that retrieves relevant schema info and context to minimize hallucinations.
-   **Vector Search Integration**: Built-in FAISS pipeline for semantic search capabilities (integrated into the RAG workflow).
-   **High Performance**: Built on FastAPI for high throughput and low latency.
-   **Robust Data Handling**: Uses SQLAlchemy and Pandas for efficient database interaction and data processing.

## 🛠️ Architecture Overview

The system is composed of several key modules:

### 1. Core API (`main.py`)
The entry point of the application using FastAPI. It handles incoming HTTP requests, routing, and response formatting.
-   **Active Routes**:
    -   `POST /nl_query`: The main interface for natural language queries.
    -   `GET /`: Health check endpoint.

### 2. SQL AI Service (`services/sql_ai_gemini/`)
The brain of the operation.
-   **`main.py`**: Orchestrates the NL-to-SQL flow.
-   **`gemini_client.py`**: Handles communication with Google's Gemini API.
-   **`prompts.py`**: Contains carefully engineered prompts to guide the AI.
-   **`validator.py` & `sanitizer.py`**: Ensures generated SQL is safe and syntactically correct before execution.
-   **`executor.py`**: Runs the sanitized SQL against the database.

### 3. FAISS Pipeline (`faiss_pipeline/`)
Provides vector search capabilities to support the RAG system.
-   **`search.py`**: Core logic for semantic and geo-spatial searches.
-   **`index_store.py`**: Manages the FAISS index.
-   **`embeddings.py`**: Generates embeddings using `sentence-transformers`.

## 📦 Project Structure

```
ocean/
├── main.py                 # API Gateway
├── services/
│   ├── sql_ai_gemini/      # NL-to-SQL Logic (Gemini + RAG)
│   ├── faiss_service.py    # (Legacy) FAISS integration
│   └── db_service.py       # (Legacy) Database utilities
├── faiss_pipeline/         # Vector search core
├── utils/                  # Helper functions
├── data/                   # Local data storage
└── requirements.txt        # Python dependencies
```

## ⚙️ Configuration

The application requires a `.env` file in the root directory. Key variables include:

-   `DATABASE_URL`: Connection string for the PostgreSQL database.
-   `GEMINI_API_KEY`: API key for Google Gemini.
-   `OPENAI_API_KEY`: (Optional) If using OpenAI models.

## 🔧 Setup & Installation

1.  **Clone the Repository**:
    ```bash
    git clone <repository-url>
    cd ocean
    ```

2.  **Create Virtual Environment**:
    ```bash
    python -m venv .venv
    # Windows
    .venv\Scripts\activate
    # Linux/Mac
    source .venv/bin/activate
    ```

3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run the Server**:
    ```bash
    uvicorn main:app --reload
    ```
    The API will be live at `http://localhost:8000`.

## 📖 Usage Example

### Natural Language Query

**Endpoint**: `POST /nl_query`

**Description**: Ask a question in plain English. The system uses RAG to understand the context and generates a SQL query to fetch the answer.

**Request**:
```json
{
  "question": "What is the maximum temperature recorded in the North Atlantic?",
  "top_k": 5
}
```

**Response**:
```json
{
  "sql_query": "SELECT MAX(temp) FROM measurements WHERE ...",
  "result": [
    { "max_temp": 28.5 }
  ],
  "explanation": "Found the maximum temperature..."
}
```

## 📝 Notes

-   **Legacy Routes**: Routes like `/search`, `/geo_search`, and `/profile` in `main.py` are currently commented out but preserved for future reference or reactivation.
-   **RAG System**: The RAG system is configured to be "always on" to ensure high accuracy for complex oceanographic queries.
