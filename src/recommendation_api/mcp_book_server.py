from fastmcp import FastMCP
from common import models, SettingsInstance as S
from pathlib import Path
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings

mcp = FastMCP("book")

emb = OpenAIEmbeddings(openai_api_key=S.openai_api_key, model=S.model_name)
vec_store = FAISS.load_local(Path(S.data_dir) / "vector_store")

@mcp.tool(
    annotations={
        "title": "Search Catalog by Keyword",
        "readOnlyHint": True,
    }
)
def search_catalog(keyword: str, k: int = 5):
    docs = vec_store.similarity_search(keyword, k=k)
    return [{"book_id": d.metadata["book_id"], "snippet": d.page_content[:200]} for d in docs]

if __name__ == "__main__":
    mcp.run(transport="stdio") 