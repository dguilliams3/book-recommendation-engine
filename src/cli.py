import asyncio
from pathlib import Path

import typer
from common.settings import settings as S

# Import the async entry-points of the three worker jobs using relative imports
from .ingestion_service.main import ingest as _ingest_job
from .graph_refresher.main import main as _graph_job
from .book_enrichment_worker.main import main as _enrich_job

app = typer.Typer(help="Book-Recommendation-Engine control-plane CLI")


# ---------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------
@app.command()
def ingest(
    catalog: Path = typer.Option(S.catalog_csv_path, help="Catalog CSV file"),
    students: Path = typer.Option(S.students_csv_path, help="Students CSV file"),
    checkouts: Path = typer.Option(S.checkouts_csv_path, help="Checkouts CSV file"),
):
    """Run the full CSV → Postgres → FAISS ingestion pipeline."""

    # NOTE: the underlying ingest() currently ignores the parameters; they are
    # wired in for future flexibility and to give developers a familiar UX.
    typer.echo("🚚  Ingestion job started…")
    asyncio.run(_ingest_job())
    typer.echo("✅  Ingestion job finished")


# ---------------------------------------------------------------------
# enrich
# ---------------------------------------------------------------------
@app.command()
def enrich():
    """Enrich missing catalog metadata via Google Books / Open Library."""
    typer.echo("🔎  Book-enrichment job started…")
    asyncio.run(_enrich_job())
    typer.echo("✅  Book-enrichment job finished")


# ---------------------------------------------------------------------
# graph
# ---------------------------------------------------------------------
@app.command()
def graph():
    """Rebuild student-similarity graph & embeddings."""
    typer.echo("🌐 graph-refresher job started…")
    asyncio.run(_graph_job())
    typer.echo("✅  graph-refresher job finished")


if __name__ == "__main__":
    app()
