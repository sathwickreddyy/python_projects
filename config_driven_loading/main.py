"""
FastAPI application for config-driven data ingestion.

@author sathwick
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from models.core.logging_config import setup_logging, DataIngestionLogger


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    setup_logging()
    logger = DataIngestionLogger(__name__)
    logger.info("Starting Config-Driven Data Ingestion API")

    yield

    # Shutdown
    logger.info("Shutting down Config-Driven Data Ingestion API")


app = FastAPI(
    title="Config-Driven Data Ingestion API",
    description="A comprehensive data ingestion library with support for CSV, JSON, and Protocol Buffers",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
# app.include_router(router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Config-Driven Data Ingestion API",
        "version": "1.0.0",
        "documentation": "/docs"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)