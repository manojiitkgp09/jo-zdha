"""
Configuration management using pydantic-settings.
Environment variables are loaded from .env file or system environment.
"""
from pydantic import Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """Application settings with validation."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # Redis Configuration
    redis_url: str = Field(
        default="redis://127.0.0.1:6379/0",
        description="Redis connection URL"
    )
    redis_max_connections: int = Field(
        default=50,
        description="Maximum Redis connection pool size"
    )
    redis_socket_timeout: int = Field(
        default=5,
        description="Redis socket timeout in seconds"
    )
    redis_socket_connect_timeout: int = Field(
        default=5,
        description="Redis socket connect timeout in seconds"
    )

    # Web Scraping Configuration
    bse_base_url: str = Field(
        default="https://www.bseindia.com",
        description="BSE website base URL"
    )
    bse_homepage_path: str = Field(
        default="/markets/MarketInfo/BhavCopy.aspx",
        description="BSE Bhavcopy page path"
    )
    bse_download_link_id: str = Field(
        default="ContentPlaceHolder1_btnhylZip",
        description="HTML element ID for download link"
    )
    request_timeout: int = Field(
        default=30,
        description="HTTP request timeout in seconds"
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for HTTP requests"
    )
    retry_backoff_factor: float = Field(
        default=2.0,
        description="Exponential backoff factor for retries"
    )

    # CherryPy Server Configuration
    server_host: str = Field(
        default="0.0.0.0",
        description="Server host address"
    )
    server_port: int = Field(
        default=5000,
        description="Server port number"
    )

    # Application Configuration
    app_name: str = Field(
        default="BSE Stock Tracker",
        description="Application name"
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    environment: str = Field(
        default="development",
        description="Application environment (development, staging, production)"
    )

    # Feature Flags
    enable_metrics: bool = Field(
        default=True,
        description="Enable Prometheus metrics"
    )
    enable_background_jobs: bool = Field(
        default=True,
        description="Enable background job processing with RQ"
    )

    # Data Processing
    top_n_stocks: int = Field(
        default=10,
        description="Number of top stocks to display"
    )
    search_result_limit: int = Field(
        default=100,
        description="Maximum number of search results to return"
    )
    cache_ttl: int = Field(
        default=300,
        description="Cache TTL in seconds for top stocks"
    )

    @validator("log_level")
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()

    @validator("environment")
    def validate_environment(cls, v):
        valid_envs = ["development", "staging", "production"]
        if v.lower() not in valid_envs:
            raise ValueError(f"environment must be one of {valid_envs}")
        return v.lower()

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "production"

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "development"


# Global settings instance
settings = Settings()
