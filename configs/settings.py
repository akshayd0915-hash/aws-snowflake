"""
Central configuration management for Banking Data Platform.
Uses pydantic-settings for type-safe, validated configuration.
All environment variables are loaded from .env files.
"""

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AWSSettings(BaseSettings):
    """AWS connection and resource settings."""

    model_config = SettingsConfigDict(
        env_prefix="AWS_",
        env_file=".env",
        extra="ignore"
    )

    region: str = Field(default="us-east-1", description="AWS region")
    s3_bucket: str = Field(..., description="Raw S3 bucket name")
    s3_processed_bucket: str = Field(..., description="Processed S3 bucket name")
    access_key_id: str = Field(default="", description="AWS access key")
    secret_access_key: str = Field(default="", description="AWS secret key")


class SnowflakeSettings(BaseSettings):
    """Snowflake connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="SNOWFLAKE_",
        env_file=".env",
        extra="ignore"
    )

    account: str = Field(..., description="Snowflake account identifier")
    user: str = Field(..., description="Snowflake username")
    password: str = Field(..., description="Snowflake password")
    role: str = Field(default="SYSADMIN", description="Snowflake role")
    warehouse: str = Field(default="BANKING_WH", description="Snowflake warehouse")
    database: str = Field(default="BANKING_DB", description="Snowflake database")
    raw_schema: str = Field(default="RAW", description="Bronze/Raw schema")
    silver_schema: str = Field(default="SILVER", description="Silver schema")
    gold_schema: str = Field(default="GOLD", description="Gold schema")


class PipelineSettings(BaseSettings):
    """Pipeline execution settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

    environment: str = Field(default="development", description="Runtime environment")
    log_level: str = Field(default="INFO", description="Logging level")
    batch_size: int = Field(default=10000, description="Records per batch")
    max_retries: int = Field(default=3, description="Max retry attempts")
    retry_delay_seconds: int = Field(default=30, description="Delay between retries")


class Settings(BaseSettings):
    """Master settings — aggregates all config groups."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

    aws: AWSSettings = AWSSettings()
    snowflake: SnowflakeSettings = SnowflakeSettings()
    pipeline: PipelineSettings = PipelineSettings()


@lru_cache()
def get_settings() -> Settings:
    """
    Returns cached Settings instance.
    lru_cache ensures this is only instantiated once per process.
    Usage: from configs.settings import get_settings
           settings = get_settings()
    """
    return Settings()