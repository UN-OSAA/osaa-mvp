"""
Custom exceptions for the pipeline module.

This module defines a hierarchy of exceptions to provide more 
precise error handling and debugging across the pipeline.
"""

class PipelineBaseError(Exception):
    """
    Base exception for all pipeline-related errors.
    
    All custom exceptions in the pipeline should inherit from this class.
    Provides a common base for catching and handling pipeline-specific errors.
    """
    pass

class ConfigurationError(PipelineBaseError):
    """
    Raised when there are configuration-related issues.
    
    This exception is used when:
    - Required configuration parameters are missing
    - Configuration values are invalid
    - Environment setup is incorrect
    """
    pass

class DataProcessingError(PipelineBaseError):
    """
    Raised when there are issues processing data.
    
    This exception covers errors related to:
    - Data transformation
    - Data validation
    - Data parsing
    - Unexpected data formats
    """
    pass

class S3OperationError(PipelineBaseError):
    """
    Raised for S3-specific operation errors.
    
    Covers issues such as:
    - Authentication failures
    - Connection problems
    - File upload/download errors
    - Bucket or object access issues
    """
    pass

class IngestError(PipelineBaseError):
    """
    Raised during the data ingestion process.
    
    Covers errors specific to data ingestion, including:
    - File conversion problems
    - Source data validation failures
    - Ingestion pipeline interruptions
    """
    pass

class DatabaseError(PipelineBaseError):
    """
    Raised for database-related errors.
    
    Includes issues such as:
    - Connection failures
    - Query execution problems
    - Data insertion/retrieval errors
    """
    pass

class NetworkError(PipelineBaseError):
    """
    Raised for network-related communication issues.
    
    Covers problems like:
    - Connection timeouts
    - DNS resolution failures
    - API communication errors
    """
    pass
