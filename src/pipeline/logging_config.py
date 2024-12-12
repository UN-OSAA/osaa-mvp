"""
Centralized logging configuration for the OSAA MVP Pipeline.

This module provides a consistent, structured logging approach 
across the entire project.
"""

import logging
import colorlog
import sys
import os
from typing import Optional, Union

def create_logger(
    name: Optional[str] = None, 
    log_level: Union[int, str] = logging.INFO, 
    log_dir: Optional[str] = None,
    log_file: Optional[str] = None
):
    """
    Create a structured, color-coded logger with optional file logging.
    
    :param name: Name of the logger (typically __name__)
    :param log_level: Logging level (default: logging.INFO)
    :param log_dir: Directory to store log files (optional)
    :param log_file: Specific log file name (optional)
    :return: Configured logger instance
    """
    # Create logger
    logger = colorlog.getLogger(name or __name__)
    logger.setLevel(log_level)
    logger.propagate = False

    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler with color
    console_handler = colorlog.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # Custom log format with clear structure
    formatter = colorlog.ColoredFormatter(
        '%(log_color)s[%(levelname)s]%(reset)s '
        '%(blue)s[%(name)s]%(reset)s '
        '%(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white'
        },
        secondary_log_colors={}
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional file logging
    if log_dir or log_file:
        # Ensure log directory exists
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        
        # Determine log file path
        if not log_file:
            log_file = f"{name or 'pipeline'}.log"
        
        if log_dir:
            log_file = os.path.join(log_dir, log_file)
        
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        
        # Plain text formatter for file logs
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger

def log_exception(logger, e, context=None):
    """
    Standardized exception logging with optional context.
    
    :param logger: Logger instance
    :param e: Exception object
    :param context: Optional additional context for the error
    """
    logger.critical('🚨 UNEXPECTED ERROR 🚨')
    logger.critical(f'Error Type: {type(e).__name__}')
    logger.critical(f'Error Details: {str(e)}')
    
    if context:
        logger.critical(f'Context: {context}')
    
    # Optional: Add troubleshooting steps or recommendations
    logger.critical('Troubleshooting:')
    logger.critical('  1. Check recent changes')
    logger.critical('  2. Verify input data')
    logger.critical('  3. Review system logs')
    logger.critical('  4. Consult project documentation')
