import sys
import structlog
import orjson
from typing import Any, Dict

def setup_logging(debug: bool = False) -> Dict[str, Any]:
    """
    Combines Django, Uvicorn, and Structlog into a single JSON stream (or colored text in DEV).
    """
    
    shared_processors = [
        structlog.contextvars.merge_contextvars,  
        structlog.stdlib.add_logger_name,         
        structlog.stdlib.add_log_level,           
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"), 
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,     
    ]

    if debug:
        renderer = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer(serializer=orjson.dumps)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json_formatter": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": renderer,
                "foreign_pre_chain": shared_processors,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "json_formatter",
                "stream": sys.stdout,
            },
        },
        "root": {
            "handlers": ["console"],
            "level": "INFO",
        },
        "loggers": {
            "django": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "uvicorn": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["console"], "level": "INFO", "propagate": False},
            "aiokafka": {"handlers": ["console"], "level": "WARNING", "propagate": False},
        }
    }
    
    return config