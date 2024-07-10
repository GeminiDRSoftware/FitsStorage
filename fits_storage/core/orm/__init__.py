from sqlalchemy.orm import declarative_base

# Add __allow_unmapped__ for SQLAlchemy 2.0 compatibility
class _Base:
    """Compatibility class that enforces the __allow_unmapped__ attribute in
    the declarative Base class.
    """
    __allow_unmapped__ = True

Base = declarative_base(cls=_Base)
