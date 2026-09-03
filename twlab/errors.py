"""twlab exception types."""


class TwlabError(Exception):
    """Base class for twlab errors."""


class UnknownDataKeyError(KeyError, TwlabError):
    """A Data Key that does not exist in the Catalog."""


class ParseError(TwlabError):
    """A source response did not match the expected format.

    Raised loudly so a silent source format change fails in the parser,
    not downstream in materialized data.
    """


class DeriveError(TwlabError):
    """A derived (ETL) Dataset could not be computed consistently from its inputs."""


class DatasetNotMaterializedError(FileNotFoundError, TwlabError):
    """No Parquet Wide Frame exists yet for the requested Data Key."""
