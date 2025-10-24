from dataclasses import dataclass
from enum import Enum


class SourceType(Enum):
    FILE = "file"
    DIRECTORY = "directory"
    REPOSITORY = "repository"


class LocationType(Enum):
    HTTP = "http"
    PATH = "path"


class FileEncoding(Enum):
    TEXT = "text"
    BINARY = "binary"
    NA = "NA"


@dataclass
class SourceCharacteristics:
    source_type: SourceType
    location_type: LocationType
    file_encoding: FileEncoding


class SourceClassification(Enum):
    REMOTE_TEXT_FILE = SourceCharacteristics(
        SourceType.FILE, LocationType.HTTP, FileEncoding.TEXT
    )
    REMOTE_BINARY_FILE = SourceCharacteristics(
        SourceType.FILE, LocationType.HTTP, FileEncoding.BINARY
    )
    LOCAL_TEXT_FILE = SourceCharacteristics(
        SourceType.FILE, LocationType.PATH, FileEncoding.TEXT
    )
    LOCAL_BINARY_FILE = SourceCharacteristics(
        SourceType.FILE, LocationType.PATH, FileEncoding.BINARY
    )
    REMOTE_REPOSITORY = SourceCharacteristics(
        SourceType.REPOSITORY, LocationType.HTTP, FileEncoding.NA
    )

    LOCAL_DIRECTORY = SourceCharacteristics(
        SourceType.DIRECTORY, LocationType.PATH, FileEncoding.NA
    )
