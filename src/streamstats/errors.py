class StreamStatsError(Exception):

    pass

class UnsupportedFormatError(StreamStatsError):

    pass

class InvalidEventError(StreamStatsError):

    pass

class InvalidTimestampError(StreamStatsError):

    pass

class CLIConfigurationError(StreamStatsError):

    pass