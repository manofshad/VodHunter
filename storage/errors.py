"""Errors raised by video storage commands."""


class VideoMutationError(Exception):
    """Base class for a video lifecycle mutation failure."""


class VideoNotFoundError(VideoMutationError):
    """The requested video does not exist."""


class VideoOwnerMismatchError(VideoMutationError):
    """The actor does not own the requested video."""


class InvalidVideoStateTransitionError(VideoMutationError):
    """The requested video lifecycle transition is not allowed."""

    def __init__(self, current_status: str):
        self.current_status = str(current_status)
        super().__init__(f"Cannot apply requested transition from status '{self.current_status}'")
