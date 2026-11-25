from .apply_button import ApplyButton
from .ats_api import AtsApi
from .board_listing import BoardListing
from .http_status import HttpStatus
from .soft404 import Soft404
from .valid_through import ValidThrough


def default_checkers():
    return [HttpStatus(), AtsApi(), BoardListing(), Soft404(), ApplyButton(), ValidThrough()]
