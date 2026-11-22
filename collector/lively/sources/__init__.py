from .ashby import Ashby
from .greenhouse import Greenhouse
from .lever import Lever
from .workable import Workable

SOURCES = {s.name: s for s in (Greenhouse(), Lever(), Ashby(), Workable())}
