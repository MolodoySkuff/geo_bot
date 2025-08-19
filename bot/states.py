from aiogram.fsm.state import StatesGroup, State

class PointArea(StatesGroup):
    waiting_location = State()
    waiting_area = State()

class Cadnum(StatesGroup):
    waiting_text = State()

class Comps(StatesGroup):
    collecting = State()