from sqlalchemy import func, text

import logging

logger = logging.getLogger(__name__)


def hours_to_dates(min_hours_life=0, max_hours_life=0):
    """get dates interval (min and max dates) from min and max hours of life"""

    min_days = min_hours_life // 24
    min_hours = min_hours_life % 24
    max_days = max_hours_life // 24
    max_hours = max_hours_life % 24
    max_interval = text(f"interval '{min_days} days {min_hours} hours'")
    min_interval = text(f"interval '{max_days} days {max_hours} hours'")
    if min_hours_life == 0:
        return func.now() - min_interval
    elif max_hours_life == 0:
        return func.now() - max_interval
    return func.now() - min_interval, func.now() - max_interval
