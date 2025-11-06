from datetime import timedelta
from typing import Set

def get_workdays_for_week(start_date, holidays_set: Set) -> int:
    """
    Calcula o número de dias úteis (Seg-Sex) para uma semana
    que começa em 'start_date', desconsiderando os feriados.
    """
    week_days = [start_date + timedelta(days=i) for i in range(5)]
    num_holidays_in_week = len(set(week_days).intersection(holidays_set))
    return 5 - num_holidays_in_week