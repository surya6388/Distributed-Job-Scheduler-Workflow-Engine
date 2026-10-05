"""
Hand-Built Standard 5-Field Cron Expression Parser.

Fields: minute (0-59), hour (0-23), day_of_month (1-31), month (1-12), day_of_week (0-6)
Supports: '*', '*/N', 'N', 'N-M', 'N,M'

Time Complexity:
- compute_next_run: O(1) bounded scan over calendar minutes
Space Complexity:
- O(1)
"""

import datetime
from typing import Set

class CronParser:
    """Parses standard 5-field cron expressions and computes future trigger timestamps."""
    def __init__(self, expression: str):
        self.expression: str = expression.strip()
        parts = self.expression.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression '{expression}'. Expected 5 fields.")

        self.minutes: Set[int] = self._parse_field(parts[0], 0, 59)
        self.hours: Set[int] = self._parse_field(parts[1], 0, 23)
        self.days_of_month: Set[int] = self._parse_field(parts[2], 1, 31)
        self.months: Set[int] = self._parse_field(parts[3], 1, 12)
        self.days_of_week: Set[int] = self._parse_field(parts[4], 0, 6)

    def _parse_field(self, field: str, min_val: int, max_val: int) -> Set[int]:
        res: Set[int] = set()
        if field == '*':
            return set(range(min_val, max_val + 1))

        for part in field.split(','):
            if '/' in part:
                sub_parts = part.split('/')
                step = int(sub_parts[1])
                start_range = sub_parts[0]
                if start_range == '*':
                    range_min, range_max = min_val, max_val
                elif '-' in start_range:
                    r_split = start_range.split('-')
                    range_min, range_max = int(r_split[0]), int(r_split[1])
                else:
                    range_min, range_max = int(start_range), max_val

                res.update(range(range_min, range_max + 1, step))
            elif '-' in part:
                r_split = part.split('-')
                res.update(range(int(r_split[0]), int(r_split[1]) + 1))
            else:
                res.add(int(part))

        return {val for val in res if min_val <= val <= max_val}

    def compute_next_run(self, from_dt: datetime.datetime) -> datetime.datetime:
        """Find the next matching datetime strictly after from_dt."""
        dt = from_dt.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
        max_search_minutes = 366 * 24 * 60  # Scan up to 1 year ahead max

        for _ in range(max_search_minutes):
            if dt.month not in self.months:
                # Advance to next month
                dt = (dt.replace(day=1, hour=0, minute=0) + datetime.timedelta(days=32)).replace(day=1)
                continue
            if dt.day not in self.days_of_month:
                dt = (dt + datetime.timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if dt.weekday() not in self.days_of_week:
                dt = (dt + datetime.timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if dt.hour not in self.hours:
                dt = (dt + datetime.timedelta(hours=1)).replace(minute=0)
                continue
            if dt.minute in self.minutes:
                return dt
            dt += datetime.timedelta(minutes=1)

        raise RuntimeError(f"Could not compute next run time for expression '{self.expression}'.")
