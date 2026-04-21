import fastf1 as f1
import numpy as np

year = int(input('Year:'))
schedule = f1.get_event_schedule(year)
print()
print(schedule[['RoundNumber', 'Country', 'Location']])
print()

event = schedule.get_event_by_name(input('Location: '))

print()
print(event[['RoundNumber','Country','Location','EventName','OfficialEventName','EventFormat']])
print()

session = event.get_race()
session.load()