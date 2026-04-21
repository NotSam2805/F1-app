def ask_question(question: str, answers: list):
    answer = None
    while answer not in answers:
        answer = input(question)
    return answer


import fastf1 as f1
from driver_data import build_dataframe_all_events, build_session_dataframe

CSV_PATH = './CSVs'

if __name__ == "__main__":
    year = int(input('Year: '))
    schedule = f1.get_event_schedule(year)

    df = None

    if ask_question('All events?', ['y','n']) == 'y':
        df,_ = build_dataframe_all_events(schedule)
    else:
        round = int(input(f'Round (1 - {len(schedule['RoundNum']) - 1}): '))
        event = schedule.get_event_by_round(round)
        df,_ = build_session_dataframe(event, year, round)
    
    df.to_csv(f'{CSV_PATH}/Y{year}.csv')