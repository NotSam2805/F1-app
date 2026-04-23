def ask_question(question: str, answers: list):
    answer = None
    while answer not in answers:
        answer = input(question)
    return answer


import fastf1 as f1
from fastf1 import events, core
import numpy as np
import pandas as pd
import os.path

#f1.Cache.clear_cache()

PICKLE_PATH = './/pickles'

#DO NOT USE
def build_driver_session_data(event: events.Event) -> dict:
    """
    DO NOT USE
    Build a nested dictionary of driver results for an F1 event.

    Structure:
    {
        "16": {
            "FP1": {...},
            "FP2": {...},
            "FP3": {...},
            "Q": {...},
            "R": {...}
        },
        ...
    }

    Keys:
        outer key  -> DriverNumber
        inner key  -> Session name
        value      -> Row data from session.results as a dictionary
    """

    session_getters = [
        ("FP1", event.get_practice, 1),
        ("FP2", event.get_practice, 2),
        ("FP3", event.get_practice, 3),
        ("Q", event.get_qualifying, None),
        ("R", event.get_race, None)
    ]

    driver_data = {}

    for session_name, getter, arg in session_getters:
        try:
            session = getter(arg) if arg is not None else getter()
            session.load()
            results = session.results

            if results is None or results.empty:
                continue

            for _, row in results.iterrows():
                driver_number = str(row["DriverNumber"])

                if driver_number not in driver_data:
                    driver_data[driver_number] = {}

                driver_data[driver_number][session_name] = row.to_dict()

        except Exception as e:
            print(f"Skipping {session_name}: {e}")

    return driver_data

def build_session_dataframe(event: events.Event, year: int, round_num: int) -> pd.DataFrame:
    """
    Build a dataframe for all sessions in an event
    """

    if os.path.isfile(f'{PICKLE_PATH}/Y{year}_R{round_num}.pkl'):
        print(f'Using saved pickle for year: {year}, round: {round_num}')
        return pd.read_pickle(f'{PICKLE_PATH}/Y{year}_R{round_num}.pkl'), []

    session_getters = [
        ("FP1", event.get_practice, 1),
        ("FP2", event.get_practice, 2),
        ("FP3", event.get_practice, 3),
        ("Q", event.get_qualifying, None),
        ("R", event.get_race, None)
    ]

    frames = []

    skipped_sessions = []

    if event.is_testing():
        try:
            session = event.get_session(0)

            temp = session.results.copy()
            temp["EventName"] = event["EventName"]
            temp["SessionName"] = session_name
            temp["Year"] = year
            temp["Round"] = round_num

            frames.append(temp)
        except Exception as e:
            print(f'Skipping testing session {event.get_session_name(0)}')
            skipped_sessions.append((year, round_num, '', e))
        #Set session getters to empty, there is no need to loop through them
        session_getters = []

    for session_name, getter, arg in session_getters:
        try:
            session = getter(arg) if arg is not None else getter()
            session.load()

            results = session.results.copy()

            temp = results
            temp["EventName"] = event["EventName"]
            temp["SessionName"] = session_name
            temp["Year"] = year
            temp["Round"] = round_num

            frames.append(temp)

        except Exception as e:
            print(f"Skipping {session_name}: {e}")
            skipped_sessions.append((year, round_num, session_name, e))

    if not frames:
        return pd.DataFrame(), skipped_sessions

    df = pd.concat(frames, ignore_index=True)

    if "DriverNumber" in df.columns:
        df["DriverNumber"] = df["DriverNumber"].astype(str)

    if "Position" in df.columns:
        df = df.sort_values(by=["SessionName", "Position"], na_position="last")

    df = df.reset_index(drop=True)
    pd.to_pickle(df, f'{PICKLE_PATH}/Y{year}_R{round_num}.pkl')

    print(f'Skipped sessions: {skipped_sessions}')

    return df, skipped_sessions

def build_dataframe_all_events(schedule: events.EventSchedule):
    """
    Builds a wide dataframe for all events in a schedule
    """
    if os.path.isfile(f'{PICKLE_PATH}/Y{schedule.year}.pkl'):
        print(f'Using saved pickle for year: {schedule.year}')
        return pd.read_pickle(f'{PICKLE_PATH}/Y{schedule.year}.pkl'), []

    n_rounds = len(schedule['RoundNumber']) - 1

    frames = []

    skipped_sessions = []

    for round in range(1, n_rounds):
        event = schedule.get_event_by_round(round)
        frame, skipped = build_session_dataframe(event, schedule.year, round)
        frames.append(frame)
        skipped_sessions += skipped
    
    if not frames:
        return pd.DataFrame(), skipped_sessions
    
    df = pd.concat(frames, ignore_index=True)

    if "DriverNumber" in df.columns:
        df["DriverNumber"] = df["DriverNumber"].astype(str)

    if "Position" in df.columns:
        df = df.sort_values(by=["SessionName", "Position"], na_position="last")

    df = df.reset_index(drop=True)
    pd.to_pickle(df, f'{PICKLE_PATH}/Y{schedule.year}.pkl')

    #print(f'Skipped sessions: {skipped_sessions}')

    return df, skipped_sessions

def massive_dataframe(years: list[int]) -> pd.DataFrame:
    frames = []
    skipped_sessions = []
    for year in years:
        schedule = f1.get_event_schedule(year)
        frame, skipped = build_dataframe_all_events(schedule)
        frames.append(frame)
        skipped_sessions += skipped

    print_skipped(skipped_sessions)
    
    df = pd.concat(frames, ignore_index=True)
    return df.reset_index(drop=True)

def print_skipped(skipped_sessions):
    print()
    print('Skipped sessions:')
    for year, round, name, reason in skipped_sessions:
        print(f'Year: {year}, Round: {round}, Session: {name} | {reason}')
    print()

def collect_dataframes():
    year = int(input('Year: '))
    schedule = f1.get_event_schedule(year)
    print(f'There are {len(schedule['RoundNumber'])} rounds')

    df = None
    skipped = None

    if ask_question('Build dataframe for all events?', ['y','n']) == 'y':
        df,skipped = build_dataframe_all_events(schedule)
    else:
        round = int(input(f'Round (1-{len(schedule["RoundNumber"]) - 1}): '))
        event = schedule.get_event_by_round(round)
        print(event['EventName'])

        df,skipped = build_session_dataframe(event, year, round)
    
    print_skipped(skipped)

    return df

def add_avg_race_finish(dataframe: pd.DataFrame, n_races: int) -> pd.DataFrame:
    dataframe = dataframe.sort_values(['Year', 'Round'])
    df = dataframe.copy()
    df = df[['DriverNumber', 'SessionName', 'Year', 'Round','ClassifiedPosition']]

    averages = []

    for index, row in df.iterrows():
        if row['SessionName'] == 'R':
            driver = row['DriverNumber']
            round_n = row['Round']
            year = row['Year']
            
            finish_sum = 0
            count = 0
            start = max(round_n - n_races, 1)
            for r in range(start, round_n):
                this_round = df[(df['Round'] == r) & (df['DriverNumber'] == driver) & (df['Year'] == year) & (df['SessionName'] == 'R')]

                try:
                    finish_sum += int(this_round['ClassifiedPosition'].iloc[0])
                    count += 1
                except Exception as e:
                    # Large value for DNF
                    finish_sum += 25
                    count += 1
            
            if (count == 0):
                averages.append(None)
            else:
                avg_position = float(finish_sum)/float(count)
                averages.append(avg_position)
        else:
            averages.append(None)

    dataframe.insert(loc=12, column=f'Last{n_races}AverageFinish', value=averages)

    return dataframe

def add_avg_quali_finish(dataframe: pd.DataFrame, n_qualis: int) -> pd.DataFrame:
    dataframe = dataframe.sort_values(['Year', 'Round'])
    df = dataframe.copy()
    df = df[['DriverNumber', 'SessionName', 'Year', 'Round','Position']]

    averages = []

    for index, row in df.iterrows():
        if row['SessionName'] == 'Q':
            driver = row['DriverNumber']
            round_n = row['Round']
            year = row['Year']
            
            finish_sum = 0
            count = 0
            start = max(round_n - n_qualis, 1)
            for r in range(start, round_n):
                this_round = df[(df['Round'] == r) & (df['DriverNumber'] == driver) & (df['Year'] == year) & (df['SessionName'] == 'Q')]

                try:
                    finish_sum += int(this_round['Position'].iloc[0])
                    count += 1
                except Exception as e:
                    # Large value for DNF
                    finish_sum += 25
                    count += 1
            
            if (count == 0):
                averages.append(None)
            else:
                avg_position = float(finish_sum)/float(count)
                averages.append(avg_position)
        else:
            averages.append(None)

    dataframe.insert(loc=12, column=f'Last{n_qualis}QualifyingAverageFinish', value=averages)

    return dataframe

def add_best_lap(dataframe: pd.DataFrame) -> pd.DataFrame:
    df = dataframe[['Year', 'Round', 'DriverNumber', 'SessionName']]

    loaded_sessions = {}

    data = []

    for index, row in df.iterrows():
        key = (row['Year'], row['Round'], row['SessionName'])
        session = None
        if key in loaded_sessions.keys():
            session = loaded_sessions[key]
        else:
            session = f1.get_session(row['Year'], row['Round'], row['SessionName'])
            session.load()
            loaded_sessions[key] = session
        driver_info = session.laps.pick_drivers(row['DriverNumber'])
        best_lap = driver_info.pick_fastest()
        data.append(best_lap)
    
    dataframe.insert(loc=12, column=f'FastestLap', value=data)

    return dataframe

if __name__ == '__main__':
    big_df = massive_dataframe([2021])
    big_df = add_avg_race_finish(big_df, 5)
    big_df = add_avg_quali_finish(big_df, 5)
    big_df = add_best_lap(big_df)

    big_df.to_csv('./CSVs/All_2021.csv')