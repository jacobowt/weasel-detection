import pm4py
import pandas as pd
import numpy as np
import os

# The calculation of the time taken for each event gets defined as a function
def calculate_time_taken(df):

    # Takes a dataframe representing the event log and calculates the duration of each event within a case.
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    df = df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # Adds a duration column to the dataframe and set duration to NaN for "start" events
    df['duration'] = (df['time:timestamp'] - df.groupby(['case:concept:name', 'concept:name'])['time:timestamp'].shift()).dt.total_seconds()
    df['duration'] = df['duration'].where(df['lifecycle:transition'] != 'start', np.nan)
    
    # Returns the dataframe with the new "duration" attribute
    return df

# The algorithm for the detection of Performance Masking gets defined as a function
def detect_performance_masking(log, threshold_events, threshold_occurrences, threshold_time):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # The duration calculation function gets called with the log converted to a dataframe to add a duration column
    df = calculate_time_taken(df)

    # The average number of events per case and the average occurrences of each activity in a case are calculated and stored
    avg_num_events = df.groupby('case:concept:name')['concept:name'].size().mean()
    avg_occurrences = df.groupby(['case:concept:name', 'concept:name']).size().groupby('concept:name').mean()

    # A set for cases that contain significantly many events is initialized, empty at first
    cases_with_many_events = set()

    # It is iterated through each case
    for case in df['case:concept:name'].unique():

        # The number of events occurring in the regarded case is calculated and stored
        num_events = df[df['case:concept:name'] == case].shape[0]

        # The threshold for a significant number of events in a case gets calculated by multiplying the given threshold factor with the average number of events in the log
        threshold_1 = int(avg_num_events * threshold_events)

        # If the number of events in the case is greater than the average number of events in a case to a significant degree, the case is added to the cases_with_many_events set
        if num_events - avg_num_events > threshold_1:
            cases_with_many_events.add(case)

    # A set for activities that occurred significantly often within one of the filtered cases is initialized, empty at first
    activities_that_occurred_often = set()

    # It is iterated through each case with significantly many events
    for case in cases_with_many_events:

        # It is iterated through each different activity in the case
        for activity in df[df['case:concept:name'] == case]['concept:name'].unique():

            # The number of times the regarded activity occurred in the regarded case is calculated and stored
            num_occurrences = df[(df['case:concept:name'] == case) & (df['concept:name'] == activity)].shape[0]

            # The threshold for significant occurrences gets calculated by multiplying the given threshold factor with the average occurrences of the activity
            threshold_2 = int(avg_occurrences[activity] * (threshold_occurrences))

            # If the number of occurrences in the case is greater than the average number of occurrences in a case to a significant degree, the activity is added to the activities_that_occurred_often set
            if num_occurrences - avg_occurrences[activity] > threshold_2:
                activities_that_occurred_often.add((case, activity))

    # It is iterated through activities that occurred often and their allocated cases
    for case, activity in activities_that_occurred_often:

        # The durations allocated to the event are extracted
        durations = df[(df['case:concept:name'] == case) & (df['concept:name'] == activity)]['duration'].values

        # It is iterated through these durations
        for i, duration in enumerate(durations):

            # Condition for Performance Masking, if an activity is performed in a significantly short amount of time while also occurring very often in the same case
            if not pd.isna(duration) and duration < threshold_time:
                resource = df[(df['case:concept:name'] == case) & (df['concept:name'] == activity)]['org:resource'].iloc[i]
                print(f"Possible Performance Masking detected, activity {activity}, performed by resource {resource}, occurred significantly often in a significantly short amount of time ({duration:.2f} seconds) in case {case}")


                # The results get stored in the results list
                results_entry = {
                    'Resource': resource,
                    'Activity': activity,
                    'Case': case,
                    'Explanation': 'The activity occurred significantly often in a significantly short amount of time'
                }
                results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\5_performancemasking.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\5_performancemasking_results.csv"

# Set a dynamic threshold for when the number of events in a case is considered significantly high, the threshold gets scaled based on the average number of events in a case. A value of e.g. 0.5 means the number of events in the regarded case has to be greater than the average number of events in a case by a factor of 50% of said average to be considered significant, so if a case in the log contains 10 events on average, the regarded case has to contain more than 10 + (10 * 0,5) = 10 + 5 = 15 times to be considered significant. Similarly, with e.g. a threshold set to 1, it would have to be more than 10 + (10 * 1) = 10 + 10 = 20 events
threshold_events = 0.5  # Adjust as needed

# Set a dynamic threshold for when the amount of times an activity occurs in a case is considered significantly greater than the average times this activity occurs in a case, the threshold gets scaled based on the average occurrences of an activity. A value of e.g. 0.5 means the amount of times the regarded activity occurs in the regarded case has to be greater than the average occurrences of this activity by a factor of 50% of said average to be considered significant, so if an activity occurs 4 times on average in a case, it has to occur more than 4 + (4 * 0,5) = 4 + 2 = 6 times to be considered significant. Similarly, with e.g. a threshold set to 1, it would have to be more than 4 + (4 * 1) = 4 + 4 = 8 times
threshold_occurrences = 0.5

# Set a threshold for significant short amounts of time the regarded events shall not exceed to get flagged. The time is measured in seconds, so a threshold of 120 means 120 seconds or 2 minutes
threshold_time = 120

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the specified log and thresholds
detect_performance_masking(log, threshold_events, threshold_occurrences, threshold_time)
