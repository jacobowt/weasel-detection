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

# The algorithm for the detection of Social Borrowing gets defined as a function
def detect_social_borrowing(log, threshold):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # The duration calculation function gets called with the log converted to a dataframe to add a duration column
    df = calculate_time_taken(df)

    # Two dictionaries are initialized for the working times of the resources, with the format 'resource':'time', empty at first
    work_starting_times = {}
    work_ending_times = {}

    # The start and ending of the working times allocated to specific resources can be defined here if needed, and will be saved in the dictionaries
    # Example: work_starting_times = {'Alice': pd.to_datetime('10:00:00').time(), 'Bob': pd.to_datetime('09:00:00').time()}
    #          work_ending_times = {'Alice': pd.to_datetime('18:00:00').time(), 'Bob': pd.to_datetime('17:00:00').time()}
    work_starting_times = {'Alice': pd.to_datetime('09:00:00').time(), 'Bob': pd.to_datetime('12:00:00').time()}
    work_ending_times = {'Alice': pd.to_datetime('17:00:00').time(), 'Bob': pd.to_datetime('19:00:00').time()}

    # Default working times for resources without specified working times are set here
    for resource in df['org:resource'].unique():
        if resource not in work_starting_times:
            work_starting_times[resource] = pd.to_datetime('09:00:00').time()
        if resource not in work_ending_times:
            work_ending_times[resource] = pd.to_datetime('17:00:00').time()

    # It is iterated through each pair of resources
    for resource1 in work_starting_times:
        for resource2 in work_starting_times:
            if resource1 != resource2:

                # The time interval where the working times of the two resources overlap is calculated and stored
                overlap_start = max(work_starting_times[resource1], work_starting_times[resource2])
                overlap_end = min(work_ending_times[resource1], work_ending_times[resource2])

                # If the working times don't overlap at all, the pair gets skipped
                if (work_starting_times[resource1] > work_ending_times[resource2]) or (work_starting_times[resource2] > work_ending_times[resource1]):
                    pass

                # Otherwise it is continued
                elif overlap_start < overlap_end:

                    # The subset of the dataframe for the pair of resources during the overlapping work times is retrieved
                    pair_df = df[(df['org:resource'].isin([resource1, resource2])) &
                                 (df['time:timestamp'].dt.time >= overlap_start) &
                                 (df['time:timestamp'].dt.time <= overlap_end)]

                    # The subset of the dataframe for each resource alone is retrieved
                    alone_df_resource1 = df[(df['org:resource'] == resource1) &
                                            ((df['time:timestamp'].dt.time < overlap_start) | (df['time:timestamp'].dt.time > overlap_end))]
                    alone_df_resource2 = df[(df['org:resource'] == resource2) &
                                            ((df['time:timestamp'].dt.time < overlap_start) | (df['time:timestamp'].dt.time > overlap_end))]

                    # The average duration each of the resources needs for tasks while the working times overlap and while working alone are calculated and stored
                    mean_duration_resource1_alone = alone_df_resource1['duration'].mean()
                    mean_duration_resource2_alone = alone_df_resource2['duration'].mean()
                    mean_duration_resource1_overlap = pair_df[pair_df['org:resource'] == resource1]['duration'].mean()
                    mean_duration_resource2_overlap = pair_df[pair_df['org:resource'] == resource2]['duration'].mean()

                    # Condition for Social Borrowing, if the performance of a resource seems to correlate with the working times of another resource
                    if (mean_duration_resource1_overlap < (mean_duration_resource1_alone * threshold)) and (mean_duration_resource2_overlap >= mean_duration_resource2_alone):
                        print(f"Possible Social Borrowing detected, {resource2} is a possible victim of {resource1}")

                        # The results get stored in the results list
                        results_entry = {
                            'Initiating Resource': resource,
                            'Victim Resource': resource,
                            'Initiating Resource Average Time (Alone)': mean_duration_resource1_alone,
                            'Initiating Resource Average Time (Overlap)': mean_duration_resource1_overlap,
                            'Victim Resource Average Time (Alone)': mean_duration_resource2_alone,
                            'Victim Resource Average Time (Overlap)': mean_duration_resource2_overlap,
                            'Explanation': 'The potentially borrowing resource needs significantly more time for tasks made while the other resource is not present'
                        }
                        results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\13_socialborrowing.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\13_socialborrowing_results.csv"

# Set a dynamic threshold for significant difference between the duration for tasks the "borrowing" resource needs while working alone or during the working times of the "victim" resource, the threshold gets scaled based on the average duration of tasks performed by the potential borrower working alone. A value of e.g. 0.5 means the average duration of tasks performed by the borrower during the overlapping working times has to be less than the average duration of tasks performed by the potential borrower working alone by a factor of 50% of said average to be considered significant, so if the average duration of tasks performed by the borrower during the overlapping working times would be 1500 seconds, the average duration of the same resource working alone would have to be greater than 3000 * 0,5 = 1500 seconds to be considered significant. Similarly, with e.g. a threshold set to 1, it would have to be greater than 3000 * 1 = 3000 seconds
threshold = 0.5

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the specified log and threshold
detect_social_borrowing(log, threshold)
