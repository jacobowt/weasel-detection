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
    
    # Convert lifecycle:transition values to lowercase for case insensitive comparison
    df['lifecycle:transition'] = df['lifecycle:transition'].str.lower()
    
    # Set duration to NaN for "start" events (case insensitive check)
    df['duration'] = df['duration'].where(df['lifecycle:transition'] != 'start', np.nan)
    
    # Returns the dataframe with the new "duration" attribute
    return df


# The algorithm for the detection of Performance Blow-out gets defined as a function
def detect_performance_blowout(log, threshold_in, threshold_sd):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # The duration calculation function gets called with the log converted to a dataframe to add a duration column
    df = calculate_time_taken(df)
    
    # Dictionary for the time taken by each resource for each activity gets initialized, empty at first
    completion_times = {}

    # Dictionary for booleans telling if a resource took increasingly more time for the same activity gets initialized, empty at first
    resource_slower = {}

    # It is iterated through each event
    for index, row in df.iterrows():

        # The activity, its assigned resource and time taken are retrieved
        activity = row['concept:name']
        resource = row['org:resource']
        time_taken = row['duration']

        # null (nan) values get skipped, the start timestamps have those
        if pd.isnull(time_taken):
            continue

        # The completion_times dictionary gets a 2D entry for each activity performed by each resource
        if activity not in completion_times:
            completion_times[activity] = {}
        if resource not in completion_times[activity]:
            completion_times[activity][resource] = []

        # The time taken by the regarded resource for the regarded activity gets stored in the dictionary entry
        completion_times[activity][resource].append(time_taken)

        # Here it gets checked if a resource needs increasingly more time for the same activity, this can only be checked if it was performed more than once
        if len(completion_times[activity][resource]) > 1:

            # The tame taken for the activity previously gets retrieved
            prev_time_taken = completion_times[activity][resource][-2]

            # The time taken gets compared with the time previously taken for the same activity, flagging affected resources and activities
            if time_taken > prev_time_taken + threshold_in:
                resource_slower[(activity, resource)] = {'slower': True, 'flagged_time': time_taken}

    # It is iterated through the dictionary containing instances where a resource got significantly slower performing the same activity
    for key, slower in resource_slower.items():
        activity, resource = key

        # First condition for Performance Blow-Out, if the activity took significantly longer to finish than previously
        print(f"Possible Performance Blow-out detected, resource {resource} shows increasing completion times over time for activity {activity}.")

        # The results get stored in the results list
        results_entry = {
            'Resource': resource,
            'Activity': activity,
            'Standard Deviation': '',
            'Explanation': 'The resource shows increasing completion times over time for the activity'
        }
        results.append(results_entry)

    # It is iterated through the dictionary containing the completion times for each activity
    for activity in completion_times:

        # Standard Deviation of mean completion times is calculated to evaluate whether resources take similar times for the same activities or not
        standard_deviation = np.std([np.mean(times) for times in completion_times[activity].values()])

        # Second condition for Performance Blow-Out, if different resources took significantly different times for the same activity
        if standard_deviation > threshold_sd:
            print(f"Possible Performance Blow-out detected, different resources took very different times for the activity {activity}. The standard deviation is {standard_deviation}.")

            # The results get stored in the results list
            results_entry = {
                'Resource': '',
                'Activity': activity,
                'Standard Deviation': standard_deviation,
                'Explanation': 'Different resources took very different times for the activity'
                }
            results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=True)
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\6_performanceblowout.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\6_performanceblowout_results.csv"

# Set a threshold for significant increase of completion time. The time is measured in seconds, so a threshold of 600 means 600 seconds or 10 minutes
threshold_in = 600 

# Set a threshold for significant standard deviation. The time is measured in seconds, so a threshold of 1800 means 1800 seconds or 30 minutes
threshold_sd = 1800

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the specified log and thresholds
detect_performance_blowout(log, threshold_in, threshold_sd)
