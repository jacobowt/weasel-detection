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

# The algorithm for the detection of the first condition of Idling, resources taking more time to perform certain activities than other resources need for the same activities,  gets defined as a function
def detect_idling_resource(log, threshold):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)
    
    # Takes a dataframe representing the event log and calculates the duration of each event within a case.
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    df = df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # The duration calculation function gets called with the log converted to a dataframe to add a duration column
    df = calculate_time_taken(df)

    # Two dictionaries are initialized for the time taken by each resource for each activity and for the count of how often each activity was performed by each resource, empty at first
    completion_times = {}
    activity_count = {}

    # It is iterated through each event
    for index, row in df.iterrows():

        # The activity, its assigned resource and time taken are retrieved
        activity = row['concept:name']
        resource = row['org:resource']
        time_taken = row['duration']

        # null (nan) values get skipped, the start timestamps have those
        if pd.isnull(time_taken):
            continue

        # The two dictionaries get 2D entries for each activity performed by each resource
        if activity not in completion_times:
            completion_times[activity] = {}
            activity_count[activity] = {}
        if resource not in completion_times[activity]:
            completion_times[activity][resource] = 0
            activity_count[activity][resource] = 0

        # The time taken by the regarded resource for the regarded activity gets stored in the dictionary entry
        completion_times[activity][resource] += time_taken

        # The count of how often the regarded activity was performed by the regarded resource gets incremented
        activity_count[activity][resource] += 1

    # Completion times for all resources combined get stored for each activity
    total_completion_times = {activity: sum(completion_times[activity].values()) for activity in completion_times}

    # It is iterated through each activity and resource
    for activity in completion_times:
        for resource in completion_times[activity]:

            # The average time the regarded resource needs for the regarded activity gets calculated and stored
            average_time = completion_times[activity][resource] / activity_count[activity][resource]
            
            # Checks if the regarded activity has a recorded total completion time
            if activity in total_completion_times:

                # First condition for Idling, if the resource needs significantly more time on average for a certain activity than other resources need for the same activities
                if average_time - total_completion_times[activity] / sum(activity_count[activity].values()) > threshold:
                    print(f"Possible Idling detected, resource {resource} takes significantly more time ({average_time}) for activity {activity} compared to the average time of all resources ({total_completion_times[activity] / sum(activity_count[activity].values())}) for this activity.")

                    # The results get stored in the results list
                    results_entry = {
                        'Resource': resource,
                        'Activity': activity,
                        'Case': '',
                        'Resource Average Time': average_time,
                        'Total Average Time': total_completion_times[activity] / sum(activity_count[activity].values()),
                        'Explanation': 'The resource takes significantly more time for the activity compared to the average time of all resources for this activity'
                    }
                    results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")


# The algorithm for the detection of the second condition of Idling, resources taking more time to perform certain activities than they need for other activities,  gets defined as a function
def detect_idling_activity(log, threshold):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)
    
    # Takes a dataframe representing the event log and calculates the duration of each event within a case.
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    df = df.sort_values(['case:concept:name', 'time:timestamp'])
    
    # The duration calculation function gets called with the log converted to a dataframe to add a duration column
    df = calculate_time_taken(df)

    # Two dictionaries are initialized for the time taken by each resource for each activity and for the count of how often each activity was performed by each resource, empty at first
    completion_times = {}
    activity_count = {}

    # It is iterated through each event
    for index, row in df.iterrows():

        # The activity, its assigned resource and time taken are retrieved
        activity = row['concept:name']
        resource = row['org:resource']
        time_taken = row['duration']

        # During debugging, it turned out that some time values ​​were null, those get skipped
        if pd.isnull(time_taken):
            continue

        # The two dictionaries get 2D entries for each activity performed by each resource
        if activity not in completion_times:
            completion_times[activity] = {}
            activity_count[activity] = {}
        if resource not in completion_times[activity]:
            completion_times[activity][resource] = 0
            activity_count[activity][resource] = 0

        # The time taken by the regarded resource for the regarded activity gets stored in the dictionary entry
        completion_times[activity][resource] += time_taken

        # The count of how often the regarded activity was performed by the regarded resource gets incremented
        activity_count[activity][resource] += 1

    # A dictionary is initialized for the average completion times of each resource, empty at first
    resource_average_times = {}

    # It is iterated through the dictionary containing the completion times for each resource and activity
    for activity in completion_times:
        for resource in completion_times[activity]:

            # The overall average completion time for each activity is calulcated and stored
            activity_average_times = completion_times[activity][resource] / activity_count[activity][resource]
        
            # The overall average completion time for each resource is calculated and stored
            if resource not in resource_average_times:
                resource_average_times[resource] = []
            resource_average_times[resource].append(activity_average_times)

    # The average of average times is calculated for each resource
    average_of_average_times = {}
    for resource, times in resource_average_times.items():
        average_of_average_times[resource] = sum(times) / len(times)

    # It is iterated through the dictionary containing the completion times for each resource and activity
    for activity in completion_times:
        for resource in completion_times[activity]:

            # Check if the resource is present in average_of_average_times to make sure the resource actually performed the activity
            if resource in average_of_average_times:
                # The average times of the resource for the regarded activity and for all activities overall are retrieved
                average_time = completion_times[activity][resource] / activity_count[activity][resource]
                avg_of_avg_time = average_of_average_times[resource]

                # Second condition for Idling, if the resource needs significantly more time on average for a certain activity than the same resource needs for the average activity
                if average_time - avg_of_avg_time > threshold:
                    print(f"Possible Idling detected, resource {resource} takes significantly more time ({average_time}) for activity {activity} compared to its average time for all activities combined ({avg_of_avg_time}).")

                    # The results get stored in the results list
                    results_entry = {
                        'Resource': resource,
                        'Activity': activity,
                        'Case': '',
                        'Resource Average Time': average_time,
                        'Total Average Time': avg_of_avg_time,
                        'Explanation': 'The resource takes significantly more time for the activity compared to its average time of all activities combined'
                    }
                    results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")


# The algorithm for the detection of the third condition of Idling, resources taking long breaks,  gets defined as a function
def detect_idling_break(log, threshold):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # A dictionary is initialized for the last registered completion timestamp of a resource, empty at first
    last_complete_timestamp = {}
    
    # It is iterated through each case
    for case_name, case_df in df.groupby('case:concept:name'):

        # Within each case, the events get sorted based on their timestamp
        case_df = case_df.sort_values(by='time:timestamp')
        last_complete_timestamp = {}

        # It is iterated through each event in the regarded case
        for index, row in case_df.iterrows():

            # The resource and timestamp associated with the regarded event are retrieved
            resource = row['org:resource']
            event_timestamp = pd.to_datetime(row['time:timestamp'])

            # Stores the last registered completion timestamp
            if row['lifecycle:transition'] == 'complete':
                last_complete_timestamp[resource] = event_timestamp

            # Checks if the regarded timestamp marks the start of an activity, and if the regarded resource registered a completion timestamp before. If so, the time difference between those is calculated
            elif row['lifecycle:transition'] == 'start' and resource in last_complete_timestamp:
                if event_timestamp.date() == last_complete_timestamp[resource].date():
                    time_difference = (event_timestamp - last_complete_timestamp[resource]).total_seconds()

                    # Third condition for Idling, if a resource appears to have taken a disproportionately long break during working time
                    if time_difference > threshold:
                        print(f"Possible Idling detected, resource {resource} idled for {time_difference} seconds between activities {case_df.loc[case_df['time:timestamp'] == last_complete_timestamp[resource]]['concept:name'].values[0]} (registered at {last_complete_timestamp[resource]}) and {row['concept:name']} (registered at {event_timestamp}) in Case {case_name}.")

                        # The results get stored in the results list
                        results_entry = {
                            'Resource': resource,
                            'Activity': case_df.loc[case_df['time:timestamp'] == last_complete_timestamp[resource]]['concept:name'].values[0],
                            'Case': case_name,
                            'Resource Average Time': '',
                            'Total Average Time': '',
                            'Explanation': 'The resource took a significantly long break after the activity'
                        }
                        results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")


# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\9_idling.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\9_idling_results.csv"

# Set a threshold for significant deviation of the completion time for a certain resource compared with other resources performing the same activities. The time is measured in seconds, so a threshold of 300 means 300 seconds or 5 minutes
threshold_resource = 300

# Set a threshold for significant deviation of the completion time for a certain activity compared with other activities performed by the same resource. The time is measured in seconds, so a threshold of 600 means 600 seconds or 10 minutes
threshold_activity = 600

# Set a threshold for significantly long breaks, gets used in the function for the third condition. The time is measured in seconds, so a threshold of 14400 means 14400 seconds or 4 hours
threshold_break = 14400

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The functions are called with the specified log and thresholds
detect_idling_resource(log, threshold_resource)
detect_idling_activity(log, threshold_activity)
detect_idling_break(log, threshold_break)
