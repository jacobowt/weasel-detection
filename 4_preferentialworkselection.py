import pm4py
import pandas as pd
import os

# The algorithm for the detection of the first condition of Preferential Work Selection, resources selecting certain activities more or less often than expected, gets defined as a function
def detect_preferential_work_selection_average(log, threshold_factor):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # A dictionary is initialized for the total frequency of each activity being performed by each resource
    activity_frequency = {resource: {} for resource in df['org:resource'].unique()}

    # A dictionary is initialized for the total frequency of each activity being performed by all resources in total, empty at first
    total_frequency = {}

    # It is iterated through each event
    for index, row in df.iterrows():

        # The activity and its assigned resource are retrieved
        activity = row['concept:name']
        resource = row['org:resource']

        # The activity_frequency dictionary gets a 2D entry for each activity being performed by each resource
        if resource not in activity_frequency:
            activity_frequency[resource] = {}
        if activity not in activity_frequency[resource]:
            activity_frequency[resource][activity] = 0

        # The count of how often the regarded activity was performed by the regarded resource gets incremented
        activity_frequency[resource][activity] += 1

    # It is iterated through each resource and activity
    for resource, activities in activity_frequency.items():
        for activity, count in activities.items():

            # The count of how often the regarded activity was performed by all resources in total gets calculated by adding the individual counts
            if activity not in total_frequency:
                total_frequency[activity] = 0
            total_frequency[activity] += count

    # The average frequency of each activity being performed gets calculated and stored in a newly initialized dictionary
    average_frequency = {activity: total_frequency[activity] / len(activity_frequency) for activity in total_frequency}

    # The threshold gets calculated by multiplying the given threshold factor with the amount of resources present in the log
    threshold = threshold_factor * len(df['org:resource'].unique())

    # It is iterated through each resource and activity
    for resource, activities in activity_frequency.items():
        for activity, count in activities.items():

            # The deviation from the average frequency is calculated by subtracting the average frequency of an activity being performed of the indivual frequency
            deviation = count - average_frequency[activity]

            # Checks if the deviation is significant
            if abs(deviation) > threshold:

                # First condition for Preferential Work Selection, if a resource performed an activity significantly more than expected
                if deviation > 0:
                    print(f"Possible Preferential Work Selection detected, resource {resource} has performed activity {activity} {count} times, while it was performed {average_frequency[activity]} times on average. That is significantly more than expected.")

                    # The results get stored in the results list
                    results_entry = {
                        'Resource': resource,
                        'Activity': activity,
                        'Case': '',
                        'Resource Frequency': count,
                        'Average Frequency': average_frequency[activity],
                        'Explanation': 'The resource conducted the activity significantly more than expected'
                    }
                    results.append(results_entry)


    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# The algorithm for the detection of the second condition of Preferential Work Selection, resources starting a new activity despite not having completed another one (therefore not following First Come First Served),  gets defined as a function
def detect_preferential_work_selection_fcfs(log):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # It is iterated through each time an activity is started in the log
    for _, start_activity in df[df['lifecycle:transition'] == 'start'].iterrows():

        # The start timestamp is retrieved
        start_timestamp = pd.to_datetime(start_activity['time:timestamp'])

        # Find the first valid complete timestamp for the corresponding activity
        complete_timestamp = pd.to_datetime(df[(df['case:concept:name'] == start_activity['case:concept:name']) & (df['org:resource'] == start_activity['org:resource']) & (df['lifecycle:transition'] == 'complete') & (df['concept:name'] == start_activity['concept:name'])]['time:timestamp'].min())

        # After a valid complete timestamp was found, it is searched for other activities being started between the two previously retrieved (and related) timestamps by the same resource
        if pd.notna(complete_timestamp):
            for _, new_start_activity in df[(df['lifecycle:transition'] == 'start') & (df['org:resource'] == start_activity['org:resource']) & (df['case:concept:name'] != start_activity['case:concept:name']) & (df['time:timestamp'] > start_timestamp) & (df['time:timestamp'] < complete_timestamp)].iterrows():

                # Second condition for Preferential Work Selection, if a resource started a new activity while not having completed another activity in another case
                print(f"Possible Preferential Work Selection detected, resource {start_activity['org:resource']} started activity {new_start_activity['concept:name']} in case {new_start_activity['case:concept:name']} at {new_start_activity['time:timestamp']} while still not having completed activity {start_activity['concept:name']} in case {start_activity['case:concept:name']} at {complete_timestamp}")

                # The results get stored in the results list
                results_entry = {
                    'Resource': start_activity['org:resource'],
                    'Activity': new_start_activity['concept:name'],
                    'Case': new_start_activity['case:concept:name'],
                    'Resource Frequency': '',
                    'Average Frequency': '',
                    'Explanation': 'The resource started the activity while still being involved in another case'
                }
                results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\4_preferentialworkselection.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\4_preferentialworkselection_results.csv"

# Set a dynamic threshold for significant deviation from the average frequency of an activity being performed by a resource, the threshold gets scaled based on log size. A value of 0.5 means a deviation of 50% of the number of resources is considered significant, so if 3 resources would appear in the log, 0.5 would mean that the deviation from the average frequency of an activity being performed by a resource must be greater than 1.5 to be considered significant
threshold_factor = 0.5

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The functions are called with the specified log and threshold
detect_preferential_work_selection_average(log, threshold_factor)
detect_preferential_work_selection_fcfs(log)
