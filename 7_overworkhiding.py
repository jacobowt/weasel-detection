import pm4py
import pandas as pd
import os

# The algorithm for the detection of Overwork Hiding gets defined as a function
def detect_overwork_hiding(log):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # Two dictionaries are initialized for the working times of the resources, with the format 'resource':'time', empty at first
    work_starting_times = {}
    work_ending_times = {}

    # The start and ending of the working times allocated to specific resources can be defined here if needed, and will be saved in the dictionaries
    # Example: work_starting_times = {'Alice': pd.to_datetime('10:00:00').time(), 'Bob': pd.to_datetime('09:00:00').time()}
    #          work_ending_times = {'Alice': pd.to_datetime('18:00:00').time(), 'Bob': pd.to_datetime('17:00:00').time()}
    


    # It is iterated through each case with all events in it
    for case_name, case_df in df.groupby('case:concept:name'):
        for index, row in case_df.iterrows():

            # The resource and timestamp associated with the regarded event are retrieved
            resource = row['org:resource']
            event_timestamp = pd.to_datetime(row['time:timestamp'])

            # Default working times for resources without specified working times are set here
            if resource not in work_starting_times:
                work_starting_times[resource] = pd.to_datetime('09:00:00').time()
            if resource not in work_ending_times:
                work_ending_times[resource] = pd.to_datetime('17:00:00').time()

            # First condition for Overwork Hiding, if an event is performed before the official working time started
            if event_timestamp.time() < work_starting_times[resource]:
                print(f"Possible Overwork Hiding detected, resource {resource} performed activity {row['concept:name']} from Case {case_name} at {event_timestamp.time()}, before the start of allocated working time ({work_starting_times[resource]}).")

                # The results get stored in the results list
                results_entry = {
                    'Resource': resource,
                    'Activity': row['concept:name'],
                    'Case': case_name,
                    'Timestamp': event_timestamp.time(),
                    'Work Starting Time': work_starting_times[resource],
                    'Work Ending Time': work_ending_times[resource],
                    'Explanation': 'The resource conducted the listed activity before the start of official working time'
                }
                results.append(results_entry)

            # Second condition for Overwork Hiding, if an event is performed after the official working time ended
            if event_timestamp.time() > work_ending_times[resource]:
                print(f"Possible Overwork Hiding detected, resource {resource} performed activity {row['concept:name']} from Case {case_name} at {event_timestamp.time()}, after the ending of allocated working time ({work_ending_times[resource]}).")

                # The results get stored in the results list
                results_entry = {
                    'Detected Weasel Pattern': 'Overwork Hiding',
                    'Resource': resource,
                    'Activity': row['concept:name'],
                    'Case': case_name,
                    'Timestamp': event_timestamp.time(),
                    'Work Starting Time': work_starting_times[resource],
                    'Work Ending Time': work_ending_times[resource],
                    'Explanation': 'The resource conducted the listed activity after the ending of official working time'
                }
                results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\7_overworkhiding.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\7_overworkhiding_results.csv"

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the specified log
detect_overwork_hiding(log)
