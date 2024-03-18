import pandas as pd
from itertools import combinations
import pm4py
import os

# Since the start and completion timestamps of each event are usually registered separate in the event logs and it is easier for the detection of groups to regard an event as one entry, they get merged in this function here
def merge_start_complete_timestamps(df):

    # The entries for the start and completion timestamps are retrieved
    start_events = df[df['lifecycle:transition'].str.lower() == 'start']
    complete_events = df[df['lifecycle:transition'].str.lower() == 'complete']
    
    # Those entries get merged if the other attributes are similar
    merged_events = pd.merge(start_events, complete_events, on=['org:resource', 'concept:name', 'case:concept:name'])
    
    # Renames the columns containing the timestamps to differentiate between them
    merged_events.rename(columns={'time:timestamp_x': 'startTimestamp', 'time:timestamp_y': 'completeTimestamp'}, inplace=True)

    # Returns the dataframe with the now merged entries for the two timestamps
    return merged_events[[ 'org:resource', 'concept:name', 'startTimestamp', 'completeTimestamp', 'case:concept:name']]

# An algorithm for the detection of groups in the log gets defined as a function
def add_group_work_flag(df):

    # Adds a Group Work Flag column to the dataframe and sets it to false per default
    df['GroupWorkFlag'] = False

    # The events are grouped by case
    grouped_by_case = df.groupby('case:concept:name')

    # It is itereted through each case
    for _, case_group in grouped_by_case:

        # It is itereted through each event in the case
        for i, current_event in case_group.iterrows():

            # It is itereted through each other event in the case
            for j, other_event in case_group.iterrows():

                # First condition for the detection of an event as group work: Two events from the same case overlap in time while also being associated with different resources
                if ((current_event['org:resource'] != other_event['org:resource']) and (current_event['startTimestamp'] <= other_event['completeTimestamp']) and (current_event['completeTimestamp'] >= other_event['startTimestamp'])):
                    df.at[i, 'GroupWorkFlag'] = True
                    df.at[j, 'GroupWorkFlag'] = True

                # Second condition for the detection of an event as group work: Two events with the same activity get registered in the same case while also being associated with different resources
                elif ((current_event['org:resource'] != other_event['org:resource']) and (current_event['concept:name'] == other_event['concept:name'])):
                    df.at[i, 'GroupWorkFlag'] = True
                    df.at[j, 'GroupWorkFlag'] = True

    # A dictionary is initialized for the count of events by each resource in each case, empty at first
    resource_event_count = {}

    # It is iterated through the dataframe to count the events associated with each resource in each case
    for i, row in df.iterrows():

        # The case and resource are retrieved
        case_id = row['case:concept:name']
        resource = row['org:resource']

        # The dictionary gets a 2D entry for the regarded case and the regarded resource
        if case_id not in resource_event_count:
            resource_event_count[case_id] = {}
        if resource not in resource_event_count[case_id]:
            resource_event_count[case_id][resource] = 0

        # The count of how often the regarded resource performed an event in the regarded case gets incremented
        resource_event_count[case_id][resource] += 1

    # It is iterated through each event
    for i, current_event in df.iterrows():

        # The case and resource are retrieved
        case_id = current_event['case:concept:name']
        resource = current_event['org:resource']

        # Third condition for the detection of an event as group work: Two different resources each have 10 or more events registered in the same case
        if case_id in resource_event_count and resource in resource_event_count[case_id] and resource_event_count[case_id][resource] > 9:
            other_resources = df[(df['case:concept:name'] == case_id) & (df['org:resource'] != resource)]['org:resource'].unique()
            for other_resource in other_resources:
                if other_resource in resource_event_count[case_id] and resource_event_count[case_id][other_resource] > 9:
                    df.loc[(df['case:concept:name'] == case_id) & ((df['org:resource'] == resource) | (df['org:resource'] == other_resource)), 'GroupWorkFlag'] = True

    # It is iterated through each case
    for _, case_group in df.groupby('case:concept:name'):

        # The case group is sorted by resource
        grouped_resources = case_group.groupby('org:resource')

        # A dictionary is initialized for time intervals, empty at first
        intervals = {}

        # It is iterated through all combinations of resource pairs
        for (res1, res1_events), (res2, res2_events) in combinations(grouped_resources, 2):

            # It is iterated through each event of the resource pair
            for _, res1_event in res1_events.iterrows():
                for _, res2_event in res2_events.iterrows():

                    # It is checked if the events are registered within a time interval of 10 minutes
                    if ((res1_event['completeTimestamp'] <= res2_event['completeTimestamp'] + pd.Timedelta(seconds=600) and res2_event['completeTimestamp'] <= res1_event['completeTimestamp'] + pd.Timedelta(seconds=600)) or (res1_event['startTimestamp'] <= res2_event['startTimestamp'] + pd.Timedelta(seconds=600) and res2_event['startTimestamp'] <= res1_event['startTimestamp'] + pd.Timedelta(seconds=600))):
  
                        # If so, the events are stored in the intervals dictionary
                        interval = (res1_event['completeTimestamp'], res2_event['completeTimestamp'])
                        intervals.setdefault(res1, set()).add(interval)
                        intervals.setdefault(res2, set()).add(interval)

        # It is iterated through the intervals dictionary
        for res, res_intervals in intervals.items():

            # Fourth condition for the detection of an event as group work: Two different resources each have an event registered in at least 3 different time intervals of 10 minutes
            if len(res_intervals) > 2:
                res_indices = case_group.index.intersection(df[df['org:resource'] == res].index).tolist()
                df.loc[res_indices, 'GroupWorkFlag'] = True

# The algorithm for the detection of Social Loafing gets defined as a function
def detect_social_loafing(log, threshold):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # It is iterated through each different resource
    for resource in log['org:resource'].unique():

        # Variables for total time spent during group work and individual time and for the count of the events categorized as one of those two are initialized
        total_group_work_time = 0
        total_individual_work_time = 0
        total_group_events = 0
        total_individual_events = 0

        # Two lists are initialized for the group events and individual events of the resources, empty at first
        group_events = []
        individual_events = []

        # It is iterated through each event of the regarded resource
        for i, event in log[log['org:resource'] == resource].iterrows():

            # The duration of the regarded event is calculated and stored
            duration = (event['completeTimestamp'] - event['startTimestamp']).total_seconds()

            # If the Group Work Flag is set to true, the duration is added to the total group work time and the total group events counter is incremented
            if event['GroupWorkFlag']:
                total_group_work_time += duration
                total_group_events += 1
                group_events.append((event['case:concept:name'], event['concept:name']))

            # If the Group Work Flag is set to false, the duration is added to the total individual work time and the total individual events counter is incremented
            else:
                total_individual_work_time += duration
                total_individual_events += 1
                individual_events.append((event['case:concept:name'], event['concept:name']))

        # Resources that have 0 registered events in either category get skipped
        if total_individual_events == 0:
            continue
        if total_group_events == 0:
            continue

        # The average times for the two categories get calculated and stored
        avg_group_time_per_event = total_group_work_time / total_group_events if total_group_events > 0 else 0
        avg_individual_time_per_event = total_individual_work_time / total_individual_events if total_individual_events > 0 else 0

        # Condition for Social Loafing, if a resource performs significantly better in individual work than in the context of group work
        if (avg_group_time_per_event - avg_individual_time_per_event) > threshold:
            print(f"Possible Social Loafing detected, resource {resource} performs significantly better in individual work than in the context of group work.")

            # The results get stored in the results list
            results_entry = {
                'Resource': resource,
                'Average Group Work Time': avg_group_time_per_event,
                'Average Individual Work Time': avg_individual_time_per_event,
                'Explanation': 'The resource performs significantly better in individual work than in the context of group work'
            }
            results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\10_socialloafing.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\10_socialloafing_results.csv"

# The event log is converted to a dataframe for easier data analysis
df = pm4py.convert_to_dataframe(log)

# Call the function to merge start and complete timestamps
df = merge_start_complete_timestamps(df)

# Call the function to add GroupWorkFlag
add_group_work_flag(df)

# Set a threshold for significant time difference between average group work time and average individual work time. The time is measured in seconds, so a threshold of 600 means 600 seconds or 10 minutes
threshold = 600

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the specified log and threshold
detect_social_loafing(df, threshold)
