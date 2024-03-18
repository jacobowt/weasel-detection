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

                # Second condition for the detection of an event as group work: Two events with the same name get registered in the same case while also being associated with different resources
                elif ((current_event['org:resource'] != other_event['org:resource']) and (current_event['concept:name'] == other_event['concept:name'])):
                    df.at[i, 'GroupWorkFlag'] = True
                    df.at[j, 'GroupWorkFlag'] = True

    # A dictionary is initialized for the count of events by each resource in each case, empty at first
    resource_event_count = {}

    # It is iterated through the dataframe to count the events by each resource in each case
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

# The algorithm for the detection of Boss Mobbing gets defined as a function
def detect_boss_mobbing(df, boss_takeover_timestamp, threshold):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The duration of the events calculated and a duration column gets added to the dataframe
    df['duration'] = (df['completeTimestamp'] - df['startTimestamp']).dt.total_seconds()

    # The boss_takeover_timestamp is converted to UTC to make it tz-aware
    boss_takeover_timestamp = boss_takeover_timestamp.tz_localize('UTC')

    # Group work events before and after the boss takeover timestamp are filtered and stored
    events_before = df[(df['completeTimestamp'] < boss_takeover_timestamp) & (df['GroupWorkFlag'] == True)]
    events_after = df[(df['completeTimestamp'] >= boss_takeover_timestamp) & (df['GroupWorkFlag'] == True)]

    # The average durations of those two event groups get calculated and stored
    avg_durations_before = events_before.groupby('org:resource')['duration'].mean()
    avg_durations_after = events_after.groupby('org:resource')['duration'].mean()

    # It is iterated through each resource
    for resource in df['org:resource'].unique():

        # The average durations before and after the boss takeover timestamp of the regarded resources are retrieved
        if resource in avg_durations_before and resource in avg_durations_after:
            avg_duration_before = avg_durations_before[resource]
            avg_duration_after = avg_durations_after[resource]

            # Condition for Boss Mobbing, if the average times groups need for work after a new boss took over are significantly higher than before
            if (avg_duration_after - avg_duration_before) > (avg_duration_before * threshold):
                print(f"Possible Boss Mobbing detected, resource {resource} has a significantly higher average event duration after the boss takeover.")

                # The results get stored in the results list
                results_entry = {
                    'Resource': resource,
                    'Average Event Duration Before Boss Takeover': avg_duration_before,
                    'Average Event Duration After Boss Takeover': avg_duration_after,
                    'Explanation': 'The resource has a significantly higher average event duration after the boss takeover'
                }
                results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\12_bossmobbing.xes"
log = pm4py.read_xes(log_path)

# Specify the timestamp where a new boss took over
boss_takeover_timestamp = pd.Timestamp("2023-08-01 12:00:00")

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\12_bossmobbing_results.csv"

# The event log is converted to a dataframe for easier data analysis
df = pm4py.convert_to_dataframe(log)

# Call the function to merge start and complete timestamps
df = merge_start_complete_timestamps(df)

# Call the function to add GroupWorkFlag
add_group_work_flag(df)

# Set a dynamic threshold for significant difference between the duration for tasks the resources working in groups need before and after the new boss took over, the threshold gets scaled based on the average duration before the boss takeover. A value of e.g. 0.4 means the average duration after the boss takeover has to be greater than the average duration before the boss takeover by a factor of 40% of said average to be considered significant, so if the average duration before the boss takeover would be 1500 seconds, the average duration after the boss takeover has to be greater than 1500 + 1500 * 0,4 = 2100 seconds to be considered significant. Similarly, with e.g. a threshold set to 1, it would have to be greater than 1500 + 1500 * 1 = 3000 seconds
threshold = 0.4

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the specified log, boss takeover timestam and threshold
detect_boss_mobbing(df, boss_takeover_timestamp, threshold)
