import pm4py
import pandas as pd
import numpy as np
import os

# The algorithm for the detection of Peer Mobbing gets defined as a function
def detect_peer_mobbing(log, threshold_dv, threshold_pm):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # Three dictionaries are initialized for the frequency of each activity for each resources and for all resources and the average, empty at first
    activity_frequency = {}
    total_frequency = {}
    average_frequency = {}

    # It is iterated through each resource and their performed activities
    for resource in df['org:resource'].unique():
        for activity in df[df['org:resource'] == resource]['concept:name']:

            # The count of how often the regarded activity was performed by the regarded resource gets incremented
            activity_frequency.setdefault(resource, {}).setdefault(activity, 0)
            activity_frequency[resource][activity] += 1

    # Total frequency for all resources combined gets calculated and stored for each activity
    for activity in df['concept:name'].unique():
        total_frequency[activity] = sum(activity_frequency.get(resource, {}).get(activity, 0) for resource in df['org:resource'].unique())

    # Average of total frequencies gets calculated and stored for each activity
    for activity in df['concept:name'].unique():
        average_frequency[activity] = total_frequency[activity] / len(df['org:resource'].unique())

    # It is iterated through each different activity
    for activity in df['concept:name'].unique():

        # It is iterated through each resource, regarding them as potential victims of Peer Mobbing
        for victim_resource in df['org:resource'].unique():

            # The deviation of the victim's frequency from the average frequency is retrieved
            victim_deviation = activity_frequency.get(victim_resource, {}).get(activity, 0) - average_frequency[activity]

            # A list for resources potentially involved in Peer Mobbing is initialized, empty at first
            mobbing_resources = []

            # It is iterated through each resource, regarding them as potential initiators of Peer Mobbing
            for resource in df['org:resource'].unique():

                # The deviation of the resource's frequency from the average frequency is retrieved
                resource_activity_frequency = activity_frequency.get(resource, {}).get(activity, 0)
                deviation = resource_activity_frequency - average_frequency[activity]

                # The threshold for a significant difference in deviation of frequency between the mobbing resource and victim resource gets calculated by multiplying the given threshold factor with the average frequency of the regarded activity being performed by a resource
                threshold_1 = int(average_frequency[activity] * threshold_dv)

                # If the deviation is significantly greater than the victim deviation, the regarded resource is added to the mobbing_resources list
                if deviation > (victim_deviation + threshold_1):
                    mobbing_resources.append(resource)

            # The threshold for a significant amount of resources in the mobbing_resources list gets calculated by multiplying the given threshold factor with the total number of resources in the log
            threshold_2 = int(len(df['org:resource'].unique()) * threshold_pm)

            # Condition for Peer Mobbing, if a group of resources seems to take away certain tasks from another resource, manifesting in the group performing the activities for these tasks more often, here it gets checked if the group is big enough to be considered significant
            if len(mobbing_resources) > threshold_2:
                print(f"Possible Peer Mobbing detected for activity {activity} initiated by resources: {mobbing_resources}, possible victim: {victim_resource}.")

                # The results get stored in the results list
                results_entry = {
                    'Initiating Resources': mobbing_resources,
                    'Victim Resource': victim_resource,
                    'Activity': activity,
                    'Explanation': 'The listed group seems to perform the listed task significantly more often than the single resource listed, indicating possible Peer Mobbing'
                }
                results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\11_peermobbing.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\11_peermobbing_results.csv"

# Set a dynamic threshold for significant deviation of the potential "Mobber"'s frequency of performing an activity from the average, the threshold gets scaled based on the average frequency of an activity being performed by a resource. A value of e.g. 1 means the frequency of an activity being performed by a resource has to be greater than the frequency of an activity being performed by a potential victim resource by a factor of 100% of the average frequency to be considered significant, so if an activity in the log is performed 0 times by the potential victim and 3 times on average, the regarded potential mobbing resource has to perform the activity more than 3 * 1 = 3 times to be considered significant, that means to get added onto the list of initiators of Peer Mobbing. Similarly, with e.g. a threshold set to 2, it would have to be more than 3 * 2 = 6 times.
threshold_dv = 1

# Set a dynamic threshold for a significant amount of resources getting identified as possible initiators of Peer Mobbing, the threshold gets scaled based on the number of resources in the log. A value of e.g. 0.5 means the number of resources identified as "Mobbers" has to be greater than the total number of resources in the log by a factor of 50% of this number to be considered significant, so if there are 4 resources in the log, the number of resources on the list of initiators of Peer Mobbing has to be greater than 4 * 0.5 = 2 resources to be considered significant. Similarly, with e.g. a threshold set to 0.25, it would have to be more than 4 * 0.25 = 1 resource.
threshold_pm = 0.4

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# Call the function with the specified log and thresholds
detect_peer_mobbing(log, threshold_dv, threshold_pm)
