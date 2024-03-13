import pm4py
import pandas as pd
import os

# The algorithm for the detection of Originator Deviation gets defined as a function
def detect_originator_deviation(log, min_k, max_k):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # A set for the different couples of originators is initialized, empty at first
    unique_originator_activity_couples = set()

    # It is iterated through the k values, the algorithm will get applied with each value in the range
    for k_value in range(min_k, max_k + 1):

        # The log is sampled into training and testing segments, the length of l1 is len(log) - k and the length of l2 is k
        l1 = log[:len(log) - k_value]
        l2 = log[len(log) - k_value:]

        # The segments get swapped every two iterations to check as much of the log as possible
        if k_value % 2 == 0:
            tmp = l1
            l1 = l2
            l2 = tmp

        # The resources (originators) assigned to the events in the training segment (l1) are stored as a model to check the testing segment against
        model_df = pm4py.convert_to_dataframe(l1)

        # A list for the events flagged with Originator Deviation is initialized, empty at first
        flagged_events = []

        # It is iterated through each event in the testing segment (l2)
        for index, row in pm4py.convert_to_dataframe(l2).iterrows():

            # The resource and activity are retrieved and stored as a tuple
            originator_activity_couple = (row['org:resource'], row['concept:name'])

            # If there is no instance of the activity represented by the regarded event in the model, it is skipped
            if row['concept:name'] not in model_df['concept:name'].values:
                continue

            # The expected originator for the activity represented by the regarded event is retrieved
            expected_originator = model_df.loc[model_df['concept:name'] == row['concept:name'], 'org:resource'].iloc[0]

            # It is checked if the assignment from the model is respected
            actual_originator = row['org:resource']
            deviation_detected = expected_originator != actual_originator

            # If the originator_activity_couple has already been flagged, it gets skipped
            if originator_activity_couple in unique_originator_activity_couples:
                continue

            # Condition for Originator Deviation, if the regarded event has an originator which is not expected accodring to the model
            if deviation_detected:
                flagged_events.append((originator_activity_couple, expected_originator, actual_originator, row['case:concept:name'], True))
                unique_originator_activity_couples.add(originator_activity_couple)

        # It is iterated through the events flagged as possible Originator Deviation
        for flagged_event in flagged_events:

            # The attributes to be printed out are retrieved from the regarded event
            originator_activity_couple, expected_originator, actual_originator, row['case:concept:name'], different_originator = flagged_event
            print(f"Possible Originator Deviation detected, the activity {originator_activity_couple[1]} in Case {row['case:concept:name']} is assigned to {actual_originator}, but it should be assigned to {expected_originator} according to the model.")

            # The results get stored in the results list
            results_entry = {
                'Activity': originator_activity_couple[1],
                'Case': row['case:concept:name'],
                'Actual Originator': actual_originator,
                'Expected Originator': expected_originator,
                'Explanation': 'The event is assigned to a different resource than what is expected according to the model'
            }
            results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\2_originatordeviation.xes"
log = pm4py.read_xes(log_path)

# Specify the minimum and maximum values of k, k determines the length of the training and testing segments of the sampled log
min_k_value = 1
max_k_value = 8

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\2_originatordeviation_results.csv"

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the specified log and k values
detect_originator_deviation(log, min_k_value, max_k_value)
