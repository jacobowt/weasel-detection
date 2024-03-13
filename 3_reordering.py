import pm4py
import pandas as pd
from collections import defaultdict
import numpy as np
import os

# An algorithm for generating the possible orders of activities from the cases in the training segment gets defined as a function, this serves as the model here
def generate_activity_orders(training_log):

    # A defaultdict for the possible activity orders is initialized, empty at first
    activity_orders = defaultdict(list)

    # The training log is grouped by cases
    grouped_cases = training_log.groupby('case:concept:name')

    # It is iterated through each case
    for case, events in grouped_cases:

        # The events are ordered based on their timestamps
        ordered_events = events.sort_values('time:timestamp')

        # The activites of these ordered events get converted to a list of dictionaries where each dictionary represents an event, and an index is added to each event starting from 1.
        activity_sequence = [f"{i}.{event['concept:name']}" for i, event in enumerate(ordered_events.to_dict(orient='records'), start=1)]

        # The sequence of activities for the regarded case with their activities is appended to the activity_orders dictionary
        activity_orders[case].append((activity_sequence,))

    # The dictionary containing all possible orders of activities according to the cases in the training segment is returned
    return activity_orders

# The algorithm for the detection of Re-Ordering gets defined as a function
def detect_reordering(testing_log, activity_orders):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # It is iterated through each case
    for case, events in testing_log.groupby('case:concept:name'):

        # The events are ordered based on their timestamps
        ordered_events = events.sort_values('time:timestamp')

        # The activites of these ordered events get converted to a list of dictionaries where each dictionary represents an event, and an index is added to each event starting from 1.
        activity_sequence = [f"{i}.{event['concept:name']}" for i, event in enumerate(ordered_events.to_dict(orient='records'), start=1)]

        # It gets assumed that Re-Ordering is present first
        reordering = True

        # It is iterated through each possible order in the generated model
        for learned_orders in activity_orders.values():
            for learned_order in learned_orders:

                # If all activities in the sequence match the regarded order from the model, there is no Re-Ordering in this sequence of events
                if all(event.lower().strip() == learned_activity.lower().strip() for event, learned_activity in zip(activity_sequence, learned_order[0])):
                    reordering = False
                    break

            # If the sequence of events already matched one order from the model, the remaining orders don't have to be regarded anymore
            if reordering == False:
                break

        # Condition for Re-Ordering, if the regarded sequence of activities (= activities in a case) doesn't match any of the possible avtivity orders from the generated model
        if reordering == True:
            print(f"Possible Re-Ordering detected, the case {case} contains a sequence of activities that does not match any sequence from the model. Following activities in this case occur in an unexpected order: ", end="")
            first = True
            for activity, learned_activity in zip(activity_sequence, learned_order[0]):
                if activity.lower().strip() != learned_activity.lower().strip():
                    if first == True:
                        print(f"{activity}", end="")
                    else:
                        print(f", {activity}", end="")
                    first = False
            print()

            # The results get stored in the results list
            results_entry = {
                'Case': case,
                'Explanation': 'The case contains an unexpected sequence of activities'
            }
            results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=True)
        print(f"Results appended to {output_csv_path}")


# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\BPI_Challenge_2012.xes"
log = pm4py.read_xes(log_path)

# The cases are shuffled randomly and then sampled into training cases and testing cases
np.random.seed(0)
cases = log['case:concept:name'].unique()
np.random.shuffle(cases)
split_idx = len(cases) // 2
training_cases = cases[:split_idx]
testing_cases = cases[split_idx:]

# A training segment and a testing segment are created from the sampled cases
training_log = log[log['case:concept:name'].isin(training_cases)]
testing_log = log[log['case:concept:name'].isin(testing_cases)]

# A model of possible activity orders is generated by calling the corresponding function
activity_orders = generate_activity_orders(training_log)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\3_reordering_results.csv"

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the testing log and event orders
detect_reordering(testing_log, activity_orders)

# The roles of training and testing logs are swapped and the algorithm is applied again to check as much of the log as possible
tmp = training_log
training_log = testing_log 
testing_log = tmp
activity_orders = generate_activity_orders(training_log)
detect_reordering(testing_log, activity_orders)
