import pm4py
import pandas as pd
import os
import numpy as np

# The algorithm for the detection of Activity Deviation gets defined as a function
def detect_activity_deviation(log):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # The dataframe gets randomly split into two samples (50% are used for creating the model, 50% are tested against the model)
    model_df = df.sample(frac=0.5, random_state=1)
    test_df = df.drop(model_df.index)

    # An inner function is defined to make it possible to apply the algorithm two times, swapping the roles of the training (model) and testing dataframes in the second iteration
    def inner_function(training_sample, testing_sample):

        # All different activities from the training segment are retrieved
        model_activities = set(training_sample['concept:name'].unique())

        # It is iterated through each event in the log
        for index, row in testing_sample.iterrows():

            # Condition for Activity Deviation, if the activity represented by the regarded event is only found in the log, but not in the model
            if row['concept:name'] not in model_activities:
                print(f"Possible Activity Deviation detected, the activity represented by the event {row['concept:name']} from the case {row['case:concept:name']} does not occur in the model")

                # The results get stored in the results list
                results_entry = {
                    'Activity': row['concept:name'],
                    'Case': row['case:concept:name'],
                    'Explanation': 'The activity does occur in the log, but not in the model'
                }
                results.append(results_entry)

    # The inner function is called two times, swapping the roles of the model and testing segments in the second iteration to check as much of the log as possible
    inner_function(model_df, test_df)
    inner_function(test_df, model_df)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")


# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\1_activitydeviation.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\1_activitydeviation_results.csv"

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The function is called with the specified log
detect_activity_deviation(log)
