import pandas as pd
import pm4py
from collections import defaultdict
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

# The calculation of the frequency of each activity occurring in the log gets defined as a function
def calculate_activity_frequencies(log):

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # The count of how often each different activity occurred in the log gets calculated, stored and returned
    activity_frequencies = df['concept:name'].value_counts(normalize=True).to_dict()
    return activity_frequencies

# The categorization of cases into process variants gets defined as a function, process variants are differentiated by their sequence of activities, so cases containing the same activity sequences are categorized in one variant
def categorize_process_variants(log):

    # A defaultdict for the possible activity orders (process variants) is initialized, empty at first
    process_variants = defaultdict(list)

    # The log is grouped by cases
    grouped_cases = log.groupby('case:concept:name')

    # It is iterated through each case
    for case, activities in grouped_cases:

        # The activities are ordered based on their timestamps
        ordered_activities = activities.sort_values('time:timestamp')

        # Those ordered activities are converted to a tuple to use it as a key in the dictionary
        activity_sequence = tuple(f"{activity['concept:name']}" for _, activity in ordered_activities.iterrows())

        # The regarded case gets appended to the corresponding process variant, categorizing it
        process_variants[activity_sequence].append(case)

    # The process_variants dictionary is returned, containing the mapping of activity sequences to lists of cases, representing process variants
    return process_variants

# The algorithm for the detection of the first condition of Gold Plating, certain process variants containing events with a significantly longer average duration compared to other variants, gets defined as a function
def detect_gold_plating_duration(log, process_variants, threshold):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # The event log is converted to a dataframe for easier data analysis
    df = pm4py.convert_to_dataframe(log)

    # The duration calculation function gets called with the log converted to a dataframe to add a duration column
    df = calculate_time_taken(df)
    
    # A dictionary is initialized for the average durations of each activity, empty at first
    avg_durations = {}

    # It is iterated through each variant and its cases
    for variant, cases in process_variants.items():

        # Only cases in the regarded variant are filtered
        variant_df = df[df['case:concept:name'].isin(cases)]
        
        # Average durations for each case within the variant are calculated and stored
        case_avg_durations = variant_df.groupby('case:concept:name')['duration'].mean()

        # The overall average duration for the variant is calculated and stored
        variant_avg_duration = case_avg_durations.mean()
        avg_durations[variant] = variant_avg_duration

        # First condition for Gold Plating, if the variant has a significantly longer average activity duration than the average variant
        if (variant_avg_duration - df['duration'].mean()) > df['duration'].mean() * threshold:
            print(f"Possible Gold Plating detected, the cases {cases} represent a process variant which contains activities with a significantly longer average event duration ({variant_avg_duration} seconds) in comparison with the average activity duration of the average process variant ({df['duration'].mean()} seconds)")

            # The results get stored in the results list
            results_entry = {
                'Cases': cases,
                'Variant Average Activity Duration': variant_avg_duration,
                'Average Variant Average Activity Duration': df['duration'].mean(),
                '"Weird" activity': '',
                'Activity Count': '',
                'Total Activity Count': '',
                'Explanation': 'The process variant has a significantly longer average activity duration than the average variant'
            }
            results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# The algorithm for the detection of the second condition of Gold Plating, certain process variants containing "weird" (significantly rare) activities, gets defined as a function
def detect_gold_plating_rare(log, process_variants, activity_frequencies, threshold):

    # A list for the results that later get written in the output csv table is initialized, empty at first
    results = []

    # A dictionary is initialized for the flagged variants (variants with a rare activity), empty at first
    flagged_variants = {}

    # It is iterated through each variant and its cases
    for variant, cases in process_variants.items():

        # A variable for a rare activity is initialized, empty at first
        rare_activity = None

        # It is iterated through each activity in the variant
        for activity in variant:

            # The proportion of the occurrence of the regarded activity in the entire log and the total number of activities in the log are retrieved
            proportion = activity_frequencies[activity]
            total_activities_count = len(pm4py.convert_to_dataframe(log))

            # If the proportion of the occurrence of the regarded activity in the log is significantly low, the activity is marked as the rare/weird activity
            if proportion < threshold:
                rare_activity = activity
                break

        # Second condition for Gold Plating, if the variant contains a significantly rare activity
        if rare_activity:
            activity_count = int(total_activities_count * proportion)
            print(f"Possible Gold Plating detected, the cases {cases} represent a process variant which contains a significantly rare activity, {rare_activity}, since it only makes up {activity_count} of total {total_activities_count} activities in the log")

            # The results get stored in the results list
            results_entry = {
                'Cases': cases,
                'Variant Average Activity Duration': '',
                'Average Variant Average Activity Duration': '',
                '"Weird" Activity': rare_activity,
                'Activity Count': activity_count,
                'Total Activity Count': total_activities_count,
                'Explanation': 'The process variant contains a significantly rare activity'
            }
            results.append(results_entry)

    # The results are written to a csv table if an according path got specified
    if output_csv_path:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv_path, mode='a', index=False, header=not os.path.exists(output_csv_path))
        print(f"Results appended to {output_csv_path}")

# Specify the path to the event log in .xes format
log_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\example logs\\8_goldplating.xes"
log = pm4py.read_xes(log_path)

# Specify the path to the output file where the results get stored in .csv format, can also be left empty, then this part just gets skipped
output_csv_path = "C:\\Users\\tbjac\\Documents\\uni\\bachelorarbeit\\pattern detection codes\\csv results\\8_goldplating_results.csv"

# The function to categorize cases into process variants is called
process_variants = categorize_process_variants(log)

# Set a dynamic threshold for significant longer average duration of an activity in a process variant, the threshold gets scaled based on the average activity duration of the average variant. A value of 0.4 means the average duration of an activity in a regarded variant has to be greater than the the average activity duration of the average variant by a factor of 0.4 to be considered significant, so if the average activity duration of the average variant would be 1000 seconds, the average duration of an activity in a regarded variant would have to be more than 1000 + (1000 * 0.4) = 1400 seconds to be considered significant. Similarly, with e.g. a threshold set to 1, it would have to be more than 1000 + (1000 * 1) = 2000 seconds.
duration_threshold = 0.4  # Replace with your desired threshold in seconds

# Set a threshold for significantly low proportions of activities to be considered a "weird"/rare activity. A value of 0.04 means an activity has to make up less than 4% of the events in the whole log, similarly, with e.g. a threshold set to 0.1, it would have less than 10%, or with a threshold of 0.01 less than 1%.
activity_threshold = 0.04  # Replace with your desired threshold for rare activities

# Clear the content of the csv file if it exists so only the results from one run are written in the file
if os.path.exists(output_csv_path):
    os.remove(output_csv_path)

# The functions are called with the specified log, process variants and thresholds
detect_gold_plating_duration(log, process_variants, duration_threshold)
detect_gold_plating_rare(log, process_variants, calculate_activity_frequencies(log), activity_threshold)
