#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import pandas as pd

def format_dataframe_for_slack(df):
    # Find the maximum width of each column
    column_widths = {column: max(df[column].astype(str).apply(len).max(), len(column)) for column in df.columns}

    # Create the header row
    header_row = ' | '.join([column.center(column_widths[column]) for column in df.columns])
    table = header_row + "\n"

    # Create the separator row
    separator_row = '-+-'.join(['-' * column_widths[column] for column in df.columns])
    table += separator_row + "\n"

    # Format and add each data row
    for _, row in df.iterrows():
        row_str = ' | '.join([str(row[column]).ljust(column_widths[column]) for column in df.columns])
        table += row_str + "\n"

    # Enclose in triple backticks for Slack formatting
    return "```\n" + table + "```"

# Example DataFrame
# df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Age': [30, 25], 'City': ['New York', 'Los Angeles']})

# Prepare the formatted table for Slack
# slack_formatted_table = format_dataframe_for_slack(df)

# This string can now be sent to Slack
# print(slack_formatted_table)
