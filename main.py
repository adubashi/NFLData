# coding=utf-8
# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from datetime import datetime, time, timedelta

import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats
import calculate_epa



def plot():
    ###
    ### Coverage
    # initialize list of lists
    data_falcons = [['Deion Jones', 90.5], ['Robert Alford', 85.3], ['Ricardo Allen', 72.5],
            ['Desmond Trufant', 55.7], ['Duke Riley', 79.7], ['Brian Poole', 74.9],
            ['DeVondre Campbell', 69.7]]
    data_eagles = [['Rodney McLeod', 81.1], ['Brandon Graham', 70.2],
                    ['Ronald Darby', 73.8], ['Sidney Jones', 73.1], ['Malcolm Jenkins', 65.9],
                    ['Corey Graham', 60.0], ['Jordan Hicks', 58.9], ['Jalen Mills', 55.9],
                   ['Kamu Grugier-Hill', 53.0],
                   ['Nathan Gerry', 55.6]]

    # Create the pandas DataFrame
    df_falcons = pd.DataFrame(data_falcons, columns=['player_in_coverage', 'coverage_grade'])
    df_eagles = pd.DataFrame(data_eagles, columns=['player_in_coverage', 'coverage_grade'])


    colors = ['r', 'g']
    plotDF = pd.read_csv("sample-epa-report/2018090600-epa_game_report-correct.csv")

    filtered_df_falcons = plotDF.loc[plotDF['defenseTeam'] == 'ATL']
    filtered_df_eagles = plotDF.loc[plotDF['defenseTeam'] == 'PHI']
    filtered_df_falcons = filtered_df_falcons.loc[filtered_df_falcons['epa_play_count'] > 3]
    filtered_df_eagles = filtered_df_eagles.loc[filtered_df_eagles['epa_play_count'] > 3]
    merged_falcons = filtered_df_falcons.merge(df_falcons, left_on=['player_in_coverage'],
                                               right_on=['player_in_coverage'],
                                               how='inner')
    merged_eagles = filtered_df_eagles.merge(df_eagles, left_on=['player_in_coverage'],
                                               right_on=['player_in_coverage'],
                                               how='inner')

    ax = merged_eagles.plot.scatter(x = 'epa_per_targeted_play', y = 'coverage_grade')
    merged_eagles[['epa_per_targeted_play',
                   'coverage_grade',
                   'player_in_coverage']].apply(lambda row: ax.text(*row), axis=1);

    plt.xticks(rotation=90)
    plt.show()

    rvalue_eagles = scipy.stats.linregress(merged_eagles[['epa_per_targeted_play', 'coverage_grade']].to_numpy()).rvalue ** 2
    rvalue_falcons = scipy.stats.linregress(merged_falcons[['epa_per_targeted_play', 'coverage_grade']].to_numpy()).rvalue ** 2
    print("RValue - Eagles" + str(rvalue_eagles))
    print("RValue - Falcons" + str(rvalue_falcons))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    calculate_epa.calculate_and_plot("2018101401")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
