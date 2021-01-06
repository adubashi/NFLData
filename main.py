# coding=utf-8
# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from datetime import datetime, time, timedelta

import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats
import calculate_epa
from datetime import datetime
import random



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

def plot2():
    ###
    # Create the pandas DataFrame
    pff_grade = pd.read_csv("sample-epa-report/2018090600-PFF-Grades.csv")
    epa_report = pd.read_csv("sample-epa-report/2018090600-epa_game_report-correct.csv")
    exclude_players = ["Rasul Douglas", "Corey Graham", "Robert Alford"]
    snap_count = 3

    epa_report_with_grade = epa_report.merge(pff_grade, left_on=['player_in_coverage'],
                                                  right_on=['displayName'],
                                                  how='inner')

    #epa_report_with_grade = epa_report_with_grade.query("displayName not in @exclude_players")
    epa_report_with_grade = epa_report_with_grade.query("epa_play_count > @snap_count")

    ax = epa_report_with_grade.plot.scatter(x = 'epa_per_targeted_play', y = 'coverageGrade')
    epa_report_with_grade[['epa_per_targeted_play',
                   'coverageGrade',
                   'player_in_coverage']].apply(lambda row: ax.text(*row), axis=1);

    plt.xticks(rotation=90)
    plt.show()


    rvalue = scipy.stats.linregress(epa_report_with_grade[['epa_per_targeted_play', 'coverageGrade']].to_numpy()).rvalue ** 2
    print("RSquared Value: " + str(rvalue))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    now = datetime.now()

    '''
    list_of_players = calculate_epa.get_list_of_players_from_pff()
    list_of_players = random.sample(list_of_players, k=80)
    excluded_players = ["P.J. Williams", "Ahkello Witherspoon","Buster Skrine", "Captain Munnerlyn", "Mike Hilton"]
    print(list_of_players)
    epaReportDefenseGrades = calculate_epa.compare_pff_with_epa_report(list_of_players)
    epaReportDefenseGrades = pd.read_csv("sample-epa-report/epa-report-corner-2018.csv")
    epaReportDefenseGrades = epaReportDefenseGrades.query("displayName not in @excluded_players")
    epaReportDefenseGrades.to_csv("epa-report-corner-2018.csv")
    epaReportDefenseGrades_Plot = epaReportDefenseGrades.copy()

    rvalue_squaredValueEPA = scipy.stats.linregress(epaReportDefenseGrades[['epa_per_targeted_play', 'grades_coverage_defense']].to_numpy()).rvalue ** 2
    rvalue_squaredValuePasserRating = scipy.stats.linregress(epaReportDefenseGrades[['qb_rating_against', 'grades_coverage_defense']].to_numpy()).rvalue ** 2
    print("RValue - EPA: " + str(rvalue_squaredValueEPA))
    print("RValue - PasserRating: " + str(rvalue_squaredValuePasserRating))
    #calculate_epa.calculate_and_plot("2018090600")
    #calculate_epa.generate_football_distance_for_tracking_week("week1-2018090600-sample.csv")
    calculate_epa.calculate_epa_game_report("2018090600")
    print("Time Taken: " + str((datetime.now() - now).total_seconds()))

    ax = epaReportDefenseGrades_Plot.plot.scatter(x='epa_per_targeted_play', y='grades_coverage_defense')
    epaReportDefenseGrades_Plot[['epa_per_targeted_play',
                   'grades_coverage_defense',
                   'displayName']].apply(lambda row: ax.text(*row), axis=1);

    plt.xticks(rotation=90)
    #plt.show()
    '''
    now = datetime.now()
    excluded_players = ["Ahkello Witherspoon", "Richard Sherman"]
    df = calculate_epa.get_merged_epa_report_for_player_list(excluded_players)
    print(df)
    print(datetime.now() - now)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
'''
#colorlist = [colors.rgb2hex(colormap(i)) for i in np.linspace(0, 1, len(epaReportDefenseGrades_Plot['displayName']))]
def duplicate(testList, n):
    x = len(testList)
    new_list = []
    for j in range(x):
        new_list.extend(testList[j] for _ in range(n))
    return new_list
colors_graph_list = [1]
colors_for_graph = duplicate(colors_graph_list,29)

ax = epaReportDefenseGrades_Plot.plot.scatter(x='epa_per_targeted_play', y='grades_coverage_defense', color=colors_for_graph)
for i,c in enumerate(colors_graph_list):
    x = epaReportDefenseGrades_Plot['epa_per_targeted_play'].iloc[i]
    y = epaReportDefenseGrades_Plot['grades_coverage_defense'].iloc[i]
    l = epaReportDefenseGrades_Plot['displayName'].iloc[i]
    ax.scatter(x, y, label=l, s=50, linewidth=0.1, c=c)
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.show()


'''