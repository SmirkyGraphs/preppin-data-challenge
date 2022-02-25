import pandas as pd
import numpy as np

df = pd.read_csv('../2022-W01/data/raw/PD 2022 Wk 1 Input.csv')
subjects = pd.read_csv('./data/raw/PD 2022 WK 3 Grades.csv')

if __name__ == '__main__':

    # get student average of all subjects
    subjects['students_avg'] = subjects.drop('Student ID', axis=1).mean(axis=1)
    subjects = subjects.melt(id_vars=['Student ID', 'students_avg'])

    # get table of pass/fail and student average
    subjects = (subjects
        .assign(passed_subjects = np.where(subjects['value'] >= 75, True, False))
        .groupby('Student ID')[['students_avg', 'passed_subjects']]
        .agg({'students_avg': 'max', 'passed_subjects': 'sum'})
        .reset_index()
    )

    # merge subjects with students gender
    df = df[['id', 'gender']].merge(subjects, left_on='id', right_on='Student ID')

    # filter wanted columns and save to csv file
    cols = ['passed_subjects', 'students_avg', 'Student ID', 'gender']
    df[cols].to_csv('./data/clean/2022-W03-output.csv', index=False)