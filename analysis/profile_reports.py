import pandas as pd
import pandas_profiling


analysis_parameters = [
    dict(
        path='../data/input_data/sales_train.csv',
        name='sales_train'
    ),
    dict(
        path='../data/input_data/items.csv',
        name='items'
    ),
    dict(
        path='../data/input_data/item_categories.csv',
        name='item_categories'
    ),
    dict(
        path='../data/input_data/shops.csv',
        name='shops'
    ),
    dict(
        path='../data/output_data/merged_data.csv',
        name='merged'
    )
]


def create_report(path, name):
    """
    Summary
    -------

    Imports a csv as a pandas dataframe and creates a profile report using
    pandas_profiling.

    Parameters
    ----------
    path: str
        The path to the csv file to be profiled.

    name: str
        The name of the profile report that is saved in data/reports

    Returns
    -------
    None

    Example
    -------
    create_report('../data/input_data/sales_train.csv', 'sales_train')
    """

    df = pd.read_csv(path)
    profile = df.profile_report(style={'full_width': True})
    profile.to_file(output_file=f'../data/reports/{name}.html')


if __name__ == '__main__':
    print('Generating reports...')
    for parameter in analysis_parameters:
        create_report(parameter['path'], parameter['name'])
        print(f'{parameter["name"]} report generated.')
