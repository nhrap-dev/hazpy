def getStudyRegions():
    """Creates a dataframe of all study regions in the local Hazus SQL Server database

            Returns:
                studyRegions: pandas dataframe
        """
    exclusionRows = ['master', 'tempdb', 'model',
                     'msdb', 'syHazus', 'CDMS', 'flTmpDB']
    cursor.execute('SELECT [StateID] FROM [syHazus].[dbo].[syState]')
    for state in cursor:
        exclusionRows.append(state[0])
    query = 'SELECT * FROM sys.databases'
    df = pd.read_sql(query, conn)
    studyRegions = df[~df['name'].isin(exclusionRows)]['name']
    studyRegions = studyRegions.reset_index()
    studyRegions = studyRegions.drop('index', axis=1)
    studyRegions = studyRegions
    return studyRegions
