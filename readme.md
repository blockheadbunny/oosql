# Under construction...
# OOSQL

OOSQL is a framework to use MSSQL in .NET, creating queries with objects instead of concatenating text.

## Usage

There are two main components:
- **Query** class (constructs the text to send to the database)
- **DataManager** class (Makes the connection and returns the result)

Example:

    string connectionString = @"User ID=user; password=pass; Initial Catalog=dbName; Data Source=server";
    DataManager manager = new DataManager(connectionString);
    Query query = new Query()
        .Sel("b.col1", "b.col2")
        .SelAs("alias.col2", "col3")
        .SelAs("date", Expression.GetDate())
        .FromAs("alias", "table1")
        .Join("b", "table2")
        .JoinOn("alias.col1", "b.col1")
        .Where("b.col4", "column")
        .WhereVal("b.col5", "value")
        .Where("b.col6", 123)
        .Where("b.col7", DateTime.Now);
    DataTable result = manager.ExecTable(query);

Sends to the database:

    SELECT b.col1, b.col2, col3 AS alias.col2, GETDATE(  ) AS date 
    FROM table1 AS alias 
    INNER JOIN table2 AS b ON alias.col1 = b.col1 
    WHERE b.col4 = column 
    AND b.col5 = N'value' 
    AND b.col6 = 123 
    AND b.col7 = '2018-10-26T14:29:49'

...without line feeds.
# Under construction...
