namespace SqlSugar;

/// <summary>Database dialect understood by the Native connector plugins.</summary>
public enum DbType
{
    MySql = 0,
    SqlServer = 1,
    Sqlite = 2,
    Oracle = 3,
    PostgreSQL = 4,
    MariaDb = 901
}
