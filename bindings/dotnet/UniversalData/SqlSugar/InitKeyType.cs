namespace SqlSugar;

public enum InitKeyType
{
    [Obsolete("Use attributes to declare entity keys.")]
    SystemTable = 0,
    Attribute = 1
}
