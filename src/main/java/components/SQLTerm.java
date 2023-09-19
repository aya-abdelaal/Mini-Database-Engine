package components;

public class SQLTerm {
    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;
    // SELECT * FROM _strTableName WHERE X = 1 AND Y = 2

    public SQLTerm(String tableName, String columnName, String operator, Object value) {
        this._strTableName = tableName;
        this._strColumnName = columnName;
        this._strOperator = operator;
        this._objValue = value;
    }
    @Override
    public boolean equals(Object o) {
        SQLTerm c = (SQLTerm) o;
        return c._strColumnName.equals(this._strColumnName) && c._objValue.equals(this._objValue) && c._strTableName.equals(this._strTableName) && c._strOperator.equals(this._strOperator);
    }

    @Override
    public String toString() {
        return "Table: " + this._strTableName + "\nColumn" + this._strColumnName + "\nValue: " + this._objValue.toString() + "\nOperator: " + this._strOperator;
    }
}