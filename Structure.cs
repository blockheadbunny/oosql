using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataFramework {
    public class Structure : IQryable {
        private class Column {
            public string Name { get; set; }
            public bool IsNullable { get; set; }
            public Constructor.dbTyp Type { get; set; }
            public int[] Lengths { get; set; }

            public Column(Constructor.dbTyp type, string name, bool isNullable, int[] lengths) {
                Name = name;
                IsNullable = isNullable;
                Type = type;
                Lengths = lengths;
            }
        }

        public enum StrucOperation { declare, create, alter, drop }
        public enum StrucType { var, table }

        private StrucOperation Operation { get; set; }
        private StrucType Type { get; set; }
        private string Name { get; set; }
        private Expression Value { get; set; }
        private Constructor.dbTyp VarType { get; set; }
        private int[] Lengths { get; set; }
        private List<Column> Columns = new List<Column>();

        public Structure(StrucOperation operation, StrucType type, Constructor.dbTyp varType, Expression value, string name, int[] lengths) {
            Operation = operation;
            Type = type;
            Name = name;
            Value = value;
            VarType = varType;
            Lengths = lengths;
        }

        public Structure(StrucOperation operation, StrucType type, Constructor.dbTyp varType, string name, int[] lengths) {
            Operation = operation;
            Type = type;
            Name = name;
            VarType = varType;
            Lengths = lengths;
        }

        public Structure(StrucOperation operation, StrucType type, string name) {
            Operation = operation;
            Type = type;
            Name = name;
        }

        public Structure AddColumn(Constructor.dbTyp type, string name, bool isNullable, params int[] lengths) {
            Columns.Add(new Column(type, name, isNullable, lengths));
            return this;
        }

        private string DataTypeToString(Constructor.dbTyp type, int[] lengths, bool isNullable, bool isColumn) {
            string[] lens = lengths.Select(l => l.ToString()).ToArray();
            string res = " "
                + type.ToString().ToUpper()
                + (lens.Length > 0 ? "(" + string.Join(", ", lens) + ")" : "");
            if (isColumn) {
                res += (isNullable ? "" : " NOT") + " NULL";
            }
            return res;
        }

        private string DataTypeToString(Constructor.dbTyp type, int[] lengths, bool isNullable) {
            return DataTypeToString(type, lengths, isNullable, false);
        }

        private string DataTypeToString(Column col) {
            return DataTypeToString(col.Type, col.Lengths, col.IsNullable, true);
        }

        public override string ToString() {
            StringBuilder declaration = new StringBuilder();
            declaration.Append("DECLARE @" + Name);
            if (Type == StrucType.table) {
                declaration.Append(" ( ");
                string[] serializedColumns = Columns
                    .Select(c => c.Name + DataTypeToString(c))
                    .ToArray();
                declaration.Append(string.Join(", ", serializedColumns));
                declaration.Append(" )");
            }
            else {
                string[] lens = Lengths.Select(l => l.ToString()).ToArray();
                declaration.Append(" " + VarType.ToString().ToUpper());
                declaration.Append(lens.Length > 0 ? "(" + string.Join(", ", lens) + ")" : "");
                declaration.Append(Value != null ? " = " + Value.ToString() : "");
            }
            return declaration.ToString();
        }
    }
}
