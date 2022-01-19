using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DataFramework {
    public class Structure : IQryable {
        private class Column {
            public class Modifier {
                private class IdentityData {
                    public int? seed { get; set; }
                    public int? increment { get; set; }

                    public IdentityData(int? seed, int? increment) {
                        this.seed = seed;
                        this.increment = increment;
                    }
                }

                private Regex words = new Regex(@"[A-Z][a-z]+");

                private Constructor.dbCMo type { get; set; }
                Expression[] details { get; set; }

                public Modifier(Constructor.dbCMo type, params Expression[] details) {
                    this.type = type;
                    this.details = details;
                }

                private string TypeToString(Constructor.dbCMo type) {
                    MatchCollection matches = this.words.Matches(type.ToString());
                    return string.Join(" ", matches.Cast<Match>().Select(m => m.Value.ToUpper()).ToArray());
                }

                private IdentityData GetIdentityData() {
                    int? seed, increment;
                    if (this.details == null || !details.Any()) {
                        return new IdentityData(null, null);
                    }
                    int seedOut;
                    if (!int.TryParse(this.details[0].ToString(), out seedOut)) {
                        throw new Exception("The argument provided for the identity seed is not an integer");
                    }
                    seed = seedOut;
                    if (this.details.Length < 2) {
                        return new IdentityData(seed, 1);
                    }
                    int incrementOut;
                    if (!int.TryParse(this.details[0].ToString(), out incrementOut)) {
                        throw new Exception("The argument provided for the identity increment is not an integer");
                    }
                    increment = incrementOut;
                    return new IdentityData(seed, increment);
                }

                public override string ToString() {
                    string extra = "";
                    switch (this.type) {
                        case Constructor.dbCMo.Identity:
                            IdentityData data = GetIdentityData();
                            if (data.seed != null) {
                                extra = $" ( {data.seed}, {data.increment} )";
                            }
                            break;
                    }
                    return " " + TypeToString(this.type) + extra;
                }
            }

            public string Name { get; set; }
            public bool IsNullable { get; set; }
            public Constructor.dbTyp Type { get; set; }
            public int[] Lengths { get; set; }
            public List<Modifier> modifiers { get; set; } = new List<Modifier>();

            public Column(Constructor.dbTyp type, string name, bool isNullable, int[] lengths) {
                Name = name;
                IsNullable = isNullable;
                Type = type;
                Lengths = lengths;
            }

            public void AddModifier(Constructor.dbCMo type, params Expression[] details) {
                this.modifiers.Add(new Modifier(type, details));
            }
        }

        public enum StrucOperation { declare, create, alter, drop }
        public enum StrucType { var, table }

        private StrucOperation Operation { get; set; }
        private StrucType Type { get; set; }
        private string Name { get; set; }
        private string CustomType { get; set; }
        private string Schema { get; set; }
        private Expression Value { get; set; }
        private Constructor.dbTyp VarType { get; set; }
        private int[] Lengths { get; set; }
        private List<Column> Columns = new List<Column>();

        public Structure(StrucOperation operation, StrucType type, Constructor.dbTyp varType, Expression value, string name, params int[] lengths) {
            Operation = operation;
            Type = type;
            Name = name;
            Value = value;
            VarType = varType;
            Lengths = lengths;
        }

        public Structure(StrucOperation operation, StrucType type, Constructor.dbTyp varType, string name, params int[] lengths) {
            Operation = operation;
            Type = type;
            Name = name;
            VarType = varType;
            Lengths = lengths;
        }

        public Structure(StrucOperation operation, StrucType type, Constructor.dbTyp varType, string name, string customType) {
            Operation = operation;
            Type = type;
            VarType = varType;
            Name = name;
            CustomType = customType;
        }

        public Structure(StrucOperation operation, StrucType type, Constructor.dbTyp varType, string name, string customType, string schema) {
            Operation = operation;
            Type = type;
            VarType = varType;
            Name = name;
            CustomType = customType;
            Schema = schema;
        }

        public Structure(StrucOperation operation, StrucType type, string name) {
            Operation = operation;
            Type = type;
            Name = name;
        }

        /// <summary>Adds a nullable column without type lengths</summary>
        public Structure AddColumn(Constructor.dbTyp type, string name) {
            return AddColumn(type, name, true);
        }

        /// <summary>Adds a column with determined nullability and optional type lengths</summary>
        public Structure AddColumn(Constructor.dbTyp type, string name, bool isNullable, params int[] lengths) {
            Columns.Add(new Column(type, name, isNullable, lengths));
            return this;
        }

        public Structure ColumnModifier(Constructor.dbCMo modifier, params Expression[] details) {
            Columns.Last().AddModifier(modifier, details);
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
                declaration.Append(" TABLE ( ");
                string[] serializedColumns = Columns
                    .Select(c => c.Name + DataTypeToString(c) + string.Join("", c.modifiers.Select(m => m.ToString()).ToArray()))
                    .ToArray();
                declaration.Append(string.Join(", ", serializedColumns));
                declaration.Append(" )");
            }
            else {
                string[] lens = (Lengths ?? new int[] { }).Select(l => l.ToString()).ToArray();
                if (VarType == Constructor.dbTyp.Custom) {
                    declaration.Append(" AS " + (Schema == null ? "" : Schema + ".") + CustomType);
                }
                else {
                    declaration.Append(" " + VarType.ToString().ToUpper());
                }
                declaration.Append(lens.Length > 0 ? "(" + string.Join(", ", lens) + ")" : "");
                declaration.Append(Value != null ? " = " + Value.ToString() : "");
            }
            return declaration.ToString();
        }
    }
}
