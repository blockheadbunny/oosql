using System;
using System.Collections.Generic;
using System.Linq;

namespace DataFramework {
    internal class Merger {
        internal class MergerDestiny {
            public Constructor.Table Table { get; set; }

            public string Alias {
                get { return Table.tableAlias; }
                set { Table.tableAlias = value; }
            }

            public string Schema {
                get { return Table.schema; }
                set { Table.schema = value; }
            }

            public string DataBase {
                get { return Table.database; }
                set { Table.database = value; }
            }

            public override string ToString() {
                return Table.ToString();
            }
        }

        internal class MergerOrigin {
            public Constructor.Table Table { get; set; }
            public Query Query { get; set; }
            public string Alias;

            public override string ToString() {
                return Table != null ? Table.ToString() : "( " + Query.ToString() + " )";
            }
        }

        internal class MergerAction {
            public bool Matched;
            public Constructor.dbMby By { get; internal set; }
            public List<Constructor.Comparison> Conditions = new List<Constructor.Comparison>();
            public Constructor.dbMrA Action;
            public Dictionary<string, object> Values;
            public Dictionary<string, object> Pairs = new Dictionary<string, object>();
            public Constructor.OutputClause outputClause = new Constructor.OutputClause();
        }

        public MergerDestiny Destiny = new MergerDestiny();
        public MergerOrigin Origin = new MergerOrigin();
        public List<Constructor.Comparison> Keys = new List<Constructor.Comparison>();
        public List<MergerAction> Actions = new List<MergerAction>();

        private MergerAction currentAction { get { return Actions.Count > 0 ? Actions[Actions.Count - 1] : null; } }

        public void MergeOn(Constructor.dbCom comp, Expression keyDestiny, params Expression[] keyOrigin) {
            Constructor.Comparison whr = new Constructor.Comparison();
            whr.comparator = keyDestiny.ToString();
            whr.type = comp;
            whr.values = new List<string>();
            foreach (Expression val in keyOrigin) {
                whr.values.Add(val.ToString());
            }
            Keys.Add(whr);
        }

        public void MergeOn(Expression where) {
            Keys.Add(new Constructor.Comparison() { expr = where });
        }

        public void MergeWhen(bool matched, Constructor.dbMby by) {
            MergerAction action = new MergerAction();
            action.Matched = matched;
            action.By = by;
            Actions.Add(action);
        }

        public void MergeWhen(Constructor.Comparison condition) {
            currentAction.Conditions.Add(condition);
        }

        public void MergeThen(Constructor.dbMrA action, Dictionary<string, object> values) {
            currentAction.Action = action;
            currentAction.Values = values;
        }

        public void MergeOut(Constructor.dbOut type, string table) {
            currentAction.outputClause.type = type;
            currentAction.outputClause.table = table;
        }

        public void MergeOutCols(params Constructor.Field[] columns) {
            currentAction.outputClause.columns.AddRange(columns);
        }

        public void MergeUpdCol(string columna, object valor) {
            currentAction.Pairs.Add(columna, valor);
        }
    }
}
