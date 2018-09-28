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
            public List<Constructor.Comparison> Conditions = new List<Constructor.Comparison>();
            public Constructor.dbMrA Action;
            public Dictionary<string, object> Values;
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

        public void MergeWhen(bool matched) {
            MergerAction action = new MergerAction();
            action.Matched = matched;
            Actions.Add(action);
        }

        public void MergeWhen(Constructor.Comparison condition) {
            currentAction.Conditions.Add(condition);
        }

        public void MergeThen(Constructor.dbMrA action, Dictionary<string, object> values) {
            currentAction.Action = action;
            currentAction.Values = values;
        }
    }
}
