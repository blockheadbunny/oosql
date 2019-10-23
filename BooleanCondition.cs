using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataFramework {
    internal class BooleanCondition : IQryable {
        internal Expression Condition { get; set; }
        internal IQryable TruePart { get; set; }
        internal IQryable FalsePart { get; set; }

        public BooleanCondition(Expression condition, IQryable truePart, IQryable falsePart) {
            Condition = condition;
            TruePart = truePart;
            FalsePart = falsePart;
        }

        public override string ToString() {
            StringBuilder sb = new StringBuilder();
            bool addParentheses = Condition.particles.First().Operation == Constructor.dbOpe.Qry;
            string strCondition = (addParentheses ? "( " : "") + Condition.ToString() + (addParentheses ? " )" : "");
            sb.AppendLine("IF " + strCondition + " BEGIN");
            sb.AppendLine(TruePart.ToString());
            sb.Append("END");
            if (FalsePart != null) {
                sb.AppendLine();
                sb.AppendLine("THEN BEGIN");
                sb.AppendLine(FalsePart.ToString());
                sb.Append("END");
            }
            return sb.ToString();
        }
    }
}
