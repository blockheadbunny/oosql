using System;
using System.Text;
using System.Collections.Generic;

namespace DataFramework {
    internal class Instruction : IQryable {
        public enum ItrType {
            use,
            tran,
            commit,
            rollback
        }

        public ItrType Type { get; set; }
        public bool SingleTransaction { get; set; }
        public string Id { get; set; }
        public string DataBase { get; set; }

        public Instruction(ItrType type) {
            Type = type;
        }

        public Instruction(string database) {
            Type = ItrType.use;
            DataBase = database;
        }

        public override string ToString() {
            StringBuilder buffer = new StringBuilder();
            switch (Type) {
                case ItrType.tran:
                    buffer.Append("BEGIN TRANSACTION");
                    if (SingleTransaction && Id == null) {
                        buffer.Append(" " + Id);
                    }
                    break;
                case ItrType.commit:
                    buffer.Append("COMMIT");
                    if (SingleTransaction && Id == null) {
                        buffer.Append(" TRANSACTION " + Id);
                    }
                    break;
                case ItrType.rollback:
                    buffer.Append("ROLLBACK");
                    if (SingleTransaction && Id == null) {
                        buffer.Append(" TRANSACTION " + Id);
                    }
                    break;
                case ItrType.use:
                    buffer.Append("USE " + DataBase);
                    break;
            }
            return buffer.ToString();
        }
    }
}
