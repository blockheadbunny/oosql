using System;
using System.Linq;
using System.Collections.Generic;

namespace DataFramework {
    internal class Particle : Constructor {
        public string Value { get; set; }
        public Expression ComplexValue { get; set; }
        public dbOpe Operation { get; set; }
        public dbFun Funct { get; set; }
        public dbAgr Agg { get; set; }
        public dbLog Log { get; set; }
        public dbWTy Wty { get; set; }
        public dbWin Win { get; set; }
        public Dictionary<Expression, Constructor.dbOrd> OrderBy { get; set; }
        public string[] PartitionBy { get; set; }
        public Comparison Compar { get; set; }
        public bool IsRootCase { get; set; }
        public bool IsQuery { get; set; }

        public Particle(dbOpe oper, string val) {
            Operation = oper;
            Value = val;
        }

        public Particle(dbOpe oper, Expression expr) {
            Operation = oper;
            IsQuery = expr.IsQuery;
            SetValue(expr);
        }

        public Particle(dbAgr agg, Expression expr) {
            Operation = dbOpe.Agg;
            Agg = agg;
            SetValue(expr);
        }

        public Particle(Comparison comp, Expression then, bool isRoot) {
            Operation = dbOpe.Case;
            SetValue(then);
            Compar = comp;
            IsRootCase = isRoot;
        }

        public Particle(dbFun funct, Expression expr) {
            Operation = dbOpe.Funct;
            Funct = funct;
            SetValue(expr);
        }

        public Particle(dbLog log, Comparison comp) {
            Operation = dbOpe.Log;
            Log = log;
            Compar = comp;
        }

        public Particle(dbLog log, Expression expr) {
            Operation = dbOpe.Log;
            Log = log;
            SetValue(expr);
        }

        public Particle(dbWTy wty, dbAgr agg, dbWin win, Expression expr, Dictionary<Expression, Constructor.dbOrd> orderBy, string[] partitionBy) {
            Operation = dbOpe.Over;
            Wty = wty;
            Win = win;
            OrderBy = orderBy;
            PartitionBy = partitionBy;
            SetValue(expr);
        }

        /// <summary>Asigna el valor que se opera</summary>
        private void SetValue(Expression expr) {
            if (expr == null) {
                return;
            }
            if (expr.IsComplex || expr.IsFunction || expr.IsAggregate || expr.IsLogical) {
                ComplexValue = expr;
            }
            else {
                Value = expr.GetBaseValue();
            }
        }

        /// <summary>Obtiene el valor de cadena sin la operación</summary>
        public string GetValue() {
            return GetValue(false);
        }

        /// <summary>Obtiene el valor de cadena sin la operación</summary>
        public string GetValue(bool addParenthesesToComplex) {
            if (ComplexValue == null) {
                if (IsQuery) {
                    return (addParenthesesToComplex ? "( " : "") + Value + (addParenthesesToComplex ? " )" : "");
                }
                if (Compar == null || Operation == dbOpe.Case) {
                    return Value;
                }
                else {
                    return base.EvalComparison(Compar);
                }
            }
            else if (ComplexValue.IsFunction) {
                return ComplexValue.ToString();
            }
            else {
                return (addParenthesesToComplex ? "( " : "") + ComplexValue.ToString() + (addParenthesesToComplex ? " )" : "");
            }
        }
    }
}
