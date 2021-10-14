using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Globalization;

namespace DataFramework {
    /// <summary>Operaciónes que representan un mismo campo en una consulta</summary>
    public class Expression : Constructor {
        internal List<Particle> particles = new List<Particle>();

        public bool IsComplex { get { return particles.Count > 1; } }
        public bool IsFunction { get { return particles[0].Operation == dbOpe.Funct; } }
        public bool IsAggregate { get { return particles[0].Operation == dbOpe.Agg; } }
        public bool IsLogical { get { return particles[0].Operation == dbOpe.Log; } }

        /// <summary>Inicia una nueva expresión matemática, lógica, de casos o de función</summary>
        public Expression(string expr) {
            particles.Add(new Particle(dbOpe.NoOp, base.SanitizeSQL(expr)));
        }

        public Expression(Query expr) {
            particles.Add(new Particle(dbOpe.Qry, expr.ToString()));
        }

        private Expression() {
            particles.Add(new Particle(dbOpe.NoOp, "NULL"));
        }

        private Expression(DateTime expr) {
            particles.Add(new Particle(dbOpe.NoOp, "'" + expr.ToString("yyyy-MM-ddTHH:mm:ss") + "'"));
        }

        private Expression(dbFun funct, Expression expr) {
            particles.Add(new Particle(funct, expr));
        }

        private Expression(dbLog log, Comparison comp) {
            particles.Add(new Particle(log, comp));
        }

        private Expression(Comparison comp, Expression then, bool isRoot) {
            particles.Add(new Particle(comp, then, isRoot));
        }

        private Expression(dbTim datepart) {
            particles.Add(new Particle(dbOpe.NoOp, datepart.ToString().ToUpper()));
        }

        private Expression(dbAgr agg, Expression expr) {
            particles.Add(new Particle(agg, expr));
        }

        private Expression(dbWTy wty, dbAgr agg, dbWin win, Expression expr, Dictionary<string, Constructor.dbOrd> orderBy, string[] partitionBy) {
            particles.Add(new Particle(wty, agg, win, expr, orderBy, partitionBy));
        }

        private Expression(List<Particle> clonedParticles) {
            particles = clonedParticles;
        }

        public static implicit operator Expression(string expr) { return new Expression(expr); }
        public static implicit operator Expression(long expr) { return new Expression(expr.ToString()); }
        public static implicit operator Expression(int expr) { return new Expression(expr.ToString()); }
        public static implicit operator Expression(decimal expr) { return new Expression(expr.ToString("G", CultureInfo.InvariantCulture)); }
        public static implicit operator Expression(float expr) { return new Expression(expr.ToString("G", CultureInfo.InvariantCulture)); }
        public static implicit operator Expression(double expr) { return new Expression(expr.ToString("G", CultureInfo.InvariantCulture)); }
        public static implicit operator Expression(bool expr) { return new Expression((expr ? 1 : 0).ToString()); }
        public static implicit operator Expression(DateTime expr) { return new Expression(expr); }
        public static implicit operator Expression(Query expr) { return new Expression(expr); }
        public static implicit operator Expression(DBNull expr) { return new Expression(); }

        /// <summary>Evalua la expressión y devuelve su representación textual</summary>
        public override string ToString() {
            StringBuilder expression = new StringBuilder();
            foreach (Particle part in particles) {
                switch (part.Operation) {
                    case dbOpe.NoOp:
                        expression.Append(part.Value);
                        break;
                    case dbOpe.Funct:
                        expression.Append(part.Funct.ToString().ToUpper() + "( " + part.GetValue() + " )");
                        break;
                    case dbOpe.Agg:
                        expression.Append(part.Agg.ToString().ToUpper() + "( " + part.GetValue() + " )");
                        break;
                    case dbOpe.Case:
                        expression.Append((part.IsRootCase ? "CASE " : "") + base.dbOpeToString(part.Operation) + " " + base.EvalComparison(part.Compar) + " THEN " + part.GetValue() + " ");
                        break;
                    case dbOpe.Else:
                        expression.Append(base.dbOpeToString(part.Operation) + " " + part.GetValue() + " END");
                        break;
                    case dbOpe.Comma:
                        expression.Append(base.dbOpeToString(part.Operation) + " " + part.GetValue(true));
                        break;
                    case dbOpe.In:
                        expression.Append(base.dbOpeToString(part.Operation) + "(" + part.GetValue(false) + ")");
                        break;
                    case dbOpe.Log:
                        expression.Append((part.Log != dbLog.Where ? " " : "") + base.dbLogToString(part.Log) + (part.Log != dbLog.Where ? " " : "") + part.GetValue(true));
                        break;
                    case dbOpe.Over:
                        string partitionBy = part.PartitionBy.Any() ? "PARTITION BY " + string.Join(", ", part.PartitionBy) + " " : "";
                        string orderBy = "ORDER BY " + string.Join(", ", part.OrderBy.Select(kv => kv.Key + " " + kv.Value.ToString().ToUpper()).ToArray());
                        expression.Append(part.Win.ToString().ToUpper() + "() OVER (" + partitionBy + orderBy + ")");
                        break;
                    case dbOpe.Qry:
                        expression.Append("( " + part.GetValue(true) + " )");
                        break;
                    default:
                        expression.Append(" " + base.dbOpeToString(part.Operation) + " " + part.GetValue(true));
                        break;
                }
            }
            return expression.ToString();
        }

        internal string GetBaseValue() {
            return particles[0].Value;
        }

        /// <summary>Crea una copia superficial de la expresión y sus particulas</summary>
        private Expression Clone() {
            List<Particle> clonedParticles = particles.ToArray().ToList();
            return new Expression(clonedParticles);
        }

        private Expression Op(Particle newParticle) {
            Expression clonedExpr = this.Clone();
            clonedExpr.particles.Add(newParticle);
            return clonedExpr;
        }

        /// <summary>Operador general</summary>
        private Expression Operate(dbOpe oper, Expression expr) {
            return Op(new Particle(oper, expr));
        }

        /// <summary>Operacion para case</summary>
        private Expression Operate(Comparison comp, Expression then) {
            return Op(new Particle(comp, then, false));
        }

        /// <summary>Operacion para expresiones lógicas</summary>
        private Expression Operate(dbLog log, Comparison comp) {
            return Op(new Particle(log, comp));
        }

        /// <summary>Operacion para expresiones lógicas compuestas</summary>
        private Expression Operate(dbLog log, Expression expr) {
            return Op(new Particle(log, expr));
        }

        /// <summary>Engloba la expresión en una funcion de sql</summary>
        public Expression Fun(dbFun funct, params string[] extraParams) {
            if (extraParams != null && extraParams.Length > 0) {
                foreach (string prm in extraParams) {
                    if (funct == dbFun.Cast) {
                        Operate(dbOpe.As, prm);
                    }
                    else {
                        Operate(dbOpe.Comma, prm);
                    }
                }
            }
            return new Expression(funct, this);
        }

        #region Arithmetic Operations
        public static Expression operator +(Expression expr1, Expression expr2) { return expr1.Operate(dbOpe.Addition, expr2); }
        public static Expression operator -(Expression expr1, Expression expr2) { return expr1.Operate(dbOpe.Substraction, expr2); }
        public static Expression operator *(Expression expr1, Expression expr2) { return expr1.Operate(dbOpe.Multiplication, expr2); }
        public static Expression operator /(Expression expr1, Expression expr2) { return expr1.Operate(dbOpe.Division, expr2); }
        public static Expression operator %(Expression expr1, Expression expr2) { return expr1.Operate(dbOpe.Modulo, expr2); }

        /// <summary>Gets the absolute value of the number</summary>
        public static Expression Abs(Expression value) {
            return value.Fun(dbFun.Abs);
        }
        #endregion

        #region BuiltIn Functions
        /// <summary>Creates a new GUID</summary>
        public static Expression NewId() {
            return new Expression("").Fun(dbFun.NewId);
        }

        /// <summary>Cantidad redondeada</summary>
        public static Expression Round(Expression value, int decimals) {
            value = value.Operate(dbOpe.Comma, decimals);
            return value.Fun(dbFun.Round);
        }

        /// <summary>Devuelve el primer valor no nulo del listado</summary>
        public static Expression Coalesce(params Expression[] values) {
            if (values != null && values.Length > 0) {
                for (int i = 1; i < values.Length; i++) {
                    values[0] = values[0].Operate(dbOpe.Comma, values[i]);
                }
                return values[0].Fun(dbFun.Coalesce);
            }
            return null;
        }

        /// <summary>Converts value into selected data type</summary>
        private static Expression Cast(Expression expr, dbTyp type, dbSiz size, params int[] ranges) {
            expr = expr.Operate(dbOpe.As, type.ToString().ToUpper());
            if (size == dbSiz.Max) {
                Expression inExpr = (Expression)dbSiz.Max.ToString().ToUpper();
                expr = expr.Operate(dbOpe.In, inExpr);
                return expr.Fun(dbFun.Cast);
            }
            else {
                if (ranges.Length > 0) {
                    Expression inExpr = (Expression)ranges[0];
                    for (int i = 1; i < ranges.Length; i++) {
                        inExpr = inExpr.Operate(dbOpe.Comma, ranges[i].ToString());
                    }
                    expr = expr.Operate(dbOpe.In, inExpr);
                }
            }
            return expr.Fun(dbFun.Cast);
        }

        /// <summary>Converts value into selected data type</summary>
        public static Expression Cast(Expression expr, dbTyp type, params int[] ranges) {
            return Cast(expr, type, dbSiz.Other, ranges);
        }

        /// <summary>Converts value into selected data type with max as size</summary>
        public static Expression CastMax(Expression expr, dbTyp type) {
            return Cast(expr, type, dbSiz.Max);
        }

        /// <summary>Converts value into selected data type</summary>
        public Expression Cast(dbTyp type, params int[] range) {
            return Expression.Cast(this, type, dbSiz.Other, range);
        }

        /// <summary>Converts value into selected data type with max as size</summary>
        public Expression CastMax(dbTyp type) {
            return Expression.Cast(this, type, dbSiz.Max);
        }

        /// <summary>Convierte el valor en el tipo indicado</summary>
        public static Expression Convert(dbTyp type, Expression expr, params int[] ranges) {
            Expression exprType = (Expression)type.ToString().ToUpper();
            if (ranges.Length > 0) {
                exprType = exprType.Operate(dbOpe.In, ranges[0].ToString());
            }
            exprType = exprType.Operate(dbOpe.Comma, expr);
            return exprType.Fun(dbFun.Convert);
        }

        /// <summary>Convierte el valor de tipo fecha en el tipo indicado</summary>
        public static Expression Convert(int style, dbTyp type, Expression expr, params int[] ranges) {
            Expression exprType = (Expression)type.ToString().ToUpper();
            if (ranges.Length > 0) {
                exprType = exprType.Operate(dbOpe.In, ranges[0].ToString());
            }
            exprType = exprType.Operate(dbOpe.Comma, expr);
            exprType = exprType.Operate(dbOpe.Comma, style);
            return exprType.Fun(dbFun.Convert);
        }

        /// <summary>Busca la expresion en dentro de una cadena iniciando en el caracter indicado</summary>
        public static Expression CharIndex(Expression find, Expression search, int startLocation) {
            find = find.Operate(dbOpe.Comma, search);
            find = find.Operate(dbOpe.Comma, startLocation);
            return find.Fun(dbFun.CharIndex);
        }

        /// <summary>Busca la expresion en dentro de una cadena iniciando en el caracter indicado</summary>
        public static Expression CharIndex(Expression find, Expression search) {
            find = find.Operate(dbOpe.Comma, search);
            return find.Fun(dbFun.CharIndex);
        }

        /// <summary>Devuelve la cantidad de caracteres indicados de la parte derecha de la cadena</summary>
        public static Expression Right(Expression value, int chars) {
            value = value.Operate(dbOpe.Comma, chars.ToString());
            return value.Fun(dbFun.Right);
        }

        /// <summary>Devuelve el segmento del texto indicado</summary>
        public static Expression SubString(Expression target, int start, int length) {
            target = target.Operate(dbOpe.Comma, start);
            target = target.Operate(dbOpe.Comma, length);
            return target.Fun(dbFun.SubString);
        }

        /// <summary>Returns current length of target string</summary>
        public static Expression Len(Expression target) {
            return target.Fun(dbFun.Len);
        }

        /// <summary>Returns current length of target string</summary>
        public Expression Len() {
            return Expression.Len(this);
        }

        /// <summary>Reemplaza un patron dentro de una cadena</summary>
        public static Expression Replace(Expression target, Expression pattern, Expression replacement) {
            target = target.Operate(dbOpe.Comma, pattern);
            target = target.Operate(dbOpe.Comma, replacement);
            return target.Fun(dbFun.Replace);
        }

        /// <summary>Inserta una cadena dentro de otra cadena</summary>
        public static Expression Stuff(Expression target, int start, int length, string replaceWith) {
            target = target.Operate(dbOpe.Comma, start);
            target = target.Operate(dbOpe.Comma, length);
            target = target.Operate(dbOpe.Comma, replaceWith);
            return target.Fun(dbFun.Stuff);
        }

        /// <summary>Adds a quantity to a component of a date</summary>
        public static Expression DateAdd(dbTim datepart, Expression quantity, Expression targetDate) {
            Expression expr = new Expression(datepart);
            expr = expr.Operate(dbOpe.Comma, quantity);
            expr = expr.Operate(dbOpe.Comma, targetDate);
            return expr.Fun(dbFun.DateAdd);
        }

        /// <summary>Devuelve la diferencia entre las dos fechas indicadas</summary>
        public static Expression DateDiff(dbTim datepart, Expression startDate, Expression endDate) {
            Expression expr = new Expression(datepart);
            expr = expr.Operate(dbOpe.Comma, startDate);
            expr = expr.Operate(dbOpe.Comma, endDate);
            return expr.Fun(dbFun.DateDiff);
        }

        /// <summary>Obtiene el componente anio de la fecha</summary>
        public static Expression Year(Expression date) {
            return date.Fun(dbFun.Year);
        }

        /// <summary>Obtiene el componente mes de la fecha</summary>
        public static Expression Month(Expression date) {
            return date.Fun(dbFun.Month);
        }

        /// <summary>Obtiene el componente dia la fecha</summary>
        public static Expression Day(Expression date) {
            return date.Fun(dbFun.Day);
        }

        /// <summary>Obtiene la fecha y hora al momento de su uso</summary>
        public static Expression GetDate() {
            return new Expression("").Fun(dbFun.GetDate);
        }

        /// <summary>Convierte la cadena a mayusculas</summary>
        public static Expression Upper(Expression strToUpper) {
            return strToUpper.Fun(dbFun.Upper);
        }

        /// <summary>Convierte la cadena a mayusculas</summary>
        public static Expression Lower(Expression strToUpper) {
            return strToUpper.Fun(dbFun.Lower);
        }

        /// <summary>Checks if the string can be converted to a number</summary>
        public static Expression IsNumeric(Expression text) {
            return text.Fun(dbFun.IsNumeric);
        }
        #endregion

        #region Logical Expressions
        private static Comparison Log(dbLog log, dbCom comp, Expression val, params Expression[] values) {
            Comparison whr = new Comparison();
            whr.comparator = val.ToString();
            whr.type = comp;
            whr.values = values.Select(v => v.ToString()).ToList();
            return whr;
        }

        /// <summary>Expresión lógica que permite anidar otras expresiones</summary>
        public static Expression Where(Expression expr) {
            Comparison whr = new Comparison();
            whr.expr = expr;
            return new Expression(dbLog.Where, whr);
        }

        /// <summary>Expresión lógica que permite uso de OR</summary>
        public static Expression Where(dbCom comp, Expression val, params Expression[] values) {
            Comparison whr = Log(dbLog.Where, comp, val, values);
            return new Expression(dbLog.Where, whr);
        }

        /// <summary>Expresión lógica de equidad que permite uso de OR</summary>
        public static Expression Where(Expression val, params Expression[] values) {
            return Where(dbCom.Equals, val, values);
        }

        /// <summary>Expresión lógica que permite uso de OR y agrega comillas en caso de ser requeridas</summary>
        public static Expression WhereVal(Expression val, string value) {
            Expression exp = (Expression)AddSingleQuotesIfMissing(value); 
            Comparison whr = Log(dbLog.Where, dbCom.Equals, val, exp);
            return new Expression(dbLog.Where, whr);
        }

        /// <summary>Expresión lógica que permite uso de OR y agrega comillas en caso de ser requeridas</summary>
        public static Expression WhereVal(dbCom comp, Expression val, params string[] values) {
            Expression[] quotedValues = values.Select(v => (Expression)AddSingleQuotesIfMissing(v)).ToArray();
            Comparison whr = Log(dbLog.Where, comp, val, quotedValues);
            return new Expression(dbLog.Where, whr);
        }

        /// <summary>And para expresión lógica</summary>
        public Expression And(dbCom comp, Expression val, params Expression[] values) {
            Comparison whr = Log(dbLog.And, comp, val, values);
            return this.Operate(dbLog.And, whr);
        }

        /// <summary>And para expresión lógica de equidad</summary>
        public Expression And(Expression val, params Expression[] values) {
            return And(dbCom.Equals, val, values);
        }

        /// <summary>And para expresión lógica agregando comillas en caso de ser requeridas</summary>
        public Expression AndVal(dbCom comp, Expression val, params string[] values) {
            Expression[] quotedValues = values.Select(v => (Expression)AddSingleQuotesIfMissing(v)).ToArray();
            Comparison whr = Log(dbLog.And, comp, val, quotedValues);
            return this.Operate(dbLog.And, whr);
        }

        /// <summary>And para expresión lógica de equidad agregando comillas en caso de ser requeridas</summary>
        public Expression AndVal(Expression val, params string[] values) {
            return AndVal(dbCom.Equals, val, values);
        }

        /// <summary>Or para expresión lógica</summary>
        public Expression Or(dbCom comp, Expression val, params Expression[] values) {
            Comparison whr = Log(dbLog.And, comp, val, values);
            return this.Operate(dbLog.Or, whr);
        }

        /// <summary>Or para expresión lógica de equidad</summary>
        public Expression Or(Expression val, params Expression[] values) {
            if (values == null || values.Length == 0) {
                return this.Operate(dbLog.Or, val);
            }
            else {
                return Or(dbCom.Equals, val, values);
            }
        }

        /// <summary>Or para expresión lógica a partir de otra expresión lógica</summary>
        public Expression Or(Expression expr) {
            return this.Operate(dbLog.Or, expr);
        }

        /// <summary>Or para expresión lógica agregando comillas en caso de ser requeridas</summary>
        public Expression OrVal(dbCom comp, Expression val, params string[] values) {
            Expression[] quotedValues = values.Select(v => (Expression)AddSingleQuotesIfMissing(v)).ToArray();
            Comparison whr = Log(dbLog.And, comp, val, quotedValues);
            return this.Operate(dbLog.Or, whr);
        }

        /// <summary>Or para expresión lógica de equidad agregando comillas en caso de ser requeridas</summary>
        public Expression OrVal(Expression val, params string[] values) {
            if (values == null || values.Length == 0) {
                return this.Operate(dbLog.Or, val);
            }
            else {
                return OrVal(dbCom.Equals, val, values);
            }
        }

        #endregion

        #region Case Flow Control
        /// <summary>Inicia una estructura de control de flujo case</summary>
        private static Expression CaseWhen(Expression then, bool isRoot, Constructor.dbCom comp, Expression cond, params Expression[] values) {
            Comparison whr = new Comparison();
            if (cond.IsComplex) {
                whr.expr = cond;
            }
            whr.comparator = cond.ToString();
            whr.type = comp;
            whr.values = values.Select(v => v.ToString()).ToList();

            return new Expression(whr, then, isRoot);
        }

        /// <summary>Inicia una estructura de control de flujo case</summary>
        public static Expression CaseWhen(Expression then, Constructor.dbCom comp, Expression cond, params Expression[] values) {
            return CaseWhen(then, true, comp, cond, values);
        }

        /// <summary>Inicia una estructura de control de flujo case</summary>
        public static Expression CaseWhen(Expression then, Expression cond, params Expression[] values) {
            return CaseWhen(then, Constructor.dbCom.Equals, cond, values);
        }

        /// <summary>Agrega una condicion a una estructura de control case</summary>
        public Expression When(Expression then, Constructor.dbCom comp, Expression cond, params Expression[] values) {
            Comparison whr = new Comparison();
            if (cond.IsComplex) {
                whr.expr = cond;
            }
            whr.comparator = cond.ToString();
            whr.type = comp;
            whr.values = values.Select(v => v.ToString()).ToList();

            return this.Operate(whr, then);
        }

        /// <summary>Agrega una condicion a una estructura de control case</summary>
        public Expression When(Expression then, Expression cond, params Expression[] values) {
            return When(then, Constructor.dbCom.Equals, cond, values);
        }

        /// <summary>Agrega una condicion por defecto a una estructura de control case</summary>
        public Expression Else(Expression value) {
            return this.Operate(dbOpe.Else, value);
        }
        #endregion

        #region Aggregate Functions
        /// <summary>Sumatoria</summary>
        public static Expression Sum(Expression expr) {
            return new Expression(dbAgr.sum, expr);
        }

        /// <summary>Conteo</summary>
        public static Expression Count(Expression expr) {
            return new Expression(dbAgr.count, expr);
        }

        /// <summary>Máximo</summary>
        public static Expression Max(Expression expr) {
            return new Expression(dbAgr.max, expr);
        }

        /// <summary>Mínimo</summary>
        public static Expression Min(Expression expr) {
            return new Expression(dbAgr.min, expr);
        }

        /// <summary>Promedio</summary>
        public static Expression Avg(Expression expr) {
            return new Expression(dbAgr.avg, expr);
        }
        #endregion

        #region Window Functions
        public static Expression Over(Constructor.dbWin func, string[] orderBy) {
            return new Expression(dbWTy.win, default(dbAgr), func, "NULL", orderBy.ToDictionary(s => s, s => dbOrd.Asc), new string[] { });
        }

        public static Expression Over(Constructor.dbWin func, string[] orderBy, string[] partitionBy) {
            return new Expression(dbWTy.win, default(dbAgr), func, "NULL", orderBy.ToDictionary(s => s, s => dbOrd.Asc), partitionBy);
        }

        //public static Expression Over(Constructor.dbAgr aggregate, Expression expr, string[] orderBy) {
        //    return null;
        //}

        //public static Expression Over(Constructor.dbAgr aggregate, Expression expr, string[] orderBy, string[] partitionBy) {
        //    return null;
        //}

        public static Expression Over(Constructor.dbWin func, Dictionary<string, Constructor.dbOrd> orderBy) {
            return new Expression(dbWTy.win, default(dbAgr), func, "NULL", orderBy, new string[] { });
        }

        public static Expression Over(Constructor.dbWin func, Dictionary<string, Constructor.dbOrd> orderBy, string[] partitionBy) {
            return new Expression(dbWTy.win, default(dbAgr), func, "NULL", orderBy, partitionBy);
        }
        #endregion

        #region Bitwise Operators
        public static Expression operator &(Expression expr1, Expression expr2) { return expr1.Operate(dbOpe.BitAnd, expr2); }
        public static Expression operator |(Expression expr1, Expression expr2) { return expr1.Operate(dbOpe.BitOr, expr2); }
        public static Expression operator ^(Expression expr1, Expression expr2) { return expr1.Operate(dbOpe.BitXOr, expr2); }
        #endregion
    }
}
