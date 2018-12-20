using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Data;

namespace DataFramework {
    public class Constructor : IQryable {
        #region Enumeradores

        /// <summary>Listado de instrucciones a ejecutar</summary>
        public enum dbItr { noQuery, sProc, sel, ins, upd, del, mer, fld };

        /// <summary>Tipos de Comparaciones</summary>
        public enum dbCom { Equals, MoreThan, LessThan, MoreThanOrEquals, LessThanOrEquals, DifferentFrom, Iss, IssNot, IssIn, IssNotIn, Like, NotLike };

        /// <summary>Tipos de uniones de tablas</summary>
        public enum dbJoi { Inner, Left, Right, Full, Cross };

        /// <summary>Tipos de funciones agregadas</summary>
        public enum dbAgr { count, countBig, min, max, sum, avg /*7 mas*/};

        /// <summary>Tipo de funcion de ventana</summary>
        public enum dbWTy { agg, win };

        /// <summary>Funciones de ventana</summary>
        public enum dbWin { row_number, rank };

        /// <summary>Tipo de ordenamiento</summary>
        public enum dbOrd { Asc, Desc };

        /// <summary>Tipo de Union</summary>
        public enum dbUni { NotUnited, Union, UnionAll };

        /// <summary>Operadores para expresiones</summary>
        public enum dbOpe { NoOp, Funct, Agg, Over, Qry, Addition, Substraction, Multiplication, Division, Modulo, Case, Else, Log, Comma, As, In }

        /// <summary>Funciones de base expresiones</summary>
        public enum dbFun { Round, Coalesce, Cast, Convert, CharIndex, Left, Mid, Right, Len, SubString, Replace, Stuff, DateDiff, Year, Month, Day, GetDate, Upper, Lower }

        /// <summary>Operadores Logicos</summary>
        public enum dbLog { Where, And, Or }

        /// <summary>Tipos de datos</summary>
        public enum dbTyp { Int, Varchar, Date, DateTime, Decimal, Bit }

        /// <summary>Intervalos de tiempo</summary>
        public enum dbTim { Year, Quarter, Month, DayOfYear, Day, Week, Hour, Minute, Second, Millisecond, Microsecond, Nanosecond }

        /// <summary>Acciones a realizar en merge</summary>
        public enum dbMrA { Insert, Update, Delete }

        /// <summary>Data origin to compare in merge when</summary>
        public enum dbMby { None, Source, Target }

        /// <summary>Tipos de output en una consulta</summary>
        public enum dbOut { Undefined, Inserted, Deleted }

        /// <summary>Metodo de serialización de resultados a xml</summary>
        public enum dbXml { Raw, Auto, Explicit, Path }

        #endregion

        #region SubClases

        /// <summary>Procedimiento Almacenado</summary>
        protected internal class StoredProcedure {
            public string name { get; set; }
            public string schema { get; set; }
            public string database { get; set; }
        }

        /// <summary>Elemento del listado de campos</summary>
        protected internal class Field {
            public Field(string fieldName) {
                name = fieldName;
            }

            public Field(string fieldAlias, Query query) {
                name = query.ToString();
                if (query.instruction == dbItr.sel) {
                    name = "( " + name + " )";
                }
                nameAlias = fieldAlias;
                subQuery = query;
            }

            public Field(string fieldAlias, Expression fieldExpr) {
                name = fieldExpr.ToString();
                nameAlias = fieldAlias;
                expression = fieldExpr;
            }

            public string name;
            public string nameAlias;
            public dbAgr aggregate;
            public string valueAgg;
            public Query subQuery;
            public Expression expression;
            public Window window;
        }

        /// <summary>Elemento del listado de tablas</summary>
        protected internal class Table {
            public Table(string tableName) {
                table = tableName;
            }

            public string tableAlias;
            public string table;
            public string schema;
            public string database;

            public override string ToString() {
                return (!string.IsNullOrEmpty(database) ? database + "." : "")
                    + (!string.IsNullOrEmpty(schema) ? schema + "." : (!string.IsNullOrEmpty(database) ? "." : ""))
                    + table;
            }
        }

        /// <summary>Representa la salida de informacion del query principal</summary>
        protected internal class OutputClause {
            public dbOut type { get; set; }
            public List<Field> columns = new List<Field>();
            public string table { get; set; }

            public override string ToString() {
                if (string.IsNullOrEmpty(table)) {
                    return "";
                }
                StringBuilder outp = new StringBuilder();
                string outputOrigin = type != dbOut.Undefined ? type.ToString().ToUpper() + "." : "";
                string[] fields = columns
                    .Select(c => (string.IsNullOrEmpty(c.nameAlias) ? outputOrigin : c.nameAlias + ".") + c.name)
                    .ToArray();
                outp.Append(" OUTPUT ");
                outp.Append(string.Join(", ", fields));
                outp.Append(" INTO " + table);
                return outp.ToString();
            }
        }

        protected internal class JoinTable {
            public string tableAlias;
            public dbJoi type;
        }

        /// <summary>Elemento del listado de comparaciones</summary>
        protected internal class Comparison {
            public string comparator { get; set; }
            public List<string> values;
            public dbCom type;
            public Expression expr;
            public string owner { get; set; }
        }

        /// <summary>Elemento del listado de funciones agregadas</summary>
        protected internal class Aggregate {
            public Aggregate(dbAgr aggregateFunct, Field insideField, string alias) {
                funct = aggregateFunct;
                field = insideField;
                fieldAlias = alias;
            }

            public dbAgr funct;
            public Field field;
            public string fieldAlias;
        }

        /// <summary>Estructura de consulta de uso repetido</summary>
        protected internal class CommonTableExpression {
            public string alias;
            public Query origin;
        }

        /// <summary>Clausula de transformación de resultados a xml</summary>
        protected internal class ForXMLClause {
            public dbXml mode;
            public string element;
        }

        /// <summary>Elemento de cada parte de las uniones</summary>
        protected internal class UnionSelect {
            public dbUni type;
            public int topCount;
            protected internal List<Field> lstFields;
            protected internal List<Aggregate> lstAggFields;
            protected internal List<Table> lstFrom;
            protected internal List<JoinTable> lstJoin;
            protected internal List<Comparison> lstJoinOn;
            protected internal List<Comparison> lstWhere;
            protected internal List<Expression> lstGroupBy;
            protected internal List<Comparison> lstHaving;
            protected internal ForXMLClause forXml;

            internal UnionSelect() {
                lstFields = new List<Field>();
                lstAggFields = new List<Aggregate>();
                lstFrom = new List<Table>();
                lstJoin = new List<JoinTable>();
                lstJoinOn = new List<Comparison>();
                lstWhere = new List<Comparison>();
                lstGroupBy = new List<Expression>();
                lstHaving = new List<Comparison>();
            }

            internal UnionSelect(Query qry) {
                lstFields = qry.curUnion.lstFields;
                lstAggFields = qry.curUnion.lstAggFields;
                lstFrom = qry.curUnion.lstFrom;
                lstJoin = qry.curUnion.lstJoin;
                lstJoinOn = qry.curUnion.lstJoinOn;
                lstWhere = qry.curUnion.lstWhere;
                lstGroupBy = qry.curUnion.lstGroupBy;
                lstHaving = qry.curUnion.lstHaving;
                forXml = qry.curUnion.forXml;
            }
        }

        #endregion

        protected internal List<UnionSelect> lstUnion = new List<UnionSelect>();

        protected dbItr instruction;
        protected OutputClause output = new OutputClause();
        protected StoredProcedure stProc = new StoredProcedure();
        protected List<Expression> lstParams = new List<Expression>();
        protected DataTable insFields = new DataTable();
        protected Query insQuery;
        protected Table updateFrom;
        protected List<string[]> lstSet = new List<string[]>();
        protected List<string> lstOrderBy = new List<string>();
        protected CommonTableExpression cte = new CommonTableExpression();
        internal Merger merge;

        protected internal UnionSelect curUnion {
            get { return lstUnion.Last(); }
        }

        public override string ToString() {
            return GetSQLText();
        }

        /// <summary>Construye la cadena de texto que constituye el query</summary>
        private string GetSQLText() {
            string sqlQuery = "";
            switch (instruction) {
                case dbItr.noQuery:
                    sqlQuery = "";
                    break;

                case dbItr.sProc:
                    //sqlQuery += "EXECUTE " + storedProcedure + " " + string.Join(", ", lstParams.ToArray());
                    string spParams = string.Join(", ", lstParams.Select(e => e.ToString()).ToArray());

                    sqlQuery += "EXECUTE ";
                    sqlQuery += string.IsNullOrEmpty(stProc.database) ? "" : stProc.database + ".";
                    sqlQuery += string.IsNullOrEmpty(stProc.schema) ? (string.IsNullOrEmpty(stProc.database) ? "" : ".") : stProc.schema + ".";
                    sqlQuery += stProc.name;
                    sqlQuery += string.IsNullOrEmpty(spParams) ? "" : " " + spParams;
                    break;

                case dbItr.sel:
                    sqlQuery += string.IsNullOrEmpty(cte.alias) ? "" : ("WITH " + cte.alias + " AS ( " + cte.origin.ToString() + " ) ");
                    for (int u = 0; u < lstUnion.Count; u++) {
                        List<Field> fieldsWithAgg = lstUnion[u].lstFields.ToArray().ToList();

                        foreach (Aggregate agg in lstUnion[u].lstAggFields) {
                            Field agF = new Field(agg.funct.ToString().ToUpper() + "( " + agg.field.name + " )");
                            agF.nameAlias = agg.fieldAlias;
                            fieldsWithAgg.Add(agF);
                        }

                        List<string> lstStrFields = new List<string>();
                        foreach (Field fld in fieldsWithAgg) {
                            lstStrFields.Add(fld.name + (fld.nameAlias == "" ? "" : " AS " + fld.nameAlias));
                        }

                        sqlQuery += "SELECT";
                        sqlQuery += lstUnion[u].topCount > 0 ? " TOP " + lstUnion[u].topCount.ToString() : "";
                        sqlQuery += ConcatList("", " ", lstStrFields, ",");
                        sqlQuery += lstUnion[u].lstFrom.Count > 0 ? " FROM" + ConcatJoin(lstUnion[u]) : "";
                        sqlQuery += ConcatWhere(lstUnion[u].lstWhere);
                        sqlQuery += ConcatList(" GROUP BY", " ", lstUnion[u].lstGroupBy.Select(g => g.ToString()), ",");
                        sqlQuery += ConcatWhere(" HAVING", lstUnion[u].lstHaving);
                        sqlQuery += dbUniToString(lstUnion[u].type);

                        if (lstUnion[u].type == dbUni.NotUnited) { break; }
                    }

                    sqlQuery += ConcatList(" ORDER BY", " ", lstOrderBy, ",");
                    sqlQuery += curUnion.forXml != null ? " FOR XML " + curUnion.forXml.mode.ToString().ToUpper() + (curUnion.forXml.element != null ? " (" + curUnion.forXml.element + ")" : "") : "";

                    break;

                case dbItr.ins:
                    sqlQuery += "INSERT INTO "
                        + (string.IsNullOrEmpty(lstUnion[0].lstFrom[0].database) ? "" : lstUnion[0].lstFrom[0].database + ".")
                        + (string.IsNullOrEmpty(lstUnion[0].lstFrom[0].schema) ? "" : lstUnion[0].lstFrom[0].schema + ".")
                        + lstUnion[0].lstFrom[0].table;
                    if (insQuery != null) {
                        IEnumerable<string> lstDestFields = insQuery.curUnion.lstFields.Select(f => string.IsNullOrEmpty(f.nameAlias) ? f.name : f.nameAlias);
                        sqlQuery += ConcatList(" (", " ", lstDestFields, ",") + " )";
                        sqlQuery += output.ToString();
                        sqlQuery += " " + insQuery.ToString();
                    }
                    else {
                        List<string> lstDestFields = new List<string>();
                        foreach (DataColumn dest in insFields.Columns) {
                            lstDestFields.Add(dest.ColumnName);
                        }
                        sqlQuery += ConcatList(" (", " ", lstDestFields, ",") + " )";
                        sqlQuery += output.ToString();
                        foreach (DataRow row in insFields.Rows) {
                            List<string> lstOrigFields = new List<string>();
                            foreach (DataColumn col in row.Table.Columns) {
                                lstOrigFields.Add(row[col.ColumnName] == DBNull.Value ? "NULL" : ((Expression)row[col.ColumnName]).ToString());
                            }
                            sqlQuery += ConcatList(" SELECT", " ", lstOrigFields, ",") + " UNION ALL";
                        }
                        sqlQuery = sqlQuery.Remove(sqlQuery.Length - " UNION ALL".Length);
                    }

                    break;

                case dbItr.upd:
                    List<string> lstStrSet = new List<string>();
                    foreach (string[] arrS in lstSet) {
                        lstStrSet.Add(arrS[0] + " = " + arrS[1]);
                    }

                    sqlQuery += "UPDATE "
                        + (string.IsNullOrEmpty(updateFrom.database) ? "" : updateFrom.database + ".")
                        + (string.IsNullOrEmpty(updateFrom.schema) ? "" : updateFrom.schema + ".")
                        + updateFrom.table;
                    sqlQuery += ConcatList(" SET", " ", lstStrSet, ",");
                    sqlQuery += output.ToString();
                    sqlQuery += lstUnion[0].lstFrom.Count > 0 ? " FROM" + ConcatJoin(lstUnion[0]) : "";
                    sqlQuery += ConcatWhere(lstUnion[0].lstWhere);

                    break;

                case dbItr.del:
                    sqlQuery += "DELETE FROM "
                        + (string.IsNullOrEmpty(lstUnion[0].lstFrom[0].database) ? "" : lstUnion[0].lstFrom[0].database + ".")
                        + (string.IsNullOrEmpty(lstUnion[0].lstFrom[0].schema) ? "" : lstUnion[0].lstFrom[0].schema + ".")
                        + lstUnion[0].lstFrom[0].table;
                    sqlQuery += ConcatWhere(lstUnion[0].lstWhere);

                    break;

                case dbItr.mer:
                    sqlQuery += ";MERGE";
                    sqlQuery += " " + merge.Destiny.ToString() + " AS " + merge.Destiny.Alias;
                    sqlQuery += " USING " + merge.Origin.ToString() + " AS " + merge.Origin.Alias;
                    sqlQuery += ConcatWhere(" ON", merge.Keys);
                    foreach (Merger.MergerAction act in merge.Actions) {
                        sqlQuery += " WHEN " + (act.Matched ? "MATCHED" : "NOT MATCHED");
                        sqlQuery += (act.By == dbMby.None ? "" : " BY " + act.By.ToString().ToUpper());
                        sqlQuery += (act.Conditions.Count > 0 ? ConcatWhere(" AND", act.Conditions) : "") + " THEN";
                        sqlQuery += " " + act.Action.ToString().ToUpper();
                        switch (act.Action) {
                            case dbMrA.Insert:
                                sqlQuery += ConcatList(" (", " ", act.Values.Keys, ",") + " )";
                                sqlQuery += " VALUES" + ConcatList(" (", " ", act.Values.Keys.Select(k => merge.Origin.Alias + "." + k), ",") + " )";
                                sqlQuery += act.outputClause.ToString();
                                break;
                            case dbMrA.Update:
                                sqlQuery += ConcatList(" SET", " ", act.Values.Keys.Select(k => merge.Destiny.Alias + "." + k + " = " + merge.Origin.Alias + "." + k), ",");
                                break;
                            case dbMrA.Delete:
                                break;
                        }
                    }
                    sqlQuery += ";";
                    break;

                case dbItr.fld:
                    Field fldSel = curUnion.lstFields[0];
                    sqlQuery += fldSel.name;

                    break;
            }
            return sqlQuery;
        }

        /// <summary>Concatena precedingStr y divider antes y despues de cada elemento en lstStrVal y antepone start al resultado</summary>
        private string ConcatList(string start, string precedingStr, IEnumerable<string> lstStrVal, string divider) {
            List<string> lstQuery = new List<string>();
            if (lstStrVal.Any()) {
                foreach (string strC in lstStrVal) {
                    lstQuery.Add(precedingStr + strC);
                }
                return start + string.Join(divider, lstQuery.ToArray());
            }
            return "";
        }

        /// <summary>Concatena el listado de condiciones para la clausula where</summary>
        private string ConcatWhere(List<Comparison> lstStrWhr) {
            return ConcatWhere(" WHERE", lstStrWhr);
        }

        /// <summary>Concatena el listado de condiciones para la clausula where</summary>
        private string ConcatWhere(string startingWord, List<Comparison> lstStrWhr) {
            List<string> lstStrWhere = new List<string>();
            foreach (Comparison arrW in lstStrWhr) {
                lstStrWhere.Add(EvalComparison(arrW));
            }
            return ConcatList(startingWord, " ", lstStrWhere, " AND");
        }

        /// <summary>Obtiene la representación textual de una comparación lógica</summary>
        protected string EvalComparison(Comparison comp) {
            string whr;
            //valor a la izquierda del comparador
            whr = comp.comparator + " ";
            if (comp.expr == null) {
                //comparador
                whr += dbComToString(comp.type);
                //apertura parentesis
                whr += (comp.type == dbCom.IssIn || comp.type == dbCom.IssNotIn) ? " (" : "";
                //valor(es) a la derecha del comparador
                whr += ConcatList("", " ", comp.values, ",");
                //cierre parentesis
                whr += (comp.type == dbCom.IssIn || comp.type == dbCom.IssNotIn) ? " )" : "";
            }
            else {
                whr = "( " + comp.expr.ToString() + " )";
            }
            //agregar al listado
            return whr;
        }

        /// <summary>Concatena la estructura de tablas para la clausula from</summary>
        private string ConcatJoin(UnionSelect union) {
            StringBuilder sqlQuery = new StringBuilder();
            bool isFirstTable = true;
            bool joinFound;
            string onWhere = "";
            foreach (Table tbl in union.lstFrom) {
                //Si no es la primera tabla del from, agrega el tipo de union con la previa
                if (!isFirstTable) {
                    joinFound = false;
                    List<JoinTable> lstJoin = union.lstJoin.Select(j => j).ToList(); //Clonar lista
                    for (int iAls = 0; iAls < lstJoin.Count; iAls++) {
                        if (tbl.tableAlias == lstJoin[iAls].tableAlias) {
                            sqlQuery.Append(" " + lstJoin[iAls].type.ToString().ToUpper() + " JOIN");
                            lstJoin.RemoveAt(iAls);
                            joinFound = true;
                            break;
                        }
                    }
                    if (!joinFound) {
                        sqlQuery.Append(",");
                    }
                }
                //Agrega la tabla y su alias en caso de tener
                sqlQuery.Append(" " + (tbl.database == "" ? "" : tbl.database + ".") + (tbl.schema == "" ? "" : tbl.schema + ".") + tbl.table + (tbl.tableAlias == "" ? "" : " AS " + tbl.tableAlias));
                //Si no es la primera tabla del from, agrega las condiciones de union con las previas
                if (!isFirstTable) {
                    onWhere = "";
                    List<Comparison> lstJoinOn = new List<Comparison>(union.lstJoinOn);
                    string[] condicionesJoin = lstJoinOn
                        .Where(j => j.owner == tbl.tableAlias)
                        .Select(j => EvalComparison(j)).ToArray();
                    JoinTable currentJoin = union.lstJoin.First(j => j.tableAlias == tbl.tableAlias);
                    if (currentJoin.type != dbJoi.Cross) {
                        onWhere += " ON " + string.Join(" AND ", condicionesJoin);
                    }

                    //onWhere = "";
                    ////Se revisan todas las anteriores tablas agregadas hasta llegar a la actual
                    //foreach (Table prevTbl in union.lstFrom)
                    //{
                    //    if (prevTbl.tableAlias == tbl.tableAlias)
                    //    {
                    //        break;
                    //    }
                    //    iCnd = 0;
                    //    //Ciclo para control de condicion de repeticion
                    //    while (iCnd <= union.lstJoinOn.Count - 1)
                    //    {
                    //        //Si ambas partes de la condicion son campos
                    //        if (union.lstJoinOn[iCnd].expr == null && (Regex.IsMatch(union.lstJoinOn[iCnd].comparator, ".+[.].+") && Regex.IsMatch(union.lstJoinOn[iCnd].values[0], ".+[.].+")))
                    //        {
                    //            Regex rgxCurTable = new Regex("^" + tbl.tableAlias + "[.].+");
                    //            Regex rgxPreTable = new Regex("^" + prevTbl.tableAlias + "[.].+");
                    //            //Si la condicion esta relacionada a esta tabla y la otra condicion a la tabla anterior
                    //            if ((rgxCurTable.IsMatch(union.lstJoinOn[iCnd].comparator) || rgxCurTable.IsMatch(union.lstJoinOn[iCnd].values[0])) && (rgxPreTable.IsMatch(union.lstJoinOn[iCnd].comparator) || rgxPreTable.IsMatch(union.lstJoinOn[iCnd].values[0])))
                    //            {
                    //                //Agregar condicion al where
                    //                onWhere += (onWhere == "") ? " ON" : " AND";
                    //                onWhere += " " + union.lstJoinOn[iCnd].comparator + " " + dbComToString(union.lstJoinOn[iCnd].type) + " " + union.lstJoinOn[iCnd].values[0];
                    //                //Remover condicion para que ya no aparezca en el where y ajustar el control de ciclo
                    //                union.lstJoinOn.RemoveAt(iCnd);
                    //                iCnd--;
                    //            }
                    //        }
                    //        iCnd++;
                    //    }
                    //}
                }
                sqlQuery.Append(onWhere);
                isFirstTable = false;
            }
            return sqlQuery.ToString();
        }

        /// <summary>Devuelve la cadena que representa el valor del objeto y agrega comillas a los textos del objeto si ya son de tipo cadena</summary>
        protected string FormatVarchar(Object value) {
            if (value.GetType() == typeof(String)) {
                return "'" + value.ToString() + "'";
            }
            return value.ToString();
        }

        /// <summary>Convierte el objeto a expression dependiendo de su tipo</summary>
        protected Expression ObjectToExpression(object o) {
            if (o == null) { return (Expression)"NULL"; }
            if (o is string) { return (Expression)(string)o; }
            if (o is int) { return (Expression)(int)o; }
            if (o is int?) { return (Expression)((int?)o).Value; }
            if (o is long) { return (Expression)(long)o; }
            if (o is decimal) { return (Expression)(decimal)o; }
            if (o is float) { return (Expression)(float)o; }
            if (o is double) { return (Expression)(double)o; }
            if (o is bool) { return (Expression)(bool)o; }
            if (o is DateTime) { return (Expression)(DateTime)o; }
            if (o is DateTime?) { return (Expression)((DateTime?)o).Value; }
            if (o is Query) { return new Expression((Query)o); }
            if (o is Enum) { return (Expression)Convert.ToInt32(o); }
            if (o is Expression) { return (Expression)o; }
            if (o is DBNull) { return (Expression)DBNull.Value; }
            return (Expression)o.ToString();
        }

        /// <summary>Agrega comillas a la cadena en caso de que esta no posea comillas al principio y al final</summary>
        protected static string AddSingleQuotesIfMissing(string s) {
            Regex rgxApos = new Regex("^'([^']|'')*'$");
            if (!rgxApos.IsMatch(s)) {
                return "N'" + s + "'";
            }
            return s;
        }

        /// <summary>Devuelve los caracteres específicos para la comparación de base de datos</summary>
        private string dbComToString(dbCom Com) {
            switch (Com) {
                case dbCom.Equals:
                    return "=";
                case dbCom.MoreThan:
                    return ">";
                case dbCom.LessThan:
                    return "<";
                case dbCom.MoreThanOrEquals:
                    return ">=";
                case dbCom.LessThanOrEquals:
                    return "<=";
                case dbCom.DifferentFrom:
                    return "<>";
                case dbCom.Iss:
                    return "IS";
                case dbCom.IssNot:
                    return "IS NOT";
                case dbCom.IssIn:
                    return "IN";
                case dbCom.IssNotIn:
                    return "NOT IN";
                case dbCom.Like:
                    return "LIKE";
                case dbCom.NotLike:
                    return "NOT LIKE";
            }
            return "";
        }

        /// <summary>Devuelve la palabra reservada especifica para el tipo de union</summary>
        private string dbUniToString(dbUni Uni) {
            switch (Uni) {
                case dbUni.Union:
                    return " UNION ";
                case dbUni.UnionAll:
                    return " UNION ALL ";
            }
            return "";
        }

        /// <summary>Devuelve los caracteres especificos para la operación solicitada</summary>
        protected string dbOpeToString(dbOpe oper) {
            switch (oper) {
                case dbOpe.Addition:
                    return "+";
                case dbOpe.Substraction:
                    return "-";
                case dbOpe.Multiplication:
                    return "*";
                case dbOpe.Division:
                    return "/";
                case dbOpe.Modulo:
                    return "%";
                case dbOpe.Case:
                    return "WHEN";
                case dbOpe.Else:
                    return "ELSE";
                case dbOpe.Comma:
                    return ",";
                case dbOpe.As:
                    return "AS";
            }
            return "";
        }

        /// <summary>Devuelve los caracteres especificos para la operación lógica solicitada</summary>
        protected string dbLogToString(dbLog log) {
            switch (log) {
                case dbLog.And:
                    return "AND";
                case dbLog.Or:
                    return "OR";
                case dbLog.Where:
                    return "";
            }
            return "";
        }

        #region Seguridad

        /// <summary>Limpia el elemento enviado para prevencion de inyeccion de SQL</summary>
        protected string SanitizeSQL(string strSQL) {
            if (strSQL == null) {
                strSQL = "NULL";
            }

            List<string> validaciones = new List<string>();
            string validSql = "";

            //Cadenas reservadas permitidas por SQL
            string[] reserved = { "NULL" };
            foreach (string rw in reserved) {
                if (strSQL == rw) {
                    return strSQL;
                }
            }

            //Cadenas vacias
            validaciones.Add("");
            //Comillas a principio y fin y sin comillas intermedias
            validaciones.Add("(N?'([^']|'')*')");
            //Corchetes al principio y fin sin corchetes intermedios
            validaciones.Add(@"\[[^\[\]]+\]");
            //Iniciando una letra y despues letras, numeros o guion bajo, un punto que puede o no estar y despues letras, numeros o guion bajo
            validaciones.Add("((@|[a-zñA-ZÑ])[a-zñA-ZÑ0-9_]*([.][a-zñA-ZÑ][a-zñA-ZÑ0-9_]*)?)");
            //Numeros con o sin signo y con o sin punto (obligatorio numero a la izquierda del punto)
            validaciones.Add("([-+]?[0-9]+[.]?[0-9]*)");
            //Numeros con o sin signo y con o sin punto (obligatorio numero a la derecha del punto)
            validaciones.Add("([-+]?[0-9]*[.]?[0-9]+)");

            //Concatenar validaciones en un solo regex
            foreach (string val in validaciones) {
                validSql += "^" + val + "$|";
            }
            validSql = validSql.Remove(validSql.Length - 1);

            if (!Regex.IsMatch(strSQL, validSql)) {
                throw new System.Security.SecurityException("invalid sql input: \"" + strSQL + "\"");
            }
            return strSQL;
        }

        /// <summary>Limpia el arreglo de elementos enviado para prevencion de inyeccion de SQL</summary>
        protected string[] SanitizeSQL(string[] arrStrSQL) {
            for (int iSQL = 0; iSQL < arrStrSQL.Length; iSQL++) {
                arrStrSQL[iSQL] = SanitizeSQL(arrStrSQL[iSQL]);
            }
            return arrStrSQL;
        }

        #endregion
    }
}
