using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Data;

namespace DataFramework {
    /// <summary>Constructor de CommandText</summary>
    public class Query : Constructor {
        public Query() {
            lstUnion.Add(new UnionSelect());
        }

        public override string ToString() {
            return base.ToString();
        }

        /// <summary>Prepara la consulta actual para realizar una union</summary>
        public Query Union(dbUni type) {
            curUnion.type = type;
            lstUnion.Add(new UnionSelect());
            return this;
        }

        /// <summary>Realiza la union de la consulta actual con la proveida</summary>
        public Query Union(dbUni type, Query qry) {
            curUnion.type = type;
            lstUnion.Add(new UnionSelect(qry));
            return this;
        }

        #region Metodos Preparadores
        /// <summary>Prepara el query para que al ser ejecutado no se envie nada al servidor</summary>
        public Query NoQuery() {
            instruction = dbItr.noQuery;
            return this;
        }

        /// <summary>Prepara el query para accesar al procedimiento almacenado indicado</summary>
        public Query SP(string storedProcedure, string schema, string database) {
            instruction = dbItr.sProc;
            stProc.name = SanitizeSQL(storedProcedure);
            stProc.schema = SanitizeSQL(schema ?? "");
            stProc.database = SanitizeSQL(database ?? "");
            return this;
        }

        /// <summary>Prepara el query para accesar al procedimiento almacenado indicado</summary>
        public Query SP(string storedProcedure, string schema) {
            return SP(storedProcedure, schema, null);
        }

        /// <summary>Prepara el query para accesar al procedimiento almacenado indicado</summary>
        public Query SP(string storedProcedure) {
            return SP(storedProcedure, null, null);
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega un campo al listado de campos</summary>
        public Query SelAs(string fieldAlias, Query subQuery) {
            instruction = dbItr.sel;
            Field fld = new Field(SanitizeSQL(fieldAlias), subQuery);
            curUnion.lstFields.Add(fld);
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega un campo al listado de campos</summary>
        public Query SelAs(string fieldAlias, Expression expression) {
            instruction = dbItr.sel;
            string exprField = expression.ToString();
            if (!expression.IsFunction && expression.IsComplex) {
                exprField = "( " + exprField + " )";
            }
            Field fld = new Field(exprField);
            fld.nameAlias = SanitizeSQL(fieldAlias);
            fld.expression = expression;
            curUnion.lstFields.Add(fld);
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega un campo al listado de campos</summary>
        public Query SelAs(string fieldAlias, string field) {
            Query qrySel = new Query();
            Field fld = new Field(SanitizeSQL(field));
            fld.nameAlias = SanitizeSQL(fieldAlias);
            qrySel.curUnion.lstFields.Add(fld);
            qrySel.instruction = dbItr.fld;
            return SelAs(fieldAlias, qrySel);
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega un campo al listado de campos agregando comillas en caso de que este no posea</summary>
        public Query SelValAs(string fieldAlias, string value) {
            return SelAs(fieldAlias, AddSingleQuotesIfMissing(value));
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega un campo al listado de campos agregando comillas en caso de que este no posea</summary>
        public Query SelValAs(string fieldAlias, Expression value) {
            return SelAs(fieldAlias, AddSingleQuotesIfMissing(value.ToString()));
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega los campos al listado de campos</summary>
        public Query SelAs(IEnumerable<KeyValuePair<string, string>> namedFields) {
            foreach (KeyValuePair<string, string> fld in namedFields) {
                SelAs(fld.Key, fld.Value);
            }
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega los campos al listado de campos</summary>
        public Query Sel(params string[] fields) {
            instruction = dbItr.sel;
            foreach (string fld in fields) {
                SelAs("", fld);
            }
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega una lista de campos al listado de campos</summary>
        public Query Sel(List<string> fields) {
            instruction = dbItr.sel;
            foreach (string fld in fields) {
                SelAs("", fld);
            }
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega una lista de campos al listado de campos</summary>
        public Query Sel(Dictionary<string, string> fields) {
            instruction = dbItr.sel;
            foreach (KeyValuePair<string, string> fld in fields) {
                SelAs(fld.Key, fld.Value);
            }
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select y agrega una lista de campos al listado de campos</summary>
        public Query Sel(Dictionary<string, object> fields) {
            instruction = dbItr.sel;
            foreach (KeyValuePair<string, object> fld in fields) {
                SelAs(fld.Key, ObjectToExpression(fld.Value));
            }
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select de multiples registros</summary>
        public Query Sel(IEnumerable<Dictionary<string, object>> rows) {
            Dictionary<string, object>[] orderedRows = rows.ToArray();
            List<string> allFields = new List<string>();
            foreach (Dictionary<string, object> row in rows) {
                allFields.AddRange(row.Select(k => k.Key).Where(s => !allFields.Contains(s)));
            }
            for (int i = 0; i < orderedRows.Length; i++) {
                foreach (string alias in allFields) {
                    if (orderedRows[i].ContainsKey(alias)) {
                        SelAs(alias, ObjectToExpression(orderedRows[i][alias]));
                    }
                    else {
                        SelAs(alias, "NULL");
                    }
                }
                if (i < orderedRows.Length - 1) {
                    Union(dbUni.UnionAll);
                }
            }
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select de multiples registros agregando comillas a las cadenas</summary>
        public Query SelVal(IEnumerable<Dictionary<string, object>> rows) {
            Dictionary<string, object>[] orderedRows = rows.ToArray();
            foreach (Dictionary<string, object> row in orderedRows) {
                KeyValuePair<string, object>[] fixedReferenceRow = row.Cast<KeyValuePair<string, object>>().ToArray();
                foreach (KeyValuePair<string, object> kv in fixedReferenceRow) {
                    if (row[kv.Key] is System.String) {
                        row[kv.Key] = AddSingleQuotesIfMissing(kv.Value.ToString());
                    }
                }
            }
            Sel(rows);
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta select agregando comillas a las cadenas</summary>
        public Query SelVal(Dictionary<string, object> row) {
            foreach (KeyValuePair<string, object> kv in row) {
                if (kv.Value is System.String) {
                    SelAs(kv.Key, AddSingleQuotesIfMissing(kv.Value.ToString()));
                }
                else {
                    SelAs(kv.Key, ObjectToExpression(kv.Value));
                }
            }
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta insert y asigna la tabla afectada en el esquema correspondiente de la base de datos indicada</summary>
        public Query Insert(string table, string schema, string database) {
            instruction = dbItr.ins;
            return FromAs("", table, schema, database);
        }

        /// <summary>Prepara el query para realizar una consulta insert y asigna la tabla afectada en el esquema correspondiente</summary>
        public Query Insert(string table, string schema) {
            instruction = dbItr.ins;
            return FromAs("", table, schema);
        }

        /// <summary>Prepara el query para realizar una consulta insert y asigna la tabla afectada</summary>
        public Query Insert(string table) {
            instruction = dbItr.ins;
            return From(table);
        }

        /// <summary>Prepara el query para realizar una consulta update y asigna la tabla afectada</summary>
        public Query Update(string table, string schema, string database) {
            instruction = dbItr.upd;
            updateFrom = new Table(SanitizeSQL(table));
            updateFrom.schema = schema == null ? "" : SanitizeSQL(schema);
            updateFrom.database = database == null ? "" : SanitizeSQL(database);
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta update y asigna la tabla afectada</summary>
        public Query Update(string table, string schema) {
            return Update(table, schema, null);
        }

        /// <summary>Prepara el query para realizar una consulta update y asigna la tabla afectada</summary>
        public Query Update(string table) {
            return Update(table, null, null);
        }

        /// <summary>Prepara el query para realizar una consulta delete y asigna la tabla afectada</summary>
        public Query Delete(string table, string schema, string database) {
            instruction = dbItr.del;
            return FromAs("", table, schema, database);
        }

        /// <summary>Prepara el query para realizar una consulta delete y asigna la tabla afectada</summary>
        public Query Delete(string table, string schema) {
            instruction = dbItr.del;
            return FromAs("", table, schema);
        }

        /// <summary>Prepara el query para realizar una consulta delete y asigna la tabla afectada</summary>
        public Query Delete(string table) {
            instruction = dbItr.del;
            return From(table);
        }

        /// <summary>Prepara el query para realizar una consulta merge y asigna las tablas afectadas</summary>
        public Query Merge(string aliasDestiny, string destiny, string aliasOrigin, Query origin) {
            instruction = dbItr.mer;
            merge = new Merger();
            merge.Destiny.Table = new Table(SanitizeSQL(destiny));
            merge.Destiny.Alias = aliasDestiny;
            merge.Origin.Query = origin;
            merge.Origin.Alias = aliasOrigin;
            return this;
        }

        /// <summary>Prepara el query para realizar una consulta merge y asigna las tablas afectadas</summary>
        public Query Merge(string destiny, Query origin) {
            return Merge("d", destiny, "o", origin);
        }

        #endregion

        #region Metodos Estructurales
        /// <summary>Agrega una tabla basada en un query con un alias al listado de tablas</summary>
        public Query FromAs(string tableAlias, Query table) {
            Table tbl = new Table("( " + table.ToString() + " )");
            tbl.tableAlias = SanitizeSQL(tableAlias);
            tbl.schema = SanitizeSQL("");
            tbl.database = SanitizeSQL("");
            curUnion.lstFrom.Add(tbl);
            return this;
        }

        /// <summary>Agrega una tabla con un alias al listado de tablas</summary>
        public Query FromAs(string tableAlias, string table, string schema, string database) {
            Table tbl = new Table(SanitizeSQL(table));
            tbl.tableAlias = SanitizeSQL(tableAlias);
            tbl.schema = SanitizeSQL(schema);
            tbl.database = SanitizeSQL(database);
            curUnion.lstFrom.Add(tbl);
            return this;
        }

        /// <summary>Agrega una tabla con un alias al listado de tablas</summary>
        public Query FromAs(string tableAlias, string table, string schema) {
            return FromAs(tableAlias, table, schema, "");
        }

        /// <summary>Agrega una tabla con un alias al listado de tablas</summary>
        public Query FromAs(string tableAlias, string table) {
            return FromAs(tableAlias, table, "", "");
        }

        /// <summary>Agrega una tabla al listado de tablas</summary>
        public Query From(string table) {
            return FromAs("", table);
        }

        /// <summary>Agrega una clausula de salida</summary>
        public Query Output(dbOut type, string table, params string[] columns) {
            IEnumerable<Field> fields = columns.Select(c => new Field(c));
            if (instruction == dbItr.mer) {
                merge.MergeOut(type, table);
                merge.MergeOutCols(fields.ToArray());
            }
            else {
                output.type = type;
                output.table = table;
                output.columns.AddRange(fields);
            }
            return this;
        }

        /// <summary>Agrega una clausula de salida</summary>
        public Query Output(string table, params string[] columns) {
            return Output(dbOut.Undefined, table, columns);
        }

        /// <summary>Agrega una clausula de salida</summary>
        public Query Output(string table) {
            return Output(dbOut.Undefined, table, new string[] { });
        }

        /// <summary>Agrega columnas a la clausula de salida</summary>
        public Query OutputCols(string[] columns) {
            IEnumerable<Field> fields = columns.Select(c => new Field(c));
            if (instruction == dbItr.mer) {
                merge.MergeOutCols(fields.ToArray());
            }
            else {
                output.columns.AddRange(fields);
            }
            return this;
        }

        /// <summary>Agrega columnas a la clausula de salida</summary>
        public Query OutputCol(string alias, string column) {
            Field field = new Field(column);
            field.nameAlias = alias;
            if (instruction == dbItr.mer) {
                merge.MergeOutCols(field);
            }
            else {
                output.columns.Add(field);
            }
            return this;
        }

        /// <summary>Agrega columnas a la clausula de salida</summary>
        public Query OutputCol(string column) {
            return OutputCol(null, column);
        }

        /// <summary>Agrega una tabla al listado de tablas a unir</summary>
        public Query Join(dbJoi joinType, string tableAlias, Query table) {
            Table tbl = new Table("( " + table.ToString() + " )");
            tbl.tableAlias = SanitizeSQL(tableAlias);
            tbl.schema = SanitizeSQL("");
            tbl.database = SanitizeSQL("");
            curUnion.lstFrom.Add(tbl);
            if (curUnion.lstJoin.Any(j => j.tableAlias == tableAlias)) {
                throw new Exception("Ya existe el alias " + SanitizeSQL(tableAlias) + " en la consulta");
            }
            curUnion.lstJoin.Add(new JoinTable() { tableAlias = SanitizeSQL(tableAlias), type = joinType });
            return this;
        }

        /// <summary>Agrega una tabla al listado de tablas a unir bajo inner join</summary>
        public Query Join(string tableAlias, Query table) {
            return Join(dbJoi.Inner, tableAlias, table);
        }

        /// <summary>Agrega una tabla al listado de tablas a unir</summary>
        public Query Join(dbJoi joinType, string tableAlias, string table, string schema, string database) {
            Table tbl = new Table(SanitizeSQL(table));
            tbl.tableAlias = SanitizeSQL(tableAlias);
            tbl.schema = SanitizeSQL(schema);
            tbl.database = SanitizeSQL(database);
            curUnion.lstFrom.Add(tbl);
            if (curUnion.lstJoin.Any(j => j.tableAlias == tableAlias)) {
                throw new Exception("Ya existe el alias " + SanitizeSQL(tableAlias) + " en la consulta");
            }
            curUnion.lstJoin.Add(new JoinTable() { tableAlias = SanitizeSQL(tableAlias), type = joinType });
            return this;
        }

        /// <summary>Agrega una tabla al listado de tablas a unir</summary>
        public Query Join(dbJoi joinType, string tableAlias, string table, string schema) {
            return Join(joinType, tableAlias, table, schema, "");
        }

        /// <summary>Agrega una tabla al listado de tablas a unir</summary>
        public Query Join(dbJoi joinType, string tableAlias, string table) {
            return Join(joinType, tableAlias, table, "", "");
        }

        /// <summary>Agrega una tabla al listado de tablas a unir bajo inner join</summary>
        public Query Join(string tableAlias, string table) {
            return Join(dbJoi.Inner, tableAlias, table, "", "");
        }

        /// <summary>Agrega una condicion de union para las tablas usando una expression</summary>
        public Query JoinOn(Expression expr) {
            Comparison whr = new Comparison();
            whr.expr = expr;
            whr.owner = curUnion.lstFrom.Last().tableAlias;
            curUnion.lstJoinOn.Add(whr);
            return this;
        }

        /// <summary>Agrega una condicion de union para las tablas</summary>
        public Query JoinOn(dbCom comp, Expression value1, params Expression[] values) {
            Comparison whr = new Comparison();
            whr.comparator = value1.ToString();
            whr.type = comp;
            whr.values = values.Select(v => v.ToString()).ToList();
            whr.owner = curUnion.lstFrom.Last().tableAlias;
            curUnion.lstJoinOn.Add(whr);
            return this;
        }

        /// <summary>Agrega una condicion de union de equidad para las tablas</summary>
        public Query JoinOn(Expression value1, Expression value2) {
            return JoinOn(dbCom.Equals, value1, value2);
        }

        /// <summary>Agrega una condicion de union a valor para las tablas</summary>
        public Query JoinOnVal(dbCom comp, string value1, params string[] values) {
            Expression[] expr = values.Select(s => (Expression)(AddSingleQuotesIfMissing(s))).ToArray();
            return JoinOn(dbCom.Equals, value1, expr);
        }

        /// <summary>Agrega una condicion de union de equidad a valor para las tablas</summary>
        public Query JoinOnVal(string value1, string value2) {
            return JoinOnVal(dbCom.Equals, value1, value2);
        }

        /// <summary>Agrega una condición de union para el origen y el destino de la consulta merge</summary>
        public Query MergeOn(Constructor.dbCom comp, Expression keyDestiny, Expression keyOrigin) {
            merge.MergeOn(comp, keyDestiny, keyOrigin);
            return this;
        }

        /// <summary>Agrega una condición de union de equidad para el origen y el destino de la consulta merge</summary>
        public Query MergeOn(Expression keyDestiny, Expression keyOrigin) {
            return MergeOn(Constructor.dbCom.Equals, keyDestiny, keyOrigin);
        }

        /// <summary>Agrega una condición de union de tipo expressión para el origen y el destino de la consulta merge</summary>
        public Query MergeOn(Expression where) {
            merge.MergeOn(where);
            return this;
        }

        /// <summary>Agrega un nuevo criterio a comparar de registros de origen y destino en la consulta merge</summary>
        public Query MergeWhen(bool matched) {
            merge.MergeWhen(matched);
            return this;
        }

        /// <summary>Agrega una condición al criterio a comparar de registros de origen y destino en la consulta merge</summary>
        public Query MergeWhen(Constructor.dbCom comp, Expression comparator, params Expression[] values) {
            Comparison whr = new Comparison();
            whr.comparator = comparator.ToString();
            whr.type = comp;
            whr.values = new List<string>();
            foreach (Expression val in values) {
                whr.values.Add(val.ToString());
            }
            merge.MergeWhen(whr);
            return this;
        }

        /// <summary>Agrega una condición de igualdad al criterio a comparar de registros de origen y destino en la consulta merge</summary>
        public Query MergeWhen(Expression comparator, params Expression[] values) {
            return MergeWhen(dbCom.Equals, comparator, values);
        }

        /// <summary>Especifica la acción a realizar para la comparación en la consulta merge</summary>
        public Query MergeThen(Constructor.dbMrA action, Dictionary<string, object> values) {
            merge.MergeThen(action, values);
            return this;
        }

        /// <summary>Especifica la acción a realizar para la comparación en la consulta merge</summary>
        public Query MergeThen(Constructor.dbMrA action, Dictionary<string, string> values) {
            MergeThen(action, values.ToDictionary(k => k.Key, v => (object)v.Value));
            return this;
        }

        /// <summary>Especifica la acción a realizar para la comparación en la consulta merge</summary>
        public Query MergeThen(Constructor.dbMrA action, IEnumerable<string> values) {
            MergeThen(action, values.ToDictionary(s => s, s => ""));
            return this;
        }

        /// <summary>Especifica la acción a realizar para la comparación en la consulta merge</summary>
        public Query MergeThen(Constructor.dbMrA action, params string[] values) {
            MergeThen(action, values.ToDictionary(s => s, s => ""));
            return this;
        }

        /// <summary>Especifica la acción a realizar para la comparación en la consulta merge</summary>
        public Query MergeThen(Constructor.dbMrA action) {
            Dictionary<string, object> nullDic = null;
            return MergeThen(action, nullDic);
        }

        /// <summary>Agrega una condición de expresion lógica al listado de condiciones</summary>
        public Query Where(Expression expr) {
            Comparison whr = new Comparison();
            whr.expr = expr;
            curUnion.lstWhere.Add(whr);
            return this;
        }

        /// <summary>Agrega una o varias condiciones al listado de condiciones</summary>
        public Query Where(dbCom comp, Expression val, params Expression[] values) {
            Comparison whr = new Comparison();
            whr.comparator = val.ToString();
            whr.type = comp;
            whr.values = values.Select(v => v.ToString()).ToList();
            curUnion.lstWhere.Add(whr);
            return this;
        }

        /// <summary>Agrega una o varias condiciones envasadas al listado de condiciones</summary>
        public Query Where(dbCom comp, Expression val, params object[] values) {
            return Where(comp, val, values.Select(v => ObjectToExpression(v)).ToArray());
        }

        /// <summary>Agrega una condicion envasada de equidad al listado de condiciones</summary>
        public Query Where(Expression value1, object value2) {
            return Where(dbCom.Equals, value1, value2);
        }

        /// <summary>Agrega una o varias condiciones al listado de condiciones</summary>
        public Query Where(dbCom comp, Expression val, params string[] values) {
            return Where(comp, val, values.Select(v => (Expression)v).ToArray());
        }

        /// <summary>Agrega una o varias condiciones al listado de condiciones</summary>
        public Query Where(dbCom comp, Expression val, params int[] values) {
            return Where(comp, val, values.Select(v => (Expression)v).ToArray());
        }

        /// <summary>Agrega una condicion de equidad al listado de condiciones</summary>
        public Query Where(Expression value1, Expression value2) {
            return Where(dbCom.Equals, value1, new Expression[] { value2 });
        }

        /// <summary>Agrega una condicion a valor al listado de condiciones</summary>
        public Query WhereVal(dbCom comp, Expression val, params string[] values) {
            Expression[] expr = values.Select(s => (Expression)(AddSingleQuotesIfMissing(s))).ToArray();
            return Where(comp, val, expr);
        }

        /// <summary>Agrega una condicion de equidad a valor al listado de condiciones</summary>
        public Query WhereVal(Expression val, string values) {
            return WhereVal(dbCom.Equals, val, values);
        }

        /// <summary>Agrega un campo al listado de campos de agrupacion</summary>
        public Query GroupBy(params Expression[] field) {
            curUnion.lstGroupBy.AddRange(field);
            return this;
        }

        /// <summary>Agrega una lista de campos al listado de campos de agrupacion</summary>
        public Query GroupBy(IEnumerable<string> fields) {
            foreach (string fld in fields) {
                GroupBy(fld);
            }
            return this;
        }

        /// <summary>Agrega los campos faltantes al listado de campos de agrupacion</summary>
        public Query GroupByMissing() {
            bool isPresent;
            foreach (Field fld in curUnion.lstFields) {
                if ((fld.subQuery != null && fld.subQuery.instruction == dbItr.fld) || fld.expression != null) {
                    isPresent = false;
                    foreach (Expression gro in curUnion.lstGroupBy) {
                        if (fld.name == gro.ToString()) {
                            isPresent = true;
                        }
                    }
                    if (!isPresent) {
                        GroupBy(fld.name);
                    }
                }
            }
            return this;
        }

        /// <summary>Agrega una condicion al listado de condiciones de agrupacion</summary>
        public Query Having(dbCom oper, Expression comp1, params Expression[] comp2) {
            Comparison whr = new Comparison();
            whr.comparator = comp1.ToString();
            whr.type = oper;
            whr.values = comp2.Select(c => c.ToString()).ToList();
            curUnion.lstHaving.Add(whr);
            return this;
        }

        /// <summary>Agrega una condicion de equidad al listado de condiciones de agrupacion</summary>
        public Query Having(Expression comp1, Expression comp2) {
            return Having(Constructor.dbCom.Equals, comp1, comp2);
        }

        /// <summary>Agrega una condicion al listado de condiciones de agrupacion</summary>
        public Query Having(Expression expr) {
            Comparison whr = new Comparison();
            whr.expr = expr;
            curUnion.lstHaving.Add(whr);
            return this;
        }

        /// <summary>Agrega un campo al listado de campos de orden</summary>
        public Query OrderBy(string field, dbOrd order) {
            lstOrderBy.Add((SanitizeSQL(field) + " " + order.ToString().ToUpper()).Trim());
            return this;
        }

        /// <summary>Agrega un campo al listado de campos de orden</summary>
        public Query OrderBy(params string[] fields) {
            foreach (string fld in fields) {
                OrderBy(fld, dbOrd.Asc);
            }
            return this;
        }

        /// <summary>Genera una expresion de tabla comun</summary>
        public Query With(string alias, Query origin) {
            cte.alias = alias;
            cte.origin = origin;
            return this;
        }

        /// <summary>Serializa en XML el resultado de la consulta</summary>
        public Query ForXML(dbXml mode, string element) {
            curUnion.forXml = new ForXMLClause();
            curUnion.forXml.mode = mode;
            curUnion.forXml.element = element;
            return this;
        }

        /// <summary>Serializa en XML el resultado de la consulta</summary>
        public Query ForXML(dbXml mode) {
            ForXML(mode, null);
            return this;
        }

        #endregion

        #region Metodos Argumentales
        /// <summary>Limita la cantidad de registros en la tabla resultante</summary>
        public Query Top(int rows) {
            curUnion.topCount = rows;
            return this;
        }

        /// <summary>Agrega los argumentos al listado de argumentos del procedimiento almacenado</summary>
        public Query Arg(params Expression[] prms) {
            lstParams.AddRange(prms);
            return this;
        }

        /// <summary>Agrega un campo destino y el valor correspondiente para la consulta insert</summary>
        public Query InsFld(string destField, Expression origField) {
            //Agrega un nuevo renglon en caso de ser necesario
            if (insFields.Rows.Count == 0) {
                insFields.Rows.Add(insFields.NewRow());
            }
            //Agrega la columna si no existe
            if (!insFields.Columns.Contains(destField)) {
                insFields.Columns.Add(SanitizeSQL(destField), typeof(Expression));
            }
            //Agrega el valor a la columna en el primer renglon
            insFields.Rows[0][destField] = origField;
            return this;
        }

        /// <summary>Agrega un campo de destino y el valor correspondiente para la consulta insert</summary>
        public Query InsFld(string destField, object origField) {
            return InsFld(destField, ObjectToExpression(origField));
        }

        /// <summary>Agrega un campo destino y el valor correspondiente para la consulta insert</summary>
        public Query InsFldVal(string destField, string origField) {
            Expression expr = (Expression)AddSingleQuotesIfMissing(origField.ToString());
            return InsFld(destField, expr);
        }

        /// <summary>Agrega un campo destino y el valor correspondiente para la consulta insert</summary>
        public Query InsFldVal(string destField, object origField) {
            Expression expr;
            if (origField is string) {
                expr = (Expression)AddSingleQuotesIfMissing(origField.ToString());
            }
            else {
                expr = (Expression)AddSingleQuotesIfMissing(ObjectToExpression(origField).ToString());
            }
            return InsFld(destField, expr);
        }

        /// <summary>Agrega los campos de la tabla a la tabla de insercion</summary>
        public Query InsFlds(DataTable origTable) {
            DataTable insTable = new DataTable();
            //Crea una copia de las columnas en formato de Expression y verifica la seguridad en los nombres de las columnas
            origTable.Columns.Cast<DataColumn>().ToList().ForEach(c => insTable.Columns.Add(SanitizeSQL(c.ColumnName), typeof(Expression)));
            //Verifica que los valores de cada celda sean adecuados
            foreach (DataRow row in origTable.Rows) {
                DataRow newRow = insTable.NewRow();
                foreach (DataColumn col in origTable.Columns) {
                    if (row[col.ColumnName] == DBNull.Value) {
                        newRow[col.ColumnName] = new Expression("NULL");
                    }
                    else {
                        if (row[col.ColumnName] is string) {
                            newRow[col.ColumnName] = (Expression)AddSingleQuotesIfMissing((string)row[col.ColumnName]);
                        }
                        else {
                            newRow[col.ColumnName] = ObjectToExpression(row[col.ColumnName]);
                        }
                    }
                }
                insTable.Rows.Add(newRow);
            }
            insFields = insTable;
            return this;
        }

        /// <summary>Agrega los campos de la tabla a la tabla de insercion</summary>
        public Query InsFlds(Dictionary<string, object> fields) {
            DataTable insTable = new DataTable();
            foreach (string key in fields.Keys) {
                insTable.Columns.Add(SanitizeSQL(key), typeof(Expression));
            }
            DataRow newRow = insTable.NewRow();
            foreach (KeyValuePair<string, object> fld in fields) {
                if (fld.Value == null) {
                    newRow[fld.Key] = new Expression("NULL");
                }
                else {
                    if (fld.Value is string) {
                        newRow[fld.Key] = (Expression)AddSingleQuotesIfMissing((string)fld.Value);
                    }
                    else {
                        newRow[fld.Key] = ObjectToExpression(fld.Value);
                    }
                }
            }
            insTable.Rows.Add(newRow);
            insFields = insTable;
            return this;
        }

        /// <summary>Agrega el query como origen de datos a la consulta insert</summary>
        public Query InsFlds(Query query) {
            insQuery = query;
            return this;
        }

        /// <summary>Agrega un campo a asignar en la instruccion update</summary>
        public Query SetUpd(Dictionary<string, Object> values) {
            lstSet.AddRange(values.Select(v => new string[] { v.Key, ObjectToExpression((v.Value is string) ? AddSingleQuotesIfMissing((string)v.Value) : v.Value).ToString() }));
            return this;
        }

        /// <summary>Agrega un campo a asignar en la instruccion update</summary>
        public Query SetUpd(Dictionary<string, string> values) {
            lstSet.AddRange(values.Select(v => new string[] { v.Key, v.Value }));
            return this;
        }

        /// <summary>Agrega un campo a asignar en la instruccion update</summary>
        public Query SetUpd(string field, Expression newValue) {
            lstSet.Add(new string[] { field, newValue != null ? newValue.ToString() : "NULL" });
            return this;
        }

        /// <summary>Agrega un valor a asignar en la instruccion update</summary>
        public Query SetUpdVal(string field, string newValue) {
            return SetUpd(field, AddSingleQuotesIfMissing(newValue));
        }

        /// <summary>Agrega un campo con funcion agregada al listado de campos</summary>
        public Query AggAs(dbAgr aggregate, string field, string fieldAlias) {
            instruction = dbItr.sel;
            curUnion.lstAggFields.Add(new Aggregate(aggregate, new Field(SanitizeSQL(field)), SanitizeSQL(fieldAlias)));
            return this;
        }

        /// <summary>Agrega un campo con funcion agregada al listado de campos</summary>
        public Query AggAs(dbAgr aggregate, Query field, string fieldAlias) {
            instruction = dbItr.sel;
            Field fld = new Field(SanitizeSQL(fieldAlias), field);
            curUnion.lstAggFields.Add(new Aggregate(aggregate, fld, SanitizeSQL(fieldAlias)));
            return this;
        }

        /// <summary>Agrega un campo con funcion agregada al listado de campos</summary>
        public Query AggAs(dbAgr aggregate, Expression expr, string fieldAlias) {
            instruction = dbItr.sel;
            Field fld = new Field(SanitizeSQL(fieldAlias), expr);
            fld.expression = expr;
            curUnion.lstAggFields.Add(new Aggregate(aggregate, fld, SanitizeSQL(fieldAlias)));
            return this;
        }

        /// <summary>Agrega un campo con funcion agregada al listado de campos</summary>
        public Query Agg(dbAgr aggregate, string field) {
            AggAs(aggregate, SanitizeSQL(field), "");
            return this;
        }

        /// <summary>Agrega un campo con funcion agregada al listado de campos</summary>
        public Query Agg(dbAgr aggregate, Query field) {
            return AggAs(aggregate, field, "");
        }

        /// <summary>Agrega un campo con funcion agregada al listado de campos</summary>
        public Query Agg(dbAgr aggregate, Expression field) {
            return AggAs(aggregate, field, "");
        }
        #endregion
    }
}
