using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace DataFramework {
    /// <summary>Manipulador de conexiones y consultas a la base de datos</summary>
    public class DataManager : IDisposable {
        private string strCon;
        private SqlConnection sqlCon;
        private SqlCommand sqlCmd;
        private SqlDataAdapter sqlAda;
        private bool openCon;

        public Batch Bch { get; set; }
        public Query Qry { get { return Bch.Qry; } }
        public DataMapper Map { get; set; }

        public DataManager(string ConectionString) {
            strCon = StripConnString(ConectionString);
        }

        private string StripConnString(string ConectionString) {
            string[] parms;
            string finalParms = "";
            string[] validParms = { "User ID", "UID", "Password", "PWD", "Initial Catalog", "Database", "Data Source", "Addr", "Server", "Packet Size", "Workstation ID", "WSID", "Timeout" };
            parms = ConectionString.Split(new char[] { ';' });
            foreach (string prm in parms) {
                foreach (string vld in validParms) {
                    if (Regex.IsMatch(prm, "^[ ]*" + vld + "[ ]*=.*", RegexOptions.IgnoreCase)) {
                        finalParms += prm + ";";
                    }
                }
            }
            return finalParms;
        }

        /// <summary>Crea un nuevo batch vacio y lo alista para su posterior ejecucion</summary>
        public Batch NewBatch(params Batch.Option[] options) {
            Batch newBch = new Batch(options);
            Bch = newBch;
            return newBch;
        }

        /// <summary>Crea un nuevo batch vacio y lo alista para su posterior ejecucion</summary>
        public Batch NewBatch() {
            Batch newBch = new Batch();
            Bch = newBch;
            return newBch;
        }

        /// <summary>Crea un nuevo query vacio y lo alista para su posterior ejecucion</summary>
        public Query NewQuery() {
            Batch newBch = NewBatch();
            newBch.NewQuery();
            return newBch.Qry;
        }

        /// <summary>Crea un nuevo mapeador en base a una tabla y lo alista para su posterior ejecucion</summary>
        public DataMapper NewMapper(string table) {
            DataMapper newMpr = new DataMapper(table);
            Map = newMpr;
            return newMpr;
        }

        /// <summary>Ejecuta el query proveido y devuelve un dataset con los resultados del mismo</summary>
        public DataSet Exec(IQryable query, bool keepAlive) {
            if (!openCon) {
                sqlCon = new SqlConnection(strCon);
                sqlAda = new SqlDataAdapter();
            }
            DataSet dtsRes = new DataSet();
            try {
                if (!openCon) {
                    sqlCon.Open();
                }
                sqlCmd = sqlCon.CreateCommand();
                sqlCmd.CommandText = query.ToString();
                sqlCmd.CommandTimeout = 0;
                sqlAda.SelectCommand = sqlCmd;
                sqlAda.Fill(dtsRes);
                openCon = keepAlive;
                return dtsRes;
            }
            finally {
                if (!keepAlive) {
                    Dispose();
                    openCon = false;
                }
            }
        }

        /// <summary>Ejecuta el query proveido y devuelve un dataset con los resultados del mismo</summary>
        public DataSet Exec(IQryable query) {
            return Exec(query, false);
        }

        /// <summary>Ejecuta el ultimo query almacenado y devuelve un dataset con los resultados del mismo</summary>
        public DataSet Exec() {
            return Exec(Bch);
        }

        /// <summary>Ejecuta los queries enviados en una transacción de rollback automático</summary>
        public DataSet ExecTran(params Query[] queries) {
            Batch b = new Batch(Batch.Option.Transaction);
            b.AddStatement(Batch.SetStatement.xact_abort, true);
            b.AddInstruction(queries);
            return Exec(b);
        }

        /// <summary>Ejecuta el query proveido y devuelve la primer tabla del resultado</summary>
        public DataTable ExecTable(IQryable query) {
            DataSet dtsRes = default(DataSet);
            dtsRes = Exec(query);
            if (dtsRes != null && dtsRes.Tables.Count > 0) {
                return dtsRes.Tables[0];
            }
            return null;
        }

        /// <summary>Ejecuta el ultimo query almacenado y devuelve la primer tabla del resultado</summary>
        public DataTable ExecTable() {
            return ExecTable(Bch);
        }

        /// <summary>Ejecuta el query proveido y devuelve la primer tabla del resultado convirtiendo cada registro en el tipo solicitado</summary>
        public List<T> ExecList<T>(IQryable query) where T : new() {
            DataTable dttRes = ExecTable(query);
            List<T> entities = new List<T>();
            IEnumerable<PropertyInfo> properties = (typeof(T)).GetProperties();
            foreach (DataRow row in dttRes.Rows) {
                T entity = new T();
                foreach (PropertyInfo prop in properties) {
                    if (dttRes.Columns.Contains(prop.Name)) {
                        prop.SetValue(entity, row[prop.Name], null);
                    }
                }
                entities.Add(entity);
            }
            return entities;
        }

        /// <summary>Ejecuta el query proveido y lo convierte al tipo solicitado</summary>
        public T ExecEntity<T>(IQryable query) where T : new()
        {
            return ExecList<T>(query).FirstOrDefault();
        }

        /// <summary>Ejecuta el query proveido y devuelve el primer renglon de la primer tabla del resultado</summary>
        public DataRow ExecRow(IQryable query) {
            DataSet dtsRes = default(DataSet);
            dtsRes = Exec(query);
            if (dtsRes != null && dtsRes.Tables.Count > 0 && dtsRes.Tables[0].Rows.Count > 0) {
                return dtsRes.Tables[0].Rows[0];
            }
            return null;
        }

        /// <summary>Ejecuta el ultimo query almacenado y devuelve el primer renglon de la primer tabla del resultado</summary>
        public DataRow ExecRow() {
            return ExecRow(Bch);
        }

        /// <summary>Ejecuta el query proveido y devuelve el valor de la primera celda del primer renglon de la primer tabla del resultado</summary>
        public object ExecScalar(IQryable query) {
            DataSet dtsRes = default(DataSet);
            dtsRes = Exec(query);
            if (dtsRes != null && dtsRes.Tables.Count > 0 && dtsRes.Tables[0].Rows.Count > 0 && dtsRes.Tables[0].Columns.Count > 0) {
                return dtsRes.Tables[0].Rows[0][0];
            }
            return null;
        }

        /// <summary>Ejecuta el ultimo query almacenado y devuelve el valor de la primera celda del primer renglon de la primer tabla del resultado</summary>
        public object ExecScalar() {
            return ExecScalar(Bch);
        }

        /// <summary>Ejecuta el mapeo proveido y devuelve un dataset con los resultados del mismo</summary>
        public DataSet ExecMap(DataMapper DataMapper, DataMapper.Type MapType) {
            DataSet dtsRes = Exec(DataMapper.MapQuery(MapType));
            if (MapType == DataMapper.Type.Sel & dtsRes.Tables.Count > 0) {
                Map.SelectResult(dtsRes.Tables[0]);
            }
            return dtsRes;
        }

        /// <summary>Ejecuta el ultimo mapeo almacenado y devuelve un dataset con los resultados del mismo</summary>
        public DataSet ExecMap(DataMapper.Type MapType) {
            return ExecMap(Map, MapType);
        }

        public void Dispose() {
            if (sqlAda != null) { sqlAda.Dispose(); }
            if (sqlCmd != null) { sqlCmd.Dispose(); }
            if (sqlCon != null) { sqlCon.Dispose(); }
        }
    }
}
