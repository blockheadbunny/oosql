using System;
using System.Data;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace DataFramework {
    /// <summary>
    /// Genera consultas basicas de altas bajas y cambios sobre objetos 
    /// cuyos nombres de clase y nombres de propiedades 
    /// equivalen al nombre de la tabla y al nombre de las columnas respectivamente
    /// </summary>
    public class Mapper {
        private string strCon;

        public Mapper(string ConectionString) {
            strCon = StripConnString(ConectionString);
        }

        private string StripConnString(string ConectionString) {
            string[] validParms = { "User ID", "UID", "Password", "PWD", "Initial Catalog", "Database", "Data Source", "Server", "Packet Size", "Workstation ID", "WSID" };
            string[] parms = ConectionString.Split(new char[] { ';' });
            IEnumerable<string> parmsFiltrados = parms.Where(p => validParms.Any(v => Regex.IsMatch(p, "^[ ]*" + v + "[ ]*=.*", RegexOptions.IgnoreCase)));
            return string.Join(";", parmsFiltrados.ToArray());
        }

        /// <summary>Obtiene un listado de instancias de T a partir de la información obtenida en la base de datos filtrando con condición where</summary>
        public List<T> GettMany<T>(Expression where) where T : new() {
            List<PropertyInfo> properties = typeof(T).GetProperties().ToList();
            DataManager dm = new DataManager(strCon);
            Query q = dm.NewQuery().Sel(properties.Select(p => p.Name).ToArray()).From(typeof(T).Name);
            if (where != null) { q.Where(where); }
            DataTable res = dm.ExecTable();
            if (res == null) { return new List<T>(); }
            return res.Rows.Cast<DataRow>().Select(r => Map<T>(r, properties)).ToList();
        }

        /// <summary>Obtiene un listado de instancias de T a partir de la información obtenida en la base de datos</summary>
        public List<T> GettMany<T>() where T : new() {
            return GettMany<T>(null);
        }

        /// <summary>Obtiene una instancia de T a partir de la información obtenida en la base de datos filtrando con condición where</summary>
        public T Gett<T>(Expression where) where T : new() {
            return GettMany<T>(where).First();
        }

        private Query SelBoxedAs(Query q, string name, object value) {
            if (value is string) {
                if (Regex.IsMatch((string)value, "^'([^']|'')*'$")) {
                    q.SelAs(name, (string)value);
                }
                else {
                    q.SelAs(name, "'" + (string)value + "'");
                }
            }
            if (value is long) { q.SelAs(name, (long)value); }
            if (value is int) { q.SelAs(name, (int)value); }
            if (value is decimal) { q.SelAs(name, (decimal)value); }
            if (value is float) { q.SelAs(name, (float)value); }
            if (value is double) { q.SelAs(name, (double)value); }
            if (value is DateTime) { q.SelAs(name, (DateTime)value); }
            if (value is Query) { q.SelAs(name, (Query)value); }
            return q;
        }

        private Query GetSelectMerge<T>(IEnumerable<T> origin) {
            T[] indexableOrigin = origin.ToArray();
            PropertyInfo[] properties = typeof(T).GetProperties();
            Query q = new Query();
            for (int i = 0; i < origin.Count(); i++) {
                foreach (PropertyInfo prop in properties) {
                    q = SelBoxedAs(q, prop.Name, prop.GetValue(indexableOrigin[i], null));
                }
                if (i + 1 < origin.Count()) {
                    q.Union(Constructor.dbUni.UnionAll);
                }
            }
            return q;
        }

        /// <summary>Devuelve un query que compara las instancias de T en base a la condición de expresión, las actualiza si las encuentra y las inserta si no las encuentra</summary>
        public Query QueryMerge<T>(IEnumerable<T> origin, Expression where, params string[] keys) {
            if (origin == null) { throw new ArgumentNullException("origin"); }
            if (!origin.Any()) { return new Query().NoQuery(); }

            IEnumerable<PropertyInfo> properties = typeof(T).GetProperties().Where(p => !keys.Contains(p.Name));
            Query q = new Query()
                .Merge(typeof(T).Name, GetSelectMerge(origin))
                .MergeOn(where)
                .MergeWhen(true)
                .MergeThen(Constructor.dbMrA.Update, properties.ToDictionary(p => p.Name, p => p.Name))
                .MergeWhen(false)
                .MergeThen(Constructor.dbMrA.Insert, properties.ToDictionary(p => p.Name, p => p.Name));
            return q;
        }

        /// <summary>Devuelve un query que compara las instancias de T en base a las llaves, las actualiza si las encuentra y las inserta si no las encuentra</summary>
        public Query QueryMerge<T>(IEnumerable<T> origin, string[] keys) {
            if (keys == null) { throw new ArgumentNullException("origin"); }
            if (!keys.Any()) { throw new ArgumentException("Argument length can not be zero", "origin"); }

            Expression where = Expression.Where("d." + keys[0], "o." + keys[0]);
            for (int i = 1; i < keys.Length; i++) {
                where.And("d." + keys[i], "o." + keys[i]);
            }
            return QueryMerge(origin, where);
        }

        /// <summary>Compara las instancias de T en base a la condición de expresión, las actualiza si las encuentra y las inserta si no las encuentra</summary>
        public void Merge<T>(IEnumerable<T> origin, Expression where, params string[] keys) {
            new DataManager(strCon).Exec(QueryMerge(origin, where, keys));
        }

        /// <summary>Compara las instancias de T en base a las llaves, las actualiza si las encuentra y las inserta si no las encuentra</summary>
        public void Merge<T>(IEnumerable<T> origin, string[] keys) {
            if (keys == null) { throw new ArgumentNullException("origin"); }
            if (!keys.Any()) { throw new ArgumentException("Argument length can not be zero", "origin"); }

            T[] indexableOrigin = origin.ToArray();
            Expression where = Expression.Where("d." + keys[0], "o." + keys[0]);
            for (int i = 1; i < indexableOrigin.Length; i++) {
                where.And("d." + keys[i], "o." + keys[i]);
            }
            Merge(origin, where);
        }

        private T Map<T>(DataRow row, List<PropertyInfo> properties) where T : new() {
            T entity = new T();
            properties.ForEach(p => p.SetValue(entity, DBNullToNull(row[p.Name]), null));
            return entity;
        }

        private object DBNullToNull(object val) {
            if (val is DBNull) { return null; }
            return val;
        }
    }
}
