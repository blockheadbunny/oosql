using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataFramework {
    /// <summary>Non standard methods</summary>
    public static class QueryExtensions {
        /// <summary>Prepares a select query based on an object properties adding missing quotes</summary>
        public static Query SelEntity<T>(this Query query, T row) {
            Dictionary<string, object> propPairs = row
                .GetType()
                .GetProperties()
                .ToDictionary(p => p.Name, p => p.GetValue(row, null));
            return query.SelVal(propPairs);
        }

        /// <summary>Prepares a multi union select query based on an object properties adding missing quotes</summary>
        public static Query SelEntities<T>(this Query query, IEnumerable<T> table) {
            List<T> list = table.ToList();
            if (table.Any()) {
                query.SelEntity<T>(list.First());
                foreach (T row in list.Skip(1)) {
                    query.Union(Constructor.dbUni.UnionAll, new Query().SelEntity<T>(row));
                }
            }
            return query;
        }
    }
}
