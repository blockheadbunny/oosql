using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace DataFramework
{
    /// <summary>Facilita consultas de lectura y escritura de objetos logicos</summary>
    public class DataMapper
    {
        private class FieldMap
        {
	        public string name;
	        public string value;
	        public Query.dbCom comp;
	        public bool isModified;
        }

        public enum Type { Sel = 1, Ins = 2, Upd = 3, Del = 4 };

        private string strTable;
        private Query Query;
        private List<FieldMap> lstFieldMap = new List<FieldMap>();
        private List<FieldMap> lstKeyMap = new List<FieldMap>();

        /// <summary>Campo llave para identificar los registros que se afectaran</summary>

        public DataMapper(string table)
        {
	        strTable = table;
        }

        /// <summary>Crea y/o establece el valor en cadena vacia de las propiedades nombradas</summary>
        public void AddItems(params string[] fields)
        {
	        foreach (string fld in fields) {
		        SetItem(fld, "");
	        }
        }

        /// <summary>Crea y/o establece el valor en cadena vacia de las propiedades nombradas</summary>
        public void AddItems(Dictionary<string, string>.ValueCollection fields)
        {
            foreach (string fld in fields)
            {
                SetItem(fld, "");
            }
        }

        /// <summary>devuelve el valor de la propiedad nombrada</summary>
        public string GetItem(string field)
        {
            return GetField(field, lstFieldMap);
        }

        /// <summary>devuelve el valor del campo llave nombrado</summary>
        public string GetKey(string key)
        {
            return GetField(key, lstKeyMap);
        }

        /// <summary>Obtiene el valor de una posicion en una lista de cadenas</summary>
        private string GetField(string field, List<FieldMap> lstFields)
        {
	        foreach (FieldMap fdmField in lstFields) {
		        if (fdmField.name == field) {
			        return fdmField.value;
		        }
	        }
	        return null;
        }

        /// <summary>crea o establece el valor de la propiedad nombrada</summary>
        public void SetItem(string field, string value)
        {
            if (GetField(field, lstFieldMap) != value)
            {
                SetField(field, value, lstFieldMap);
            }
        }

        /// <summary>crea o establece el valor del campo llave nombrado</summary>
        public void SetKey(string key, string value)
        {
            SetField(key, value, lstKeyMap);
        }

        /// <summary>Crea o asigna el valor de una posicion en una lista de cadenas</summary>
        private void SetField(string field, string value, List<FieldMap> lstFields)
        {
	        FieldMap fdmAdd = new FieldMap();
	        fdmAdd.name = field;
	        fdmAdd.value = value;
            fdmAdd.comp = Query.dbCom.Equals;
	        fdmAdd.isModified = true;
	        foreach (FieldMap fldM in lstFieldMap) {
		        if (fldM.name == field) {
			        fldM.value = value;
                    fdmAdd.comp = Query.dbCom.Equals;
                    fldM.isModified = true;
			        return;
		        }
	        }
	        lstFieldMap.Add(fdmAdd);
        }

        /// <summary>Genera el objeto query correspondiente al mapeo</summary>
        public Query MapQuery(DataMapper.Type MapType)
        {
	        Query = new Query();
	        switch (MapType) {
		        case Type.Sel:
			        return MapQuerySelect();
		        case Type.Ins:
			        return MapQueryInsert();
		        case Type.Upd:
			        return MapQueryUpdate();
		        case Type.Del:
			        return MapQueryDelete();
	        }
	        return null;
        }

        /// <summary>Genera el objeto query correspondiente al mapeo del select</summary>
        private Query MapQuerySelect()
        {
	        foreach (FieldMap fldM in lstFieldMap) {
		        Query.Sel(fldM.name);
	        }
	        Query.From(strTable);
	        foreach (FieldMap fldM in lstKeyMap) {
		        Query.Where(fldM.comp, fldM.name, fldM.value.ToString());
	        }
	        return Query;
        }

        /// <summary>Genera el objeto query correspondiente al mapeo del insert</summary>
        private Query MapQueryInsert()
        {
	        Query.Insert(strTable);
	        foreach (FieldMap fldM in lstFieldMap) {
		        Query.InsFld(fldM.name, fldM.value);
	        }
	        return Query;
        }

        /// <summary>Genera el objeto query correspondiente al mapeo del update</summary>
        private Query MapQueryUpdate()
        {
	        bool atLeastOneInList = false;
	        Query.Update(strTable);
	        foreach (FieldMap fldM in lstFieldMap) {
		        if (fldM.isModified) {
			        Query.SetUpd(fldM.name, fldM.value);
			        atLeastOneInList = true;
		        }
	        }
	        foreach (FieldMap fldM in lstKeyMap) {
		        Query.Where(fldM.comp, fldM.name, fldM.value.ToString());
	        }
	        if (!atLeastOneInList) {
		        Query.NoQuery();
	        }
	        return Query;
        }

        /// <summary>Genera el objeto query correspondiente al mapeo del delete</summary>
        private Query MapQueryDelete()
        {
	        Query.Delete(strTable);
	        foreach (FieldMap fldM in lstKeyMap) {
		        Query.Where(fldM.comp, fldM.name, fldM.value.ToString());
	        }
	        return null;
        }

        /// <summary>Asigna los valores de los campos consultados en la base de datos de vuelta al listado de campos</summary>
        public void SelectResult(DataTable dttResult)
        {
	        if ((dttResult != null)) {
		        if (dttResult.Rows.Count == 1) {
			        foreach (DataColumn colR in dttResult.Columns) {
				        foreach (FieldMap fdmField in lstFieldMap) {
					        if (fdmField.name == colR.ColumnName) {
                                fdmField.value = dttResult.Rows[0][colR.ColumnName].ToString();
						        fdmField.isModified = false;
					        }
				        }
			        }
		        } else if (dttResult.Rows.Count == 0) {
			        throw new Exception("La entidad buscada no se encontro en la base de datos");
		        } else {
			        throw new Exception("Se recibio mas de un registro para la entidad buscada");
		        }
	        }
        }

    }
}
