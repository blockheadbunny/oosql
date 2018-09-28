using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;

namespace DataFramework
{
    public class FileManager
    {
        /// <summary>Crea o agrega datos a un archivo separado por comas y saltos de linea</summary>
        public void AppendCSVFile(string path, string name, DataTable table)
        {
            AppendCSVFile(path, name, table, false);
        }

        /// <summary>Crea o agrega datos a un archivo separado por comas y saltos de linea</summary>
        public void AppendCSVFile(string path, string name, DataTable table, bool removeHeaders)
        {
            StringBuilder content = new StringBuilder();
            string reg = "";
            if (!removeHeaders)
            {
                foreach (DataColumn col in table.Columns)
                {
                    reg += col.ColumnName + ",";
                }
                reg.Remove(reg.Length - 1);
                content.AppendLine(reg);
            }
            foreach (DataRow row in table.Rows)
            {
                reg = "";
                foreach (DataColumn col in table.Columns)
                {
                    reg += row[col.ColumnName] + ",";
                }
                reg.Remove(reg.Length - 1);
                content.AppendLine(reg);
            }

            File.AppendAllText(path + name, content.ToString());
        }

        /// <summary>Genera un archivo si existe la localizacion indicada</summary>
        public void CreateTextFile(string path, string name, string content, bool overwrite)
        {
            string fullName = path + name;
            if (Directory.Exists(path))
            {
                if (overwrite | !File.Exists(fullName))
                {
                    File.WriteAllText(fullName, content);
                }
                else
                {
                    throw new IOException("El archivo \"" + fullName + "\" ya existe");
                }
            }
            else
            {
                throw new DirectoryNotFoundException("No se encuentra la ruta \"" + path + "\"");
            }
        }

        /// <summary>Genera un archivo si existe la localizacion indicada y no hay presente un archivo con el nombre indicado</summary>
        public void CreateTextFile(string path, string name, string content)
        {
            CreateTextFile(path, name, content, false);
        }

        /// <summary>Devuelve el contenido del archivo solicitado</summary>
        public string ReadTextFile(string path)
        {
            StreamReader strRdr = new StreamReader(path);
            string content = strRdr.ReadToEnd();
            strRdr.Close();
            return content;
        }

        public void DeleteFile(string path)
        {
            System.IO.File.Delete(path);
        }
    }
}
