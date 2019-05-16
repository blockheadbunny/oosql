using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataFramework {
    /// <summary>Aloja multiples consultas para su ejecucion en conjunto</summary>
    public class Batch : IQryable {
        private class BatchStatement {
            public SetStatement Name { get; set; }
            public bool Value { get; set; }
        }

        private List<BatchStatement> lstStatements = new List<BatchStatement>();
        private List<IQryable> lstQuery = new List<IQryable>();
        private List<Option> lstOption = new List<Option>();
        private bool useTransaction;

        public enum SetStatement { xact_abort, arithabort }

        public enum Option { Transaction }

        public Query Qry {
            get {
                return (Query)lstQuery.LastOrDefault(q => q.GetType() == typeof(Query));
            }
        }

        public Batch() { }

        public Batch(params Option[] options) {
            foreach (Option opt in options) {
                switch (opt) {
                    case Option.Transaction:
                        useTransaction = true;
                        break;
                }
            }
        }

        /// <summary>Devuelve el texto de todos los querys almacenados</summary>
        public override string ToString() {
            StringBuilder qryText = new StringBuilder();
            qryText.Append(GetStatements());
            if (useTransaction) { qryText.AppendLine("BEGIN TRANSACTION"); }
            foreach (IQryable qry in lstQuery) {
                qryText.AppendLine(qry.ToString());
            }
            if (useTransaction) { qryText.Append("COMMIT"); }
            return qryText.ToString();
        }

        /// <summary>Devuelve el texto correspondiente a las declaraciones de opciones del batch</summary>
        private String GetStatements() {
            StringBuilder options = new StringBuilder();
            foreach (BatchStatement sta in lstStatements) {
                options.Append("SET " + sta.Name.ToString().ToUpper() + " " + (sta.Value ? "ON" : "OFF") + " ");
            }
            string setOptions = options.ToString();
            if (setOptions.Length > 1) {
                setOptions = setOptions.Remove(setOptions.Length - 1) + Environment.NewLine;
            }
            return setOptions;
        }

        /// <summary>Crea un nuevo query vacio y lo alista para su posterior ejecucion</summary>
        public Query NewQuery() {
            Query newQry = new Query();
            lstQuery.Add(newQry);
            return newQry;
        }

        /// <summary>Agrega un nuevo query al listado para su posterior ejecucion</summary>
        public void AddInstruction(params IQryable[] query) {
            lstQuery.AddRange(query);
        }

        /// <summary>Activa o desactiva la opcion indicada para todo el batch</summary>
        public void AddStatement(SetStatement sta, bool val) {
            BatchStatement bchOpt = new BatchStatement();
            bchOpt.Name = sta;
            bchOpt.Value = val;
            foreach (BatchStatement bo in lstStatements) {
                if (bo.Name == sta) {
                    bo.Value = val;
                    return;
                }
            }
            lstStatements.Add(bchOpt);
        }

        /// <summary>Adds a database changer instruction</summary>
        public void Use(string database) {
            lstQuery.Add(new Instruction(database));
        }

        /// <summary>Agrega una instruccion de inicio de transaccion al batch</summary>
        public void Tran() {
            lstQuery.Add(new Instruction(Instruction.ItrType.tran));
        }

        /// <summary>Agrega una instruccion de terminacion de transaccion al batch</summary>
        public void Commit() {
            lstQuery.Add(new Instruction(Instruction.ItrType.commit));
        }

        /// <summary>Agrega una instruccion de terminacion de transaccion al batch</summary>
        public void Rollback() {
            lstQuery.Add(new Instruction(Instruction.ItrType.rollback));
        }

        /// <summary>Inserts a return instruction</summary>
        public void Return() {
            lstQuery.Add(new Instruction(Instruction.ItrType.returnn));
        }

        /// <summary>Agrega una instruccion de declaracion de tabla</summary>
        public void Declare(string name) {
            Structure stu = new Structure(Structure.StrucOperation.declare, Structure.StrucType.table, name);
            lstQuery.Add(stu);
        }

        /// <summary>Agrega una instruccion de declaracion de variable</summary>
        public void Declare(Constructor.dbTyp type, Expression value, string name, params int[] lengths) {
            Structure stu = new Structure(Structure.StrucOperation.declare, Structure.StrucType.var, type, value, name, lengths);
            lstQuery.Add(stu);
        }

        /// <summary>Agrega una instruccion de declaracion de variable</summary>
        public void Declare(Constructor.dbTyp type, string name, params int[] lengths) {
            Structure stu = new Structure(Structure.StrucOperation.declare, Structure.StrucType.var, type, name, lengths);
            lstQuery.Add(stu);
        }

        /// <summary>Add a boolean condition with false part to the batch</summary>
        public void IF(Expression condition, IQryable truePart, IQryable falsePart) {
            BooleanCondition bol = new BooleanCondition(condition, truePart, falsePart);
            lstQuery.Add(bol);
        }

        /// <summary>Add a boolean condition to the batch</summary>
        public void IF(Expression condition, IQryable truePart) {
            IF(condition, truePart, null);
        }
    }
}
