using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Forms;
using MessageBox = System.Windows.Forms.MessageBox;
using TextBox = System.Windows.Controls.TextBox;

namespace Bibliotecas
{
    // Torna a classe pública para permitir acesso externo
    public class AccessDataPostgreSQL
    {
        private readonly string _connectionString;
        private readonly List<OdbcParameter> _sqlParameterCollection;

        public AccessDataPostgreSQL(int codRevenda)
        {
            // Inicializa a lista de parâmetros
            _sqlParameterCollection = new List<OdbcParameter>();

            // Define o nome do Data Source configurado no ODBC (32 bits ou 64 bits)
            _connectionString = codRevenda == 1 ? "DSN=VPS DATACERV X230" : "DSN=VPS DATACERV X690";
        }

        public AccessDataPostgreSQL(string schema)
        {
            // Inicializa a lista de parâmetros
            _sqlParameterCollection = new List<OdbcParameter>();

            // Define o nome do Data Source configurado no ODBC (32 bits ou 64 bits)
            _connectionString = schema == "x230" ? "DSN=VPS DATACERV X230" : "DSN=VPS DATACERV X690";
        }

        public AccessDataPostgreSQL()
        {
            // Inicializa a lista de parâmetros
            _sqlParameterCollection = new List<OdbcParameter>();

            // Define o nome do Data Source configurado no ODBC (32 bits ou 64 bits)
            _connectionString = "DSN=POWERBI";
        }

        /// <summary>
        /// Limpa os parâmetros armazenados.
        /// </summary>
        public void parameterClear()
        {
            _sqlParameterCollection.Clear();
        }

        /// <summary>
        /// Adiciona um parâmetro para a consulta SQL.
        /// </summary>
        public void addParameter(string nameParameter, object valueParameter)
        {
            // Garante que o nome do parâmetro não tenha o prefixo "@"
            if (nameParameter.StartsWith("@"))
                nameParameter = nameParameter.Substring(1);

            // Adiciona o parâmetro corretamente
            _sqlParameterCollection.Add(new OdbcParameter(nameParameter, valueParameter));
        }

        /// <summary>
        /// Executa uma consulta SELECT e retorna um OdbcDataReader.
        /// </summary>
        public OdbcDataReader executeSelect(string sqlTexto)
        {
            try
            {
                var sqlConnection = new OdbcConnection(_connectionString);
                sqlConnection.Open();

                // Substitui os parâmetros por placeholders
                foreach (OdbcParameter sqlParameter in _sqlParameterCollection)
                {
                    if (sqlTexto.Contains($"@{sqlParameter.ParameterName}"))
                    {
                        sqlTexto = sqlTexto.Replace($"@{sqlParameter.ParameterName}", "?");
                    }
                }

                var sqlCommand = new OdbcCommand(sqlTexto, sqlConnection)
                {
                    CommandTimeout = 900
                };

                // Adiciona os parâmetros na mesma ordem que aparecem na consulta
                foreach (OdbcParameter sqlParameter in _sqlParameterCollection)
                {
                    sqlCommand.Parameters.Add(sqlParameter);
                }              

                return sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (OdbcException ex)
            {
                throw new Exception($"Erro ao executar consulta no PostgreSQL via ODBC: {ex.Message}", ex);
            }
            finally
            {
                // Limpa os parâmetros para evitar conflitos futuros
                parameterClear();
            }
        }        

        public int executeInsert(string sqlTexto) => executeNonQuery(sqlTexto);
        public int executeDelete(string sqlTexto) => executeNonQuery(sqlTexto);
        public int executeUpdate(string sqlTexto) => executeNonQuery(sqlTexto);

        /// <summary>
        /// Executa um comando (INSERT, UPDATE, DELETE) e retorna o número de linhas afetadas.
        /// </summary>
        private int executeNonQuery(string sqlTexto)
        {
            try
            {
                using (var sqlConnection = new OdbcConnection(_connectionString))
                {
                    sqlConnection.Open();

                    // Substitui os parâmetros por placeholders
                    foreach (OdbcParameter sqlParameter in _sqlParameterCollection)
                    {
                        if (sqlTexto.Contains($"@{sqlParameter.ParameterName}"))
                        {
                            sqlTexto = sqlTexto.Replace($"@{sqlParameter.ParameterName}", "?");
                        }
                    }

                    using (OdbcCommand sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = sqlTexto;
                        sqlCommand.CommandTimeout = 900;

                        foreach (OdbcParameter sqlParameter in _sqlParameterCollection)
                        {
                            sqlCommand.Parameters.Add(new OdbcParameter(sqlParameter.ParameterName, sqlParameter.Value));
                        }

                       // MessageBox.Show(sqlTexto);

                        return sqlCommand.ExecuteNonQuery(); // número de linhas afetadas
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Erro no comando (INSERT/DELETE/UPDATE). Detalhes:" + ex.Message);
            }
        }

        public int executeInsertOrUpdate(string tableName, string conditionSql, string insertSql, string updateSql)
        {
            try
            {
                if (updateSql != null)
                {

                    string existsSql = $"SELECT COUNT(1) FROM {tableName} WHERE {conditionSql}";
                    int count = 0;

                    using (var sqlConnection = new OdbcConnection(_connectionString))
                    {
                        sqlConnection.Open();
                    
                            using (OdbcCommand cmd = sqlConnection.CreateCommand())
                            {

                                cmd.CommandText = existsSql.ToLower();

                                foreach (OdbcParameter param in _sqlParameterCollection)
                                {
                                    cmd.Parameters.Add(new OdbcParameter(param.ParameterName, param.Value));
                                }

                                count = Convert.ToInt32(cmd.ExecuteScalar());
                            }
                    }

                    if (count > 0)
                    {
                        //Reorganizar parâmetros: mover o do WHERE para o final
                        //string whereParamName = ObterNomeColunaDoWhere(conditionSql);
                        MoverParametroParaFinal(ObterNomeColunaDoWhere(conditionSql));

                        return executeUpdate(updateSql);
                    }
                    else
                    {
                        return executeInsert(insertSql);
                    }
                }
                else
                {
                    return executeInsert(insertSql);
                }
                
            }
            catch (Exception ex)
            {
                throw new Exception("Erro no InsertOrUpdate. Detalhes: " + ex.Message);
            }
        }

        public void dropAndCreateTable(string schema, string nomeTabela, DataTable dataTable, string primaryKey )
        {
            try
            {
                using (var sqlConnection = new OdbcConnection(_connectionString))
                {
                    sqlConnection.Open();

                    // 1. Verificar se a tabela existe
                    string sqlCheck = $@"
                                        SELECT EXISTS (
                                                        SELECT 1 
                                                        FROM   information_schema.tables 
                                                        WHERE  table_schema = '{schema}' 
                                                               AND table_name = '{nomeTabela.ToLower()}'
                                                      )";

                    using (var checkCmd = new OdbcCommand(sqlCheck, sqlConnection))
                    {
                        int existe = Convert.ToInt32(checkCmd.ExecuteScalar());

                        if ( Convert.ToBoolean( existe ) )
                        {
                            // 2. Drop table
                            string dropSql = $"DROP TABLE IF EXISTS {schema}.{nomeTabela} CASCADE;";
                            using (var dropCmd = new OdbcCommand(dropSql, sqlConnection))
                                dropCmd.ExecuteNonQuery();
                        }
                    }

                    // 3. Gerar script CREATE TABLE baseado no DataTable
                    string createSql = $"CREATE TABLE {schema}.{nomeTabela} (\n";

                    foreach (DataColumn col in dataTable.Columns)
                    {
                        string colName = col.ColumnName.ToLower(); // força minúsculo
                        string colType = GetPostgreSqlType(col.DataType);

                        // Identidade
                        if (col.ColumnName.Equals(primaryKey, StringComparison.OrdinalIgnoreCase) )
                            colType = "SERIAL";

                        createSql += $"    {colName} {colType},\n";

                    }

                    if (primaryKey != null)
                    {
                        createSql += $"    PRIMARY KEY (\"{primaryKey}\")\n";
                        createSql += ");";
                    }


                    createSql = createSql.TrimEnd(',', '\n') + "\n);";

                    //4.Criar a nova tabela
                    using (var createCmd = new OdbcCommand(createSql, sqlConnection))
                    {
                        createCmd.ExecuteNonQuery();
                    }

                    // 5. Conceder permissões ao usuário POWERBI
                    string grantSql = $"GRANT ALL PRIVILEGES ON TABLE {schema}.{nomeTabela} TO powerbi;";
                    using (var grantCmd = new OdbcCommand(grantSql, sqlConnection))
                    {
                        grantCmd.ExecuteNonQuery();
                    }

                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Erro ao criar a tabela {nomeTabela}: {ex.Message}");
            }
        }

        public void dropTable(string schema, string nomeTabela, DataTable dataTable, string primaryKey)
        {
            try
            {
                using (var sqlConnection = new OdbcConnection(_connectionString))
                {
                    sqlConnection.Open();

                    // Garante que sessões futuras sejam read - write
                    //using (var cmdSetSessionRW = new OdbcCommand("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;", sqlConnection))
                    //    cmdSetSessionRW.ExecuteNonQuery();

                    // 1. Verificar se a tabela existe
                    string sqlCheck = $@"
                                        SELECT EXISTS (
                                                        SELECT 1 
                                                        FROM   information_schema.tables 
                                                        WHERE  table_schema = {schema}
                                                               AND table_name = '{nomeTabela.ToLower()}'
                                                      )";

                    using (var checkCmd = new OdbcCommand(sqlCheck, sqlConnection))
                    {
                        int existe = Convert.ToInt32(checkCmd.ExecuteScalar());

                        if (Convert.ToBoolean(existe))
                        {
                            // Garantir que a sessão está em modo de escrita
                            using (var cmdSetRW = new OdbcCommand("SET TRANSACTION READ WRITE;", sqlConnection))
                                cmdSetRW.ExecuteNonQuery();


                            // 2. Drop table
                            string dropSql = $"DROP TABLE IF EXISTS {schema}.{nomeTabela} CASCADE;";
                            using (var dropCmd = new OdbcCommand(dropSql, sqlConnection))
                                dropCmd.ExecuteNonQuery();
                        }
                    }                    
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Erro ao criar a tabela {nomeTabela.ToUpper()}: {ex.Message}");
            }
        }

        public Boolean isExist(string schema, string tableName, string conditionSql) {

            try
            {
                    string existsSql = $"SELECT COUNT(1) FROM {schema}.{tableName} WHERE {conditionSql}";
                    int count = 0;

                    using (var sqlConnection = new OdbcConnection(_connectionString))
                    {
                        sqlConnection.Open();

                        using (OdbcCommand cmd = sqlConnection.CreateCommand())
                        {

                            cmd.CommandText = existsSql;
                            
                            count = Convert.ToInt32(cmd.ExecuteScalar());                      

                        }
                    }                                      

                    if (count > 0)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }                                

            }
            catch (Exception ex)
            {
                throw new Exception("Erro no isExist. Detalhes: " + ex.Message);
            }

        }

        private string GetPostgreSqlType(Type type)
        {
            if (type == typeof(string))
                return "VARCHAR";
            if (type == typeof(int) || type == typeof(Int16) || type == typeof(Int32))
                return "INTEGER";
            if (type == typeof(long) || type == typeof(Int64))
                return "BIGINT";
            if (type == typeof(decimal) || type == typeof(double) || type == typeof(float))
                return "NUMERIC";
            if (type == typeof(bool))
                return "BOOLEAN";
            if (type == typeof(DateTime))
                return "TIMESTAMP";
            return "TEXT"; // fallback para tipos não mapeados
        }

        private string ObterNomeColunaDoWhere(string conditionSql)
        {
            // Ex: "tbl_acoesid = ?" → retorna "tbl_acoesid"
            var partes = conditionSql.Split('=');
            return partes[0].Trim().Trim('"');
        }

        private void MoverParametroParaFinal(string nomeParametro)
        {
            var index = _sqlParameterCollection
                .Cast<OdbcParameter>()
                .ToList()
                .FindIndex(p => p.ParameterName.Equals("@" + nomeParametro, StringComparison.OrdinalIgnoreCase));

            //MessageBox.Show(nomeParametro + "    " + index);

            index = 0;

            //if (index >= 0)
            //{
                var parametro = _sqlParameterCollection[index];
                _sqlParameterCollection.RemoveAt(index);
                _sqlParameterCollection.Add(parametro); // move para o final
            //}
        }

    }
}
