using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bibliotecas
{
    public class AccessDataMySql
    {
        public MySqlConnection createConnection()
        {
            try
            {
                var connString = "Server=192.168.0.8;Database=producao;Uid=etl;Pwd=etl";

                var connection = new MySqlConnection(connString);

                return connection;
            }
            catch (Exception ex)
            {
                throw new Exception("Erro na conexão com o Banco de dados. Detalhes:" + ex.Message);
            }
        }

        public MySqlParameterCollection sqlParameterCollection = new MySqlCommand().Parameters;

        /// <summary>
        /// Método responsável por limpar os dados referentes aos parametros
        /// </summary>
        public void parameterClear()
        {
            sqlParameterCollection.Clear();
        }

        /// <summary>
        /// Método responsável por atribuir valores aos parametros
        /// </summary>
        /// <param name="nameParameter">Nome do parametro que vai ser usado na query sql ("@valor")</param>
        /// <param name="valueParameter">Valor que é atribuído ao parametro que vai ser usado na query sql (objeto.valor)</param>
        public void addParameters(string nameParameter, object valueParameter)
        {
            sqlParameterCollection.Add(new MySqlParameter(nameParameter, valueParameter));
        }

        /// <summary>
        /// Método responsável pelas consultas na base de dados
        /// param sqlText uma query sql em forma de string
        /// return  SqlDataReader
        /// </summary>
        /// <param name="sqlTexto">Parametro (sqlText) uma string contendo uma query sql</param>
        /// <returns>SqlDataReader</returns>
        public MySqlDataReader executeSelect(string sqlTexto)
        {
            try
            {

                // criar conexao
                MySqlConnection sqlConnection = createConnection();

                // abrir
                sqlConnection.Open();
                // criar o comando
                using (MySqlCommand sqlCommand = sqlConnection.CreateCommand())
                {



                    //sqlCommand.CommandType = commandType;
                    sqlCommand.CommandText = sqlTexto;
                    sqlCommand.CommandTimeout = 900;
                    sqlCommand.Connection = sqlConnection;


                    // adicionar os parametros

                    foreach (MySqlParameter sqlParameter in sqlParameterCollection)
                    {
                        sqlCommand.Parameters.Add(new MySqlParameter(sqlParameter.ParameterName, sqlParameter.Value));
                    }


                    // buscar os dados
                    MySqlDataReader data = sqlCommand.ExecuteReader();
                    return data;

                }

            }
            catch (Exception ex)
            {
                throw new Exception("Erro no select. Detalhes:" + ex.Message);
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
                using (MySqlConnection sqlConnection = createConnection())
                {
                    sqlConnection.Open();
                    using (MySqlCommand sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = sqlTexto;
                        sqlCommand.CommandTimeout = 900;
                        sqlCommand.Connection = sqlConnection;

                        foreach (MySqlParameter sqlParameter in sqlParameterCollection)
                        {
                            sqlCommand.Parameters.Add(new MySqlParameter(sqlParameter.ParameterName, sqlParameter.Value));
                        }

                        return sqlCommand.ExecuteNonQuery(); // número de linhas afetadas
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Erro no comando (INSERT/DELETE/UPDATE). Detalhes:" + ex.Message);
            }
        }

        /// <summary>
        /// Método que insere ou atualiza um registro dependendo se ele existe.
        /// </summary>
        public int executeInsertOrUpdate(string tableName, string conditionSql, string insertSql, string updateSql)
        {
            try
            {
                // 1 - Verificar se o registro já existe
                string existsSql = $"SELECT COUNT(1) FROM {tableName} WHERE {conditionSql}";
                int count = 0;

                using (MySqlConnection conn = createConnection())
                {
                    conn.Open();
                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = existsSql;
                        foreach (MySqlParameter p in sqlParameterCollection)
                        {
                            cmd.Parameters.Add(new MySqlParameter(p.ParameterName, p.Value));
                        }
                        count = Convert.ToInt32(cmd.ExecuteScalar());
                    }
                }

                // 2 - Decidir entre INSERT ou UPDATE
                if (count > 0)
                    return executeUpdate(updateSql);
                else
                    return executeInsert(insertSql);
            }
            catch (Exception ex)
            {
                throw new Exception("Erro no InsertOrUpdate. Detalhes: " + ex.Message);
            }
        }
    }
}
