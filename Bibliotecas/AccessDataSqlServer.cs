using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bibliotecas
{
    public class AccessDataSqlServer
    {
        private static volatile AccessDataSqlServer instance;
        private SqlConnection connection;
        private SqlTransaction transaction;
        private static object syncRoot = new object();
        SqlConnection sqlConnection;
        int revenda;

        public AccessDataSqlServer(int revenda)
        {

            this.revenda = revenda;
        }

        public SqlConnection createConnection()
        {

            SqlConnectionStringBuilder connBuilder = new SqlConnectionStringBuilder();

            try
            {
                if (revenda == 1)
                {
                    connBuilder.DataSource = "sql06.flexxone.com.br";
                    connBuilder.InitialCatalog = "AVANTESALES00111000";
                    connBuilder.IntegratedSecurity = false;
                    connBuilder.UserID = "sql111000";
                    connBuilder.Password = "11ch&con10";
                    connBuilder.ConnectTimeout = 100;
                }
                else
                {
                    connBuilder.DataSource = "sql06.flexxone.com.br";
                    connBuilder.InitialCatalog = "AVANTESALES00111002";
                    connBuilder.IntegratedSecurity = false;
                    connBuilder.UserID = "sql111002";
                    connBuilder.Password = "02#fi#TE11";
                    connBuilder.ConnectTimeout = 100;
                }

                return new SqlConnection(connBuilder.ToString());
            }
            catch (Exception ex)
            {
                throw new Exception("Erro na conexão com o Banco de dados. Detalhes:" + ex.Message);
            }
        }

        // parametros que vão para o banco
        public SqlParameterCollection sqlParameterCollection = new SqlCommand().Parameters;

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
            sqlParameterCollection.Add(new SqlParameter(nameParameter, valueParameter));
        }

        /// <summary>
        /// Método responsável pela persistencia dos dados - inserir, alterar, excluir
        /// Parâmetro (sqlText) query sql em forma de string
        /// retorna um comando sql
        /// </summary>
        /// <param name="sqlText">Parâmetro (sqlText) query sql em forma de string</param>
        /// <returns> retorna um comando sql</returns>

        public SqlDataReader executeSelect(string sqlTexto)
        {
            try
            {
                sqlConnection = createConnection();

                // abrir
                sqlConnection.Open();

                // criar o comando
                using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = sqlTexto;
                    sqlCommand.CommandTimeout = 3600;

                    // adicionar os parametros

                    foreach (SqlParameter sqlParameter in sqlParameterCollection)
                    {
                        sqlCommand.Parameters.Add(new SqlParameter(sqlParameter.ParameterName, sqlParameter.Value));
                    }

                    // executar o comando
                    return sqlCommand.ExecuteReader(); ;
                }

            }
            catch (Exception ex)
            {
                throw new Exception("Erro no select. Detalhes:" + ex.Message);
            }
        }

        public void fecharConexao()
        {

            if (sqlConnection.State == ConnectionState.Open)
            {
                sqlConnection.Close();
            }
        }

    }
}
