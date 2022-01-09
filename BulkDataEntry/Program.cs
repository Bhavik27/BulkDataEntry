using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace BulkDataEntry
{
    class Program
    {
        static void Main(string[] args)
        {
            string line;
            int counter;
            string path = ConfigurationManager.AppSettings["PATH"];
            string dbConnection = ConfigurationManager.AppSettings["ConnectionString"];

            DirectoryInfo directoryInfo = new DirectoryInfo(path);
            DataTable _UserMaster = new DataTable("BULK_USERS");
            try
            {
                foreach (FileInfo fileInfo in directoryInfo.GetFiles())
                {
                    counter = 0;
                    _UserMaster = new DataTable("BULK_USERS");
                    StreamReader reader = new StreamReader(directoryInfo + fileInfo.Name);
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] column = line.Split('\t');
                        DataRow _UserMasterRow = _UserMaster.NewRow();

                        if (counter <= 0)
                        {
                            for (int i = 0; i < column.Length; i++)
                            {
                                string columnName = column[i];
                                _UserMaster.Columns.Add(columnName, typeof(string));    //Add column
                            }
                        }

                        if (counter > 0 && column.Length == _UserMaster.Columns.Count)
                        {
                            for (int i = 0; i < _UserMaster.Columns.Count; i++)
                            {
                                _UserMasterRow[i] = column[i].ToString();   //Insert value into column
                            }
                            _UserMaster.Rows.Add(_UserMasterRow);
                        }
                        counter++;
                    }
                    reader.Close();

                    if (_UserMaster.Rows.Count > 0)
                    {
                        Console.WriteLine(_UserMaster.Rows.Count);
                        InsertIntoDB(dbConnection, _UserMaster);
                    }
                    else
                    {
                        Console.WriteLine("No records found");
                    }
                    Console.WriteLine("Complete..");
                    Console.ReadLine();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                Console.ReadLine();
            }
        }

        public static void InsertIntoDB(string ConnectionString, DataTable dataTable)
        {
            using (SqlConnection sqlConnection = new SqlConnection(ConnectionString))
            {
                SqlTransaction sqlTransaction;
                sqlConnection.Open();
                sqlTransaction = sqlConnection.BeginTransaction();

                try
                {
                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, sqlTransaction))
                    {
                        sqlBulkCopy.DestinationTableName = dataTable.TableName;
                        foreach (var dataColumn in dataTable.Columns)
                        {
                            string column = dataColumn.ToString();
                            sqlBulkCopy.ColumnMappings.Add(column, column);
                        }
                        sqlBulkCopy.WriteToServer(dataTable);
                    }

                    sqlTransaction.Commit();
                }
                catch (Exception ex)
                {

                    sqlTransaction.Rollback();
                    Console.WriteLine(ex);
                    Console.ReadLine();
                }
            }
        }
    }
}
