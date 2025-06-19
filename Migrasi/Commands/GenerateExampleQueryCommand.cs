using Dapper;
using Microsoft.Data.Sqlite;
using Migrasi.Helpers;
using Spectre.Console.Cli;
using SQLitePCL;

namespace Migrasi.Commands
{
    public class GenerateExampleQueryCommand : AsyncCommand<GenerateExampleQueryCommand.Settings>
    {
        public class Settings : CommandSettings { }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            await Utils.TrackProgress("Generate contoh query", async () =>
            {
                Batteries.Init();
                await using var conn = new SqliteConnection("Data Source=column_mapping_configurations.db");

                try
                {
                    await conn.OpenAsync();
                    var tableQuery = await conn.QueryAsync(@"
select TableName,concat('select',char(10),group_concat(DestinationColumn,','||char(10)),char(10),'from table') as Query
from (
	select * from column_mappings ORDER by TableName,SourceOrdinal
)
group by TableName");
                    string output = "example_queries";
                    if (Directory.Exists(output))
                    {
                        Directory.Delete(output, true);
                    }
                    Directory.CreateDirectory(output);
                    foreach (var table in tableQuery)
                    {
                        File.WriteAllText(Path.Combine(output, table.TableName + ".sql"), table.Query);
                    }
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    await conn.CloseAsync();
                }
            });

            return 0;
        }
    }
}
