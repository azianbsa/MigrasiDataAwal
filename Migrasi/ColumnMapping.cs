using MySqlConnector;

namespace Migrasi
{
    public class ColumnMapping
    {
        public int Id { get; set; }
        public string TableName { get; set; }
        public int SourceOrdinal { get; set; }
        public string DestinationColumn { get; set; }
    }

    public static class ColumnMappingExtention
    {
        public static MySqlBulkCopyColumnMapping ToMySqlBulkCopyColumnMapping(this ColumnMapping mapping)
        {
            return new MySqlBulkCopyColumnMapping(mapping.SourceOrdinal, mapping.DestinationColumn);
        }
    }
}
