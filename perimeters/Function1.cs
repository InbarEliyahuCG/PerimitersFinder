using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Configuration;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace perimeters
{
    public class PerimetersFinder
    {
        private readonly ILogger<PerimetersFinder> _logger;
        private readonly IConfiguration _configuration;

        public PerimetersFinder(ILogger<PerimetersFinder> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        [Function("PerimetersFinder")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req ,[FromQuery] string segment)
        {
            if (string.IsNullOrEmpty(segment))
            {
                _logger.LogInformation("Segment code is required.");
                return new BadRequestResult();
            }
            _logger.LogInformation("C# HTTP trigger function processed a request.");
            string connectionString = _configuration["SqlConnectionString"];
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                string query = "SELECT vdp.* FROM cg_dwh.dbo.vw_dim_perimeter vdp JOIN cg_operative.dbo.perimeter_type pt ON vdp.perimeter_type = pt.name AND pt.record_deleted = 0 JOIN cg_operative.dbo.segment_perimeter_type_mapping sptp ON pt.ID = sptp.perimeter_type_id AND sptp.record_deleted = 0 JOIN cg_dwh.dbo.dim_first_level_segment dfls ON sptp.segment_type_id = dfls.id WHERE dfls.segment_code = @SegmentCode and is_deleted = 0 ORDER BY vdp.display_name";
                SqlCommand command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@SegmentCode", segment);

                SqlDataReader reader = await command.ExecuteReaderAsync();
                var results = new List<Dictionary<string, object>>();

                // Process the results
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        object value = reader.IsDBNull(i) ? "null" : reader.GetValue(i);
                        row[reader.GetName(i)] = value;
                    }
                    var mapping = GenerateDynamicMapping(row);
                    var reorderedRow = ReorderProperties(row, mapping);
                    results.Add(reorderedRow);
                    _logger.LogInformation($"Fetched JSON count: {reader.FieldCount}");
                }
                // Serialize the list of dictionaries to JSON
                string jsonString = JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true });
                return new OkObjectResult(results);
            }
        }
        public Dictionary<string, int> GenerateDynamicMapping(Dictionary<string, object> row)
        {
            var customOrder = new List<string>
                {
                    "perimeter_id",
                    "city",
                    "country",
                    "source",
                    "deleted",
                    "perimeter_name",
                    "perimeter_type",
                    "perimeter_code",
                    "country_code",
                    "customer_code",
                    "longitude",
                    "latitude",
                    "radius",
                    "cms_perimeter_country",
                    "full_string",
                    "display_name"};

            // Create a mapping based on the custom order
            var mapping = customOrder
                .Select((key, index) => new { key, index })
                .Where(predicate: k => row.ContainsKey(k.key)) // Ensure the key exists in the row
                .ToDictionary(keySelector: kv => kv.key, elementSelector: kv => kv.index + 1);

            // Add any remaining keys that were not specified in the custom order at the end
            int nextIndex = customOrder.Count + 1;
            foreach (var key in row.Keys.Except(customOrder))
                mapping[key] = nextIndex++;
            
            return mapping;
        }
         
        public Dictionary<string, object> ReorderProperties(Dictionary<string, object> row, Dictionary<string, int> mapping)
        {
            // Reorder the dictionary based on the mapping and return it
            return row.OrderBy(kvp => mapping[kvp.Key])
                      .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }
    }
}
