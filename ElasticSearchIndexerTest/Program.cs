using System.Diagnostics;
using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Microsoft.Extensions.Configuration;

var config = new ConfigurationBuilder().AddJsonFile($"appsettings.json", true, true).Build();

const string AcmeIndexName = "acmedocs";

const int BatchCount = 1000;
const int BatchSize = 1000;


var settings = new ElasticsearchClientSettings(new Uri(config.GetValue<string>("ElasticSearchUrl") ?? throw new ArgumentNullException("ElasticSearchUrl")))
    .CertificateFingerprint(config.GetValue<string>("ElasticSearchFingerprint") ?? throw new ArgumentNullException("ElasticSearchFingerprint"))
    .Authentication(new ApiKey(config.GetValue<string>("ElasticSearchApiKey") ?? throw new ArgumentNullException("ElasticSearchApiKey")));

var client = new ElasticsearchClient(settings);



await UploadDocumentsAsync(client);


// await CheckConnectivityAsync();


static async Task UploadDocumentsAsync(ElasticsearchClient client)
{
    var text = await File.ReadAllTextAsync("../prodigalson.txt");

    var uploadCount = 0;
    var failedCount = 0;

    var stopWatch = Stopwatch.StartNew();

    await using var timer = new Timer(s =>
    {
        var dps = uploadCount / stopWatch.Elapsed.TotalSeconds;
        Console.WriteLine($"Uploaded: {uploadCount}, failed: {failedCount} after {stopWatch.Elapsed.TotalSeconds}s, dps: {dps}");
    }, null, 3000, 3000);


    await Parallel.ForAsync(0, BatchCount, new ParallelOptions { MaxDegreeOfParallelism = 4 }, async (batchIndex, token) =>
    {
        var documents = Enumerable.Range(0, BatchSize).Select(i =>
         {
             var id = batchIndex * BatchSize + i;
             var length = Random.Shared.Next(1000, 30000);
             var startIndex = Random.Shared.Next(text.Length - length);
             var textChunk = text[startIndex..(startIndex + length)];

             return new AcmeDocsIndexModel
             {
                 Id = id,
                 somebooleanfield = true,
                 someintfield = 42,
                 sometextfield = textChunk,
                 somedatefield = DateTime.UtcNow,
             };
         });

        var updateResponse = await client.IndexManyAsync(documents, index: AcmeIndexName, token);

        Interlocked.Add(ref uploadCount, updateResponse.Items.Count(o => o.IsValid));
        Interlocked.Add(ref failedCount, updateResponse.Items.Count(o => !o.IsValid));
    });
}


static async Task CheckDocumentReadAsync(ElasticsearchClient client)
{
    var response = await client.GetAsync<AcmeDocsIndexModel>(5, o => o.Index(AcmeIndexName));

    if (response.IsValidResponse)
    {
        var document = response.Source;
        Console.Write(JsonSerializer.Serialize(document));
    }
}


static async Task CheckConnectivityAsync(ElasticsearchClient client)
{
    Console.WriteLine("Checking connectivity...");
    var info = await client.InfoAsync();

    Console.WriteLine(JsonSerializer.Serialize(info));
    Console.WriteLine(info.IsValidResponse);
}


public class AcmeDocsIndexModel
{
    required public int Id { get; init; }
    required public string sometextfield { get; init; }
    required public bool somebooleanfield { get; init; }
    required public int someintfield { get; init; }
    required public DateTime somedatefield { get; init; }
}