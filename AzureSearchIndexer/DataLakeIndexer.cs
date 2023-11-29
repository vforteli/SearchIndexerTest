﻿using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Search.Documents;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Extensions.Logging;

namespace AzureSearchIndexer;

public class DataLakeIndexer(SearchClient searchClient, ILogger<DataLakeIndexer> logger)
{
    private const int MaxReadThreads = 128;
    private const int DocumentBatchSize = 1000;    // according to documentation this is the max batch size... although it seems to work with higher values... they dont bring performance benefits though


    /// <summary>
    /// Run document indexer with a function for mapping 
    /// </summary>
    public async Task<IndexerRunMetrics> RunDocumentIndexerOnPathsAsync<TIndex>(DataLakeServiceClient dataLakeServiceClient, IAsyncEnumerable<PathIndexModel> paths, Func<PathIndexModel, FileDownloadInfo, Task<TIndex?>> func, CancellationToken cancellationToken)
    {
        var pathsBuffer = new BlockingCollection<PathIndexModel>();

        var listPathsTask = Task.Run(async () =>
        {
            await foreach (var path in paths)
            {
                pathsBuffer.Add(path);
            }

            pathsBuffer.CompleteAdding();
        }, cancellationToken);


        var documents = new BlockingCollection<TIndex>();

        var documentReadCount = 0;
        var documentReadFailedCount = 0;

        var documentUploadCount = 0;
        var documentUploadFailedCount = 0;
        var documentUploadCreatedCount = 0;
        var documentUploadModifiedCount = 0;


        var stopwatch = Stopwatch.StartNew();

        var readDocumentsTask = Task.Run(async () =>
        {
            using var timer = new Timer(s => { logger.LogInformation("Read {documentsReadCount} documents... {dps} fps", documentReadCount, documentReadCount / (stopwatch.ElapsedMilliseconds / 1000f)); }, null, 3000, 3000);

            var readTasks = new ConcurrentDictionary<Guid, Task>();

            using var semaphore = new SemaphoreSlim(MaxReadThreads, MaxReadThreads);

            try
            {
                while (pathsBuffer.TryTake(out var path, Timeout.Infinite, cancellationToken))
                {
                    await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                    var taskId = Guid.NewGuid();
                    readTasks.TryAdd(taskId, Task.Run(async () =>
                    {
                        try
                        {
                            var file = await dataLakeServiceClient.GetFileSystemClient(path.filesystem).GetFileClient(path.path).ReadAsync(cancellationToken).ConfigureAwait(false);

                            var document = await func.Invoke(path, file.Value);
                            if (document != null)
                            {
                                documents.Add(document);
                                Interlocked.Increment(ref documentReadCount);
                            }
                        }
                        catch (TaskCanceledException) { }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref documentReadFailedCount);
                            logger.LogError(ex, "Failed deserializing document {path}", path.path);
                        }
                        finally
                        {
                            readTasks.TryRemove(taskId, out _);
                            semaphore.Release();
                        }
                    }, cancellationToken));
                }

                await Task.WhenAll(readTasks.Select(o => o.Value));
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "Something went horribly wrong reading documents");
            }
            finally
            {
                documents.CompleteAdding();
            }
        }, cancellationToken);

        var uploadDocumentsTask = Task.Run(async () =>
        {
            Task UploadBatchAsync(IReadOnlyList<TIndex> batch) => Task.Run(async () =>
            {
                try
                {
                    var currentCount = Interlocked.Add(ref documentUploadCount, batch.Count);
                    logger.LogInformation("Sending batch with {bufferCount} documents, total: {currentCount}", batch.Count, currentCount);

                    // todo handle retries? failed documents?
                    var response = await searchClient.MergeOrUploadDocumentsAsync(batch, cancellationToken: cancellationToken).ConfigureAwait(false);

                    Interlocked.Add(ref documentUploadCreatedCount, response.Value.Results.Count(o => o.Status == 201));
                    Interlocked.Add(ref documentUploadModifiedCount, response.Value.Results.Count(o => o.Status == 200));
                    Interlocked.Add(ref documentUploadFailedCount, response.Value.Results.Count(o => o.Status >= 400));

                    logger.LogInformation("Status: {status} for batch at {currentCount}", response.GetRawResponse().Status, currentCount);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Uh oh, sending batch failed...");
                }
            });

            var sendTasks = new ConcurrentBag<Task>();
            var buffer = new List<TIndex>(DocumentBatchSize);

            while (!documents.IsCompleted)
            {
                if (documents.TryTake(out var document, -1))
                {
                    buffer.Add(document);
                }

                if (buffer.Count == DocumentBatchSize || documents.IsCompleted)   // actually this should also check the size of the batch, it should be max 1000 items or 16 MB
                {
                    sendTasks.Add(UploadBatchAsync(buffer.AsReadOnly()));
                    buffer = new List<TIndex>(DocumentBatchSize);
                }
            }

            await Task.WhenAll(sendTasks).ConfigureAwait(false);
        }, cancellationToken);


        await Task.WhenAll(readDocumentsTask, uploadDocumentsTask).ConfigureAwait(false);
        logger.LogInformation("Indexing done, took {elapsed}", stopwatch.Elapsed);

        return new IndexerRunMetrics
        {
            DocumentReadCount = documentReadCount,
            DocumentReadFailedCount = documentReadFailedCount,
            DocumentUploadCount = documentUploadCount,
            DocumentUploadCreatedCount = documentUploadCreatedCount,
            DocumentUploadFailedCount = documentUploadFailedCount,
            DocumentUploadModifiedCount = documentUploadModifiedCount,
        };
    }
}
