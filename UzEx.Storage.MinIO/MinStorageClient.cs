using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minio;

namespace UzEx.Storage.MinIO
{
    /// <summary>
    /// https://github.com/minio/minio-dotnet
    /// </summary>
    public class MinStorageClient
    {
        private readonly MinioClient _client;
        private readonly string _root;
        private readonly ILogger _logger;
        private const string CONNECTION_PREFIX = "min://";

        private void WriteLog(string s)
        {
            if (_logger != null)
            {
                _logger.LogInformation(s);
            }
            else
            {
                if (Debugger.IsAttached)
                {
                    Console.WriteLine(s);
                }
                else
                {
                    Trace.WriteLine(s);
                }
            }
        }

        /// <summary>
        ///  
        /// </summary>
        /// <param name="connectionString">min://host:port/access=secret/root or min://domain/access=secret/root</param>
        public MinStorageClient(string connectionString)
        {
            if (!connectionString.StartsWith(CONNECTION_PREFIX))
                throw new Exception("Connection string is wrong");

            var slices = connectionString.Replace(CONNECTION_PREFIX, "").Split(new[] { "/", "=" }, StringSplitOptions.RemoveEmptyEntries);

            if (slices.Length != 4)
                throw new Exception("Connection string is wrong");

            _root = slices[3];
            _client = new MinioClient(slices[0], slices[1], slices[2]);
        }

        /// <summary>
        ///  
        /// </summary>
        /// <param name="connectionString">min://host:port/access=secret/root or min://domain/access=secret/root</param>
        /// <param name="proxy"></param>
        public MinStorageClient(string connectionString, IWebProxy proxy) : this(connectionString)
        {
            _client.WithProxy(proxy);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionString">min://host:port/access=secret/root or min://domain/access=secret/root</param>
        /// <param name="logger"></param>
        public MinStorageClient(string connectionString, ILogger logger) : this(connectionString)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="url">host or domain</param>
        /// <param name="access"></param>
        /// <param name="secret"></param>
        /// <param name="root">if don't have automatic created</param>
        public MinStorageClient(string url, string access, string secret, string root)
        {
            _root = root;
            _client = new MinioClient(url, access, secret);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="url">host or domain</param>
        /// <param name="access"></param>
        /// <param name="secret"></param>
        /// <param name="root">if don't have automatic created</param>
        /// <param name="proxy"></param>
        public MinStorageClient(string url, string access, string secret, string root, IWebProxy proxy) : this(url, access, secret, root)
        {
            _client.WithProxy(proxy);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="url">host or domain</param>
        /// <param name="access"></param>
        /// <param name="secret"></param>
        /// <param name="root">if don't have automatic created</param>
        /// <param name="logger"></param>
        public MinStorageClient(string url, string access, string secret, string root, ILogger logger) : this(url, access, secret, root)
        {
            _logger = logger;
        }

        /// <summary>
        ///     Upload object to bucket from file
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <param name="fileData">file open read</param>
        /// <param name="contentType">mime type</param>
        /// <returns></returns>
        public async Task Upload(string bucketName, string objectName, Stream fileData, string contentType)
        {
            try
            {
                await _client.MakeBucketAsync(_root);

                WriteLog($"Created bucket {_root}");
            }
            catch (Exception e)
            {
                WriteLog($"Exception: {e}");
            }

            try
            {
                var bucketPath = $"{bucketName}/{objectName}";

                await _client.PutObjectAsync(_root, bucketPath, fileData, fileData.Length, contentType);

                WriteLog($"Uploaded object {objectName} from bucket {bucketName}");
            }
            catch (Exception e)
            {
                WriteLog($"Exception: {e}");
            }
        }

        /// <summary>
        ///     Download object from bucket into local file
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param> 
        /// <returns></returns>
        public async Task<Stream> Download(string bucketName, string objectName)
        {
            try
            {
                var destination = new MemoryStream();
                var bucketPath = $"{bucketName}/{objectName}";

                await _client.GetObjectAsync(_root, bucketPath, source =>
                {
                    source.CopyTo(destination);
                });
                destination.Position = 0;

                WriteLog($"Downloaded object {objectName} from bucket {bucketName}");

                return destination;
            }
            catch (Exception e)
            {
                WriteLog($"Exception: {e}");
            }

            return Stream.Null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public Task<IEnumerable<string>> Search(string bucketName, string prefix)
        {
            var list = new List<string>();

            try
            {
                var bucketPath = $"{bucketName}/{prefix}";
                var observable = _client.ListObjectsAsync(_root, bucketPath);
                var subscription = observable.Subscribe(item =>
                    {
                        list.Add(item.Key.Replace($"{bucketName}/", ""));

                        WriteLog($"OnNext: {item.Key}");
                    },
                    ex => WriteLog($"OnError: {ex}"),
                    () => WriteLog("OnCompleted!"));
                observable.Wait();

                WriteLog($"Listed all objects in bucket {_root} by '{prefix}' prefix");
                subscription.Dispose();
            }
            catch (Exception e)
            {
                WriteLog($"Exception: {e}");
            }

            return Task.FromResult<IEnumerable<string>>(list);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <returns></returns>
        public async Task Delete(string bucketName, string objectName)
        {
            try
            {
                var bucketPath = $"{bucketName}/{objectName}";

                await _client.RemoveObjectAsync(_root, bucketPath);

                WriteLog($"Deleted object {objectName} from bucket {bucketName}");

            }
            catch (Exception e)
            {
                WriteLog($"Exception: {e}");
            }
        }

    }
}