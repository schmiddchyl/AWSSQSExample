using System;
using System.IO;
using System.Security.AccessControl;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Lambda.S3Events;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SQSConsumer
{
    public class SQSMessageConsumer
    {
        private AmazonSQSClient _sqsClient;
        private bool _isPolling;
        private int _delay;
        private string _queueUrl;
        private string _awsregion;
        private int _maxNumberOfMessages;
        private int _messageWaitTimeSeconds;
        private string _accessKey;
        private string _secret;
        private CancellationTokenSource _source;
        private CancellationToken _token;

        public SQSMessageConsumer()
        {
            _accessKey = Environment.GetEnvironmentVariable("AWS_KEY"); ;
            _secret = Environment.GetEnvironmentVariable("AWS_SECRET"); ;
            _queueUrl = "https://sqs.us-east-1.amazonaws.com/374255044811/sqsquepluss3demo";
            _awsregion = "us-east-1";
            _messageWaitTimeSeconds = 20;
            _maxNumberOfMessages = 10;
            _delay = 0;

            BasicAWSCredentials basicCredentials = new BasicAWSCredentials(_accessKey, _secret);
            RegionEndpoint region = RegionEndpoint.GetBySystemName(_awsregion);

            _sqsClient = new AmazonSQSClient(basicCredentials, region);
            Console.CancelKeyPress += new ConsoleCancelEventHandler(CancelKeyPressHandler);

        }

        public async Task Listen()
        {
            _isPolling = true;

            int i = 0;
            try
            {
                _source = new CancellationTokenSource();
                _token = _source.Token;

                while (_isPolling)
                {
                    i++;
                    Console.Write(i + ": ");
                    await FetchFromQueue();
                    Thread.Sleep(_delay);
                }
            }
            catch (TaskCanceledException ex)
            {
                Console.WriteLine("Application Terminated: " + ex.Message);
            }
            finally
            {
                _source.Dispose();
            }
        }

        private async Task FetchFromQueue()
        {

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
            receiveMessageRequest.QueueUrl = _queueUrl;
            receiveMessageRequest.MaxNumberOfMessages = _maxNumberOfMessages;
            receiveMessageRequest.WaitTimeSeconds = _messageWaitTimeSeconds;
            ReceiveMessageResponse receiveMessageResponse = await _sqsClient.ReceiveMessageAsync(receiveMessageRequest, _token);

            if (receiveMessageResponse.Messages.Count != 0)
            {
                for (int i = 0; i < receiveMessageResponse.Messages.Count; i++)
                {
                    string messageBody = receiveMessageResponse.Messages[i].Body;
                    var s3EventNotification = Amazon.S3.Util.S3EventNotification.ParseJson(messageBody);
                    
                    Console.WriteLine("Reading from S3 Update Queue: " + messageBody);
                    String key = s3EventNotification.Records[0].S3.Object.Key;
                    
                    await DeleteMessageAsync(receiveMessageResponse.Messages[i].ReceiptHandle);
                    s3Download(key);
                }
            }
            else
            {
                Console.WriteLine("No Messages to process");
            }
        }

        async private void s3Download(String objectName)
        {
            IAmazonS3 client = new AmazonS3Client(_accessKey, _secret, RegionEndpoint.USEast1);

            Console.WriteLine("Downloading document from S3");
            Console.WriteLine("writing to local file");
            var request = new GetObjectRequest
            {
                BucketName = "hcw-img",
                Key = objectName,
            };

            // Issue request and remember to dispose of the response
            using GetObjectResponse response = await client.GetObjectAsync(request);
            String filePath = @"C:\aws\finalout";
            try
            {
                // Save object to local file
                await response.WriteResponseStreamToFileAsync($"{filePath}\\{objectName}", true, CancellationToken.None);
                return ;
            }
            catch (AmazonS3Exception ex)
            {
                Console.WriteLine($"Error saving {objectName}: {ex.Message}");
                return ;
            }

        }

        private async Task DeleteMessageAsync(string recieptHandle)
        {

            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
            deleteMessageRequest.QueueUrl = _queueUrl;
            deleteMessageRequest.ReceiptHandle = recieptHandle;

            DeleteMessageResponse response = await _sqsClient.DeleteMessageAsync(deleteMessageRequest);

        }

        protected void CancelKeyPressHandler(object sender, ConsoleCancelEventArgs args)
        {
            args.Cancel = true;
            _source.Cancel();
            _isPolling = false;
        }

    }
}