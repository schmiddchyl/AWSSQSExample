using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
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
             _accessKey = Environment.GetEnvironmentVariable("AWS_KEY"); 
             _secret = Environment.GetEnvironmentVariable("AWS_SECRET");
            _queueUrl = "https://sqs.us-east-1.amazonaws.com/374255044811/DocumentRequestQueue";
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
            try{
                _source = new CancellationTokenSource();
                _token = _source.Token; 
                
                while(_isPolling){
                    i++;
                    Console.Write(i + ": ");
                    await FetchFromQueue();
                    Thread.Sleep(_delay);
                }                   
            }
            catch(TaskCanceledException ex){
                Console.WriteLine("Application Terminated: " + ex.Message); 
            }
            finally{
                _source.Dispose();
            }                
        }

        private async Task FetchFromQueue(){
                
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

                    Console.WriteLine("Reading Message from Queue, Uploading to S3: " + messageBody);

                    await DeleteMessageAsync(receiveMessageResponse.Messages[i].ReceiptHandle);
                    s3Upload();
                }
            }
            else{
                Console.WriteLine("No Messages to process"); 
            }                      
        }   
        
       async private void s3Upload()
        {
            IAmazonS3 client = new AmazonS3Client(_accessKey, _secret, RegionEndpoint.USEast1);

            FileInfo file = new FileInfo(@"c:\app.js");
            string destPath = "folder/sub-folder/app.js.txt"; // <-- low-level s3 path uses /
            var putRequest2 = new PutObjectRequest
            {
                BucketName = "hcw-img",
                
                Key = "app.js",
                FilePath = @"c:\AWS\sourcefile\app.js",
                ContentType = "text/plain"
                
            };

            putRequest2.Metadata.Add("x-amz-meta-title", "someTitle");
            PutObjectResponse response2 = await client.PutObjectAsync(putRequest2);
       
        }

        private async Task DeleteMessageAsync(string recieptHandle)
        {

            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
            deleteMessageRequest.QueueUrl = _queueUrl;
            deleteMessageRequest.ReceiptHandle = recieptHandle;

            DeleteMessageResponse response = await _sqsClient.DeleteMessageAsync(deleteMessageRequest);

        }

        protected void CancelKeyPressHandler(object sender, ConsoleCancelEventArgs args){
            args.Cancel = true;
            _source.Cancel();
            _isPolling = false;                              
        }
 
    }
}