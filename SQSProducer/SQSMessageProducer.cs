using System;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SQSProducer
{
    public class SQSMessageProducer
    {

        public SQSMessageProducer()
        {

        }

        public async Task Send(String message)
        {
            string accessKey = ""; 
            string secret = "";
            string queueUrl  = "https://sqs.us-east-1.amazonaws.com/374255044811/DocumentRequestQueue";
            bool useFifo = false;
            string messageGroupId = "";
            string awsregion = "us-east-1"; 

            BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secret);

            RegionEndpoint region = RegionEndpoint.GetBySystemName(awsregion);

            SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, message);

            if (useFifo)
            {
                sendMessageRequest.MessageGroupId = messageGroupId;
            }

            var sqsClient = new AmazonSQSClient(creds, region);

            await sqsClient.SendMessageAsync(sendMessageRequest);

        }

    }
}