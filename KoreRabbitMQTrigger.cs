using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EON.Function
{

  public class KoreCertificateMessage
  {
    public string msgId { get; set; }
    public string transferHash { get; set; }
    public string status { get; set; }
    public string brand { get; set; }
    public string equipmentNumber { get; set; }
    public string type { get; set; }
  }
  public static class KoreRabbitMQTrigger
  {
    private static readonly string DATABASE_NAME = "digital_certificate";
    private static readonly string KORE_TRANSACTION_CONTAINER = "KoreNodeTransaction";

    [FunctionName("KoreRabbitMQTrigger")]
    public static async Task Run([RabbitMQTrigger("node-message-certificate", ConnectionStringSetting = "KORE_RABBITMQ_CONNECTION_STRING")] string message, ILogger log)
    {
      log.LogInformation($"C# RabbitMQ queue trigger function processed message: {message}");

      string jsonMessage;
      // Try to parse our JSON content of an RMQTextMessage by simple string split
      if (message.Contains("RMQTextMessage"))
      {
        int jsonIndex = message.IndexOf("{");
        jsonMessage = message.Substring(jsonIndex);
        log.LogInformation($"Parsing JSON from JMS Message. JsonIndex: {jsonIndex}, JsonMessage: {jsonMessage}");
      }
      else
      {
        jsonMessage = message;
      }

      KoreCertificateMessage certificateMessage = JsonConvert.DeserializeObject<KoreCertificateMessage>(jsonMessage);

      // When we see a success message for a transfer, look up the transfer request and push it along by messaging back to the digital cert app (http post.. KISS for now)
      if ("SUCCESS".Equals(certificateMessage.status))
      {
        // Read row from cosmos db with id: certificateMessage.transferHash
        log.LogInformation($"Reading transaction: {certificateMessage.transferHash} from CosmosDB");

        KoreTransaction transaction = null;
        try
        {
          transaction = await ReadTransactionFromDB(certificateMessage.transferHash);
        }
        catch (Exception ex)
        {
          log.LogError($"Error reading transaction from CosmosDB: {ex.Message}");
        }


        if (transaction == null)
        {
          //failed to write or transaction expired (TTL is 24 hours on DB row)
          log.LogError($"Failed to read transaction: {certificateMessage.transferHash} from CosmosDB");
          return;
        }

        log.LogInformation($"Sending message to digital cert API for transaction: {certificateMessage.transferHash}");

        try
        {
          await SendMessageToDigitalCertApi(transaction);
        }
        catch (Exception ex)
        {
          log.LogError($"Error sending message to digital certificate API: {ex.Message}");
        }

      }
    }


    public class KoreTransaction
    {
      public string id { get; set; }
      public string certificateToken { get; set; }
    }

    private static async Task<KoreTransaction> ReadTransactionFromDB(string transactionHash)
    {
      using CosmosClient client = new(
        accountEndpoint: Environment.GetEnvironmentVariable("COSMOS_ENDPOINT")!,
        authKeyOrResourceToken: Environment.GetEnvironmentVariable("COSMOS_KEY")!
      );

      Database database = client.GetDatabase(DATABASE_NAME);
      Container container = database.GetContainer(KORE_TRANSACTION_CONTAINER);
      return await container.ReadItemAsync<KoreTransaction>(id: transactionHash, partitionKey: new PartitionKey(transactionHash));
    }

    private static async Task SendMessageToDigitalCertApi(KoreTransaction transaction)
    {
      using HttpClient client = new()
      {
        BaseAddress = new Uri(Environment.GetEnvironmentVariable("DIGITAL_CERT_APP_URL"))
      };
      using StringContent jsonContent = new(
        JsonConvert.SerializeObject(new
        {
          transactionHash = transaction.id,
          ct = transaction.certificateToken
        }),
        Encoding.UTF8,
        "application/json");

      using HttpResponseMessage response = await client.PostAsync("api/cartier/process-message", jsonContent);
      response.EnsureSuccessStatusCode();
    }
  }
}
