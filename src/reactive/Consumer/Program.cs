using System.Configuration;
using ReactiveEvents;

string? brokerList = ConfigurationManager.AppSettings["EH_FQDN"];
string? connectionString = ConfigurationManager.AppSettings["EH_CONNECTION_STRING"];
string? topic = ConfigurationManager.AppSettings["EH_NAME"];
string? caCertLocation = null; // ConfigurationManager.AppSettings["CA_CERT_LOCATION"];
string? consumerGroup = ConfigurationManager.AppSettings["CONSUMER_GROUP"];

Console.WriteLine("Initializing Consumer");
ReactiveWorker.Consumer(brokerList, connectionString, consumerGroup, topic, caCertLocation);
Console.ReadKey();

