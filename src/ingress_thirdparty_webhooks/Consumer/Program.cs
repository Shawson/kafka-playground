using System.Configuration;
using ReactiveEvents;

string? brokerList = ConfigurationManager.AppSettings["EH_FQDN"];
string? connectionString = ConfigurationManager.AppSettings["EH_CONNECTION_STRING"];
string? topic = ConfigurationManager.AppSettings["EH_NAME"];
string? caCertLocation = null; 
string? consumerGroup = ConfigurationManager.AppSettings["CONSUMER_GROUP"];

Console.WriteLine("Initializing Consumer");
Worker.Consumer(brokerList, connectionString, consumerGroup, topic, caCertLocation);
Console.ReadKey();

