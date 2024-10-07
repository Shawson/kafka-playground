using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel;
using System.Xml;

namespace API.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class EventsController : ControllerBase
    {

        private readonly ILogger<EventsController> _logger;
        private readonly string? _brokerList = null;
        private readonly string? _connectionString = null;
        private readonly string? _topic = null;
        private readonly string? _caCertLocation = null;
        private readonly string? _consumerGroup = null;
        private readonly IConfiguration _configuration;

        public EventsController(ILogger<EventsController> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            _brokerList = _configuration["EH_FQDN"];
            _connectionString = _configuration["EH_CONNECTION_STRING"];
            _topic = _configuration["EH_NAME"];
            _caCertLocation = null;
            _consumerGroup = _configuration["CONSUMER_GROUP"];
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] string eventPayload)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _brokerList,
                SecurityProtocol = SecurityProtocol.Plaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = _connectionString,
                //Debug = "security,broker,protocol"        //Uncomment for librdkafka debugging information
            };

            using (var producer = new ProducerBuilder<long, string>(config).SetKeySerializer(Serializers.Int64).SetValueSerializer(Serializers.Utf8).Build())
            {
                var deliveryReport = await producer.ProduceAsync(_topic, new Message<long, string> { Key = DateTime.Now.Ticks, Value = eventPayload });
            }

            return Ok();
        }
    }
}
