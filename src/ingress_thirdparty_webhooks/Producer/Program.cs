using System;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml;


Console.WriteLine("Initializing fake third party");

while (true)
{
    // Generate random XML data
    string xmlData = GenerateRandomXmlData();

    // Post XML data to API endpoint
    await PostStringDataToEndpoint(xmlData);

    Console.WriteLine($"XML data '${xmlData}' posted successfully- press return for another");
    Console.ReadLine();
}

static string GenerateRandomXmlData()
{
    Random random = new Random();

    // Generate a random number to determine if the XML data will be valid or invalid
    int randomNumber = random.Next(0, 2);

    if (randomNumber == 0)
    {
        // Generate valid XML data
        return "<root><data>Random XML data</data></root>";
    }
    else
    {
        // Generate invalid XML data
        return "<root><data>Random XML data</root>";
    }
}

static async Task PostXmlDataToEndpoint(string xmlData)
{
    using (HttpClient client = new HttpClient())
    {
        string endpointUrl = "http://localhost:5073/events";
        var content = new StringContent(xmlData, System.Text.Encoding.UTF8, "application/xml");
        var response = await client.PostAsync(endpointUrl, content);
        response.EnsureSuccessStatusCode();
    }
}

static async Task PostStringDataToEndpoint(string stringData)
{
    using (HttpClient client = new HttpClient())
    {
        string endpointUrl = "http://localhost:5073/events";
        var content = new StringContent(stringData, System.Text.Encoding.UTF8, "text/plain");
        var response = await client.PostAsync(endpointUrl, content);
        response.EnsureSuccessStatusCode();
    }
}
