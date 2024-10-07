# Process events passed to the system from a third party system via a web hook

Webhooks are unreliable, message formats from third party systems cannot be relied upon and there is no concept of a delivered "at least once".

This sample has three parts;
- a simulated third party system which fires some xml at the API endpoint- sometimes valid, sometimes now
- an api project which recieves the xml pushes it straight into kafka
- a consumer project which recieves messages from kafka and writes them into the log