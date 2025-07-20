from openaq import OpenAQ

client = OpenAQ(api_key="key here")
client.parameters.latest(parameters_id=2, limit=1000)
client.close()