# Social Media Copy

Replace `BLOG_URL` with the published Uber Engineering blog URL. Third person throughout, so either account can post it unchanged.

## LinkedIn

Uber's CLP platform compresses 25 PB of logs a month into 750-850 TB, about 97% less. Five Presto Native workers serve SQL over it: hundreds of weekly active users, thousands of successful queries a day, ~2s mean response.

Across a 30-day production window, successful queries' time ranges covered more than 200,000x as many events as the system ultimately examined.

More than half of those queries already come from agents. Compression itself materializes each event's schema and recurring message formats, so proposed SQL functions would let an agent learn what a slice holds without reading events. The idea reaches past logs to the wide event: logs, traces, and metrics in one record.

Part III of Uber's CLP series, with YScope and the PrestoDB community.

[BLOG_URL](BLOG_URL)

#UberEngineering #PrestoDB #Observability #AIAgents

## X / Twitter

Uber compresses 25 PB of logs a month into 750-850 TB and serves SQL over it with five Presto Native workers, ~2s mean response. More than half of those queries come from agents.

Part III: [BLOG_URL](BLOG_URL)
