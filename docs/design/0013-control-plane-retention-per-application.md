# 0013 — Control plane: retention per application, archive hours and data stream lifecycles

**Status:** accepted

Logs are not kept forever, and different applications need them for different times. The retention file gives each
application two periods per table (logs, events): `search`, how long Elasticsearch keeps them, and `archive`, how long
the Parquet archive does - the index is the short and expensive window people search, the archive the long and cheap
record. Applications not listed, and fields an application leaves out, get the default.

The archive is swept every hour: an application's hour directory goes once its hour ended longer ago than the
application's archive period. Partitioned by time, deleting never rewrites a file.

In Elasticsearch every application and table has a data stream of its own, `<prefix>-logs-<app>` and
`<prefix>-events-<app>` (not `logs-<prefix>-<app>`: `logs-*-*` belongs to a built-in template), created from an index
template the service puts at start and given its application's period with the data stream lifecycle
(`data_retention`). Elasticsearch then rolls the stream over and deletes whole backing indices once they are older than
the period: no delete-by-query, no ILM policy per application. Data may stay up to a rollover longer than the period,
never shorter. At start the service gives every existing stream the period the file now says, so a changed file applies
to them too. The application's name is made to fit a stream name (lowercase, forbidden characters replaced); two names
that end up the same share a stream.

With many applications, a stream per application means many small shards; a stream per retention class, with the
application as a field, would then be the better layout. The urgent copies of events have one stream,
`<prefix>-urgent`, with a period of their own and the times the node sent them and the service received them: the
events themselves also arrive with the events table.
