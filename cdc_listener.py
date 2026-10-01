"""Near real time: listen for Opportunity Change Data Capture events and apply them with the SAME MERGE as sf_pipeline.py.

Salesforce pushes an OpportunityChangeEvent over the Pub/Sub API (gRPC + Avro) seconds after a record changes.
Change events only carry the fields that changed, and under heavy load Salesforce can send GAP events with no field
data at all. So this listener does not trust the payload: it re-reads the changed records by Id and MERGEs the full row.
Keep the scheduled watermark run (sf_pipeline.py) as the nightly safety net: events are kept for only 72 hours.

Setup: enable CDC for Opportunity (Setup > Change Data Capture), then
    pip install grpcio fastavro
    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. pubsub_api.proto
(pubsub_api.proto: https://github.com/forcedotcom/pub-sub-api)
"""
import io
import json
import queue

import fastavro
import grpc
import pubsub_api_pb2 as pb
import pubsub_api_pb2_grpc as pbg

from sf_pipeline import FIELDS, OBJECT, connect_salesforce, connect_sql, load, to_frame

TOPIC = f"/data/{OBJECT}ChangeEvent"


def main():
    sf, db = connect_salesforce(), connect_sql()
    auth = (("accesstoken", sf.session_id), ("instanceurl", f"https://{sf.sf_instance}"), ("tenantid", sf.org_id))
    stub = pbg.PubSubStub(grpc.secure_channel("api.pubsub.salesforce.com:7443", grpc.ssl_channel_credentials()))
    schemas = {}

    def decode(event):
        sid = event.event.schema_id   # the schema changes when an admin adds or removes a field
        if sid not in schemas:
            schema_json = stub.GetSchema(pb.SchemaRequest(schema_id=sid), metadata=auth).schema_json
            schemas[sid] = fastavro.parse_schema(json.loads(schema_json))
        return fastavro.schemaless_reader(io.BytesIO(event.event.payload), schemas[sid])

    more = queue.Queue()

    def requests():
        yield pb.FetchRequest(topic_name=TOPIC, replay_preset=pb.ReplayPreset.LATEST, num_requested=100)
        while True:   # flow control: only ask for more once the previous batch is used up
            more.get()
            yield pb.FetchRequest(topic_name=TOPIC, num_requested=100)

    print(f"Listening for {TOPIC} ...", flush=True)
    for response in stub.Subscribe(requests(), metadata=auth):
        for event in response.events:
            header = decode(event)["ChangeEventHeader"]
            ids = list(header["recordIds"])
            print(f"\n{header['changeType']} {header['entityName']} {', '.join(ids)}", flush=True)
            # re-read the full records (handles sparse and GAP events), then the same MERGE as the batch pipeline
            id_list = ", ".join(f"'{i}'" for i in ids)
            records = sf.query_all(f"SELECT {', '.join(FIELDS)} FROM {OBJECT} WHERE Id IN ({id_list})",
                                   include_deleted=True)["records"]
            inserted, updated = load(db, to_frame(records))
            db.commit()
            print(f"MERGE: {inserted} inserted, {updated} updated", flush=True)
        if response.pending_num_requested == 0:
            more.put(True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped listening.")
