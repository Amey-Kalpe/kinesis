import json
import base64

print("Loading function...")


def lambda_handler(event, context):
    # TODO implement
    success = 0
    failure = 0

    output = []

    for record in event["records"]:
        payload = base64.b64decode(record["data"]).decode("utf-8")
        print("Decoded Payload: ", payload)
        record_id = record["recordId"]
        print(record_id)
        match = payload.split("|")

        if match:
            result = {
                "object_id": int(match[0]),
                "record_id": int(match[1]),
                "offense_code": int(match[2]),
                "offense_ext": int(match[3]) if match[3].isnumeric() else match[3],
                "offense_category": match[4],
                "description": match[5],
                "police_district": int(match[6]) if match[6].isnumeric() else match[6],
                "beat": match[7],
                "grid": match[8],
                "occurence_date": match[9],
            }
            success += 1
            output.append(
                {"recordId": record_id, "result": "Ok", "data": record["data"]}
            )
        else:
            failure += 1
            output.append(
                {
                    "recordId": record_id,
                    "result": "ProcessingFailed",
                    "data": base64.b64encode(
                        json.dumps(record["data"]).encode("utf-8")
                    ),
                }
            )

    print(
        f"Processing completed.  Successful records {success}, Failed records {failure}."
    )
    return {"records": output}
