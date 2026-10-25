use serde_json::json;

use super::IngestStreamRecordRequest;

#[test]
fn given_contract_and_standard_records_when_converted_then_key_precedence_and_images_are_preserved()
{
    for standard in [false, true] {
        for explicit in [None, Some("explicit"), Some("")] {
            for event in [None, Some("event"), Some("")] {
                let storage = json!({"Keys": {"pk": {"S": "key"}}, "SequenceNumber": "sequence", "OldImage": {"value": {"S": "old"}}, "NewImage": {"value": {"S": "new"}}});
                let record = if standard {
                    json!({"eventID": event, "dynamodb": storage})
                } else {
                    storage.clone()
                };
                let request: IngestStreamRecordRequest =
                    serde_json::from_value(json!({"record_key": explicit, "record": record}))
                        .unwrap();
                let (key, converted) = request.into_contract_record();
                let expected = explicit
                    .or(if standard { event } else { None })
                    .unwrap_or("sequence");
                assert_eq!(
                    key, expected,
                    "standard={standard} explicit={explicit:?} event={event:?}"
                );
                assert_eq!(serde_json::to_value(converted).unwrap(), storage);
            }
        }
    }
}
