#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_protocol::messages::{BatchPusherApiMessage, PusherApiMessage};
use sockudo_protocol::versioned_messages::{
    AppendMessageRequest, DeleteMessageRequest, MessageVersionsQuery, MutationResponse,
    UpdateMessageRequest,
};

const MAX_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    if let Ok(request) = serde_json::from_slice::<UpdateMessageRequest>(data) {
        let validation = request.validate().is_ok();
        if let Some(extras) = request.extras.as_ref() {
            let _ = extras.validate_ai_headers();
        }
        let encoded = serde_json::to_vec(&request).expect("update request must serialize");
        let decoded = serde_json::from_slice::<UpdateMessageRequest>(&encoded)
            .expect("serialized update request must deserialize");
        assert_eq!(decoded, request);
        assert_eq!(decoded.validate().is_ok(), validation);
    }

    if let Ok(request) = serde_json::from_slice::<DeleteMessageRequest>(data) {
        let validation = request.validate().is_ok();
        if let Some(extras) = request.extras.as_ref() {
            let _ = extras.validate_ai_headers();
        }
        let encoded = serde_json::to_vec(&request).expect("delete request must serialize");
        let decoded = serde_json::from_slice::<DeleteMessageRequest>(&encoded)
            .expect("serialized delete request must deserialize");
        assert_eq!(decoded, request);
        assert_eq!(decoded.validate().is_ok(), validation);
    }

    if let Ok(request) = serde_json::from_slice::<AppendMessageRequest>(data) {
        let validation = request.validate().is_ok();
        if let Some(extras) = request.extras.as_ref() {
            let _ = extras.validate_ai_headers();
        }
        let encoded = serde_json::to_vec(&request).expect("append request must serialize");
        let decoded = serde_json::from_slice::<AppendMessageRequest>(&encoded)
            .expect("serialized append request must deserialize");
        assert_eq!(decoded, request);
        assert_eq!(decoded.validate().is_ok(), validation);
    }

    if let Ok(batch) = serde_json::from_slice::<BatchPusherApiMessage>(data) {
        let original = serde_json::to_value(&batch).expect("batch request must serialize");
        let encoded = serde_json::to_vec(&batch).expect("batch request must serialize");
        let decoded = serde_json::from_slice::<BatchPusherApiMessage>(&encoded)
            .expect("serialized batch request must deserialize");
        let normalized = serde_json::to_value(&decoded).expect("batch request must serialize");
        assert_eq!(normalized, original);
        for message in batch.batch {
            if let Some(extras) = message.extras.as_ref() {
                let _ = extras.validate_ai_headers();
            }
        }
    }

    if let Ok(message) = serde_json::from_slice::<PusherApiMessage>(data) {
        let original = serde_json::to_value(&message).expect("publish request must serialize");
        let encoded = serde_json::to_vec(&message).expect("publish request must serialize");
        let decoded = serde_json::from_slice::<PusherApiMessage>(&encoded)
            .expect("serialized publish request must deserialize");
        assert_eq!(
            serde_json::to_value(decoded).expect("publish request must serialize"),
            original
        );
    }

    if let Ok(query) = serde_json::from_slice::<MessageVersionsQuery>(data) {
        let validation = query.validate().is_ok();
        let encoded = serde_json::to_vec(&query).expect("versions query must serialize");
        let decoded = serde_json::from_slice::<MessageVersionsQuery>(&encoded)
            .expect("serialized versions query must deserialize");
        assert_eq!(decoded, query);
        assert_eq!(decoded.validate().is_ok(), validation);
    }

    if let Ok(response) = serde_json::from_slice::<MutationResponse>(data) {
        let encoded = serde_json::to_vec(&response).expect("mutation response must serialize");
        let decoded = serde_json::from_slice::<MutationResponse>(&encoded)
            .expect("serialized mutation response must deserialize");
        assert_eq!(decoded, response);
    }
});
