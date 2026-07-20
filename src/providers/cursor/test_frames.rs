use prost::Message;

use super::connect::encode_connect_frame;
use super::proto::{AgentServerMessage, InteractionUpdate, TextDelta, ThinkingDelta, TurnEnded};

pub(crate) fn text_frame(text: &str) -> Vec<u8> {
    encode_agent_message(AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            text_delta: Some(TextDelta {
                text: text.to_string(),
            }),
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            token_delta: None,
            turn_ended: None,
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    })
}

pub(crate) fn thinking_frame(text: &str) -> Vec<u8> {
    encode_agent_message(AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            text_delta: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: Some(ThinkingDelta {
                text: text.to_string(),
            }),
            thinking_completed: None,
            token_delta: None,
            turn_ended: None,
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    })
}

pub(crate) fn usage_frame(input: u64, output: u64) -> Vec<u8> {
    usage_frame_full(input, output, 0, 0)
}

pub(crate) fn usage_frame_full(
    input: u64,
    output: u64,
    cache_read: u64,
    cache_write: u64,
) -> Vec<u8> {
    encode_agent_message(AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            text_delta: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            token_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: Some(input),
                output_tokens: Some(output),
                cache_read_tokens: Some(cache_read),
                cache_write_tokens: Some(cache_write),
                reasoning_tokens: None,
            }),
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    })
}

pub(crate) fn end_frame() -> Vec<u8> {
    encode_connect_frame(b"", 2).to_vec()
}

fn encode_agent_message(msg: AgentServerMessage) -> Vec<u8> {
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    encode_connect_frame(&payload, 0).to_vec()
}
