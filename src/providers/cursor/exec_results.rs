//! Encoding of Claude Code tool results back onto Cursor's native exec stream.

use bytes::Bytes;
use prost::Message;

use super::connect::encode_connect_frame;
use super::proto::*;
use super::tool_bridge::{render_tool_result_content, tool_result_is_error};

#[derive(Debug, Clone, PartialEq)]
pub enum CursorExecKind {
    Read {
        path: String,
        range_applied: bool,
    },
    Write {
        path: String,
        content: String,
    },
    Delete {
        path: String,
    },
    Grep {
        pattern: String,
        path: String,
    },
    Ls {
        path: String,
    },
    Shell {
        command: String,
        working_directory: String,
        streaming: bool,
    },
}

#[derive(Debug, Clone)]
pub struct PendingCursorExec {
    pub id: u32,
    pub exec_id: Option<String>,
    pub tool_use_id: String,
    pub claude_name: String,
    pub claude_input: serde_json::Value,
    pub kind: CursorExecKind,
}

impl PendingCursorExec {
    pub fn from_server(exec: &ExecServerMessage) -> Option<Self> {
        let mapped = super::native_tools::map_exec_server_message(exec)?;
        let kind = if let Some(args) = exec.read_args.as_ref() {
            CursorExecKind::Read {
                path: args.path.clone(),
                range_applied: args.offset.is_some() || args.limit.is_some(),
            }
        } else if let Some(args) = exec.write_args.as_ref() {
            CursorExecKind::Write {
                path: args.path.clone(),
                content: args.file_text.clone(),
            }
        } else if let Some(args) = exec.delete_args.as_ref() {
            CursorExecKind::Delete {
                path: args.path.clone(),
            }
        } else if let Some(args) = exec.grep_args.as_ref() {
            CursorExecKind::Grep {
                pattern: args.pattern.clone(),
                path: args.path.clone().unwrap_or_default(),
            }
        } else if let Some(args) = exec.ls_args.as_ref() {
            CursorExecKind::Ls {
                path: args.path.clone(),
            }
        } else if let Some(args) = exec.shell_stream_args.as_ref() {
            CursorExecKind::Shell {
                command: args.command.clone(),
                working_directory: args.working_directory.clone(),
                streaming: true,
            }
        } else {
            let args = exec.shell_args.as_ref()?;
            CursorExecKind::Shell {
                command: args.command.clone(),
                working_directory: args.working_directory.clone(),
                streaming: false,
            }
        };

        Some(Self {
            id: exec.id,
            exec_id: exec.exec_id.clone(),
            tool_use_id: mapped.tool_use_id,
            claude_name: mapped.name,
            claude_input: mapped.input,
            kind,
        })
    }
}

pub fn encode_tool_result_frames(
    pending: &PendingCursorExec,
    tool_result: &serde_json::Value,
) -> Result<Vec<Bytes>, prost::EncodeError> {
    let content = render_tool_result_content(tool_result);
    let is_error = tool_result_is_error(tool_result);
    let mut frames = match &pending.kind {
        CursorExecKind::Read {
            path,
            range_applied,
        } => vec![encode_exec_message(
            pending,
            ExecPayload::Read(if is_error {
                ReadResult {
                    success: None,
                    error: Some(ReadError {
                        path: path.clone(),
                        error: content,
                    }),
                }
            } else {
                ReadResult {
                    success: Some(ReadSuccess {
                        path: path.clone(),
                        total_lines: saturating_i32(content.lines().count()),
                        file_size: content.len() as i64,
                        content: Some(content),
                        truncated: false,
                        range_applied: *range_applied,
                    }),
                    error: None,
                }
            }),
        )?],
        CursorExecKind::Write {
            path,
            content: file,
        } => vec![encode_exec_message(
            pending,
            ExecPayload::Write(if is_error {
                WriteResult {
                    success: None,
                    error: Some(WriteError {
                        path: path.clone(),
                        error: content,
                    }),
                }
            } else {
                WriteResult {
                    success: Some(WriteSuccess {
                        path: path.clone(),
                        lines_created: saturating_i32(file.lines().count()),
                        file_size: saturating_i32(file.len()),
                        file_content_after_write: Some(file.clone()),
                    }),
                    error: None,
                }
            }),
        )?],
        CursorExecKind::Delete { path } => vec![encode_exec_message(
            pending,
            ExecPayload::Delete(if is_error {
                DeleteResult {
                    success: None,
                    error: Some(DeleteError {
                        path: path.clone(),
                        error: content,
                    }),
                }
            } else {
                DeleteResult {
                    success: Some(DeleteSuccess {
                        path: path.clone(),
                        deleted_file: path.clone(),
                        file_size: 0,
                        prev_content: String::new(),
                    }),
                    error: None,
                }
            }),
        )?],
        CursorExecKind::Grep { pattern, path } => vec![encode_exec_message(
            pending,
            ExecPayload::Grep(if is_error {
                GrepResult {
                    success: None,
                    error: Some(GrepError { error: content }),
                }
            } else {
                let line_count = content.lines().count();
                GrepResult {
                    success: Some(GrepSuccess {
                        pattern: pattern.clone(),
                        path: path.clone(),
                        output_mode: "content".into(),
                        workspace_results: Default::default(),
                        active_editor_result: Some(GrepUnionResult {
                            content: Some(GrepContentResult {
                                matches: vec![GrepFileMatch {
                                    file: path.clone(),
                                    matches: content
                                        .lines()
                                        .enumerate()
                                        .map(|(index, line)| GrepContentMatch {
                                            line_number: saturating_i32(index + 1),
                                            content: line.to_string(),
                                            content_truncated: false,
                                            is_context_line: false,
                                        })
                                        .collect(),
                                }],
                                total_lines: saturating_i32(line_count),
                                total_matched_lines: saturating_i32(line_count),
                                client_truncated: false,
                                ripgrep_truncated: false,
                            }),
                        }),
                    }),
                    error: None,
                }
            }),
        )?],
        CursorExecKind::Ls { path } => vec![encode_exec_message(
            pending,
            ExecPayload::Ls(if is_error {
                LsResult {
                    success: None,
                    error: Some(LsError {
                        path: path.clone(),
                        error: content,
                    }),
                }
            } else {
                let children_files: Vec<LsFile> = content
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(|line| LsFile {
                        name: line.to_string(),
                    })
                    .collect();
                LsResult {
                    success: Some(LsSuccess {
                        directory_tree_root: Some(LsDirectoryTreeNode {
                            abs_path: path.clone(),
                            children_dirs: vec![],
                            num_files: saturating_i32(children_files.len()),
                            children_files,
                            children_were_processed: true,
                            full_subtree_extension_counts: Default::default(),
                        }),
                    }),
                    error: None,
                }
            }),
        )?],
        CursorExecKind::Shell {
            command,
            working_directory,
            streaming,
        } => {
            if *streaming {
                let mut out = Vec::new();
                out.push(encode_exec_message(
                    pending,
                    ExecPayload::ShellStream(ShellStream {
                        start: Some(ShellStreamStart {}),
                        stdout: None,
                        stderr: None,
                        exit: None,
                    }),
                )?);
                if !content.is_empty() {
                    out.push(encode_exec_message(
                        pending,
                        ExecPayload::ShellStream(ShellStream {
                            stdout: (!is_error).then(|| ShellStreamStdout {
                                data: content.clone(),
                            }),
                            stderr: is_error.then(|| ShellStreamStderr {
                                data: content.clone(),
                            }),
                            exit: None,
                            start: None,
                        }),
                    )?);
                }
                out.push(encode_exec_message(
                    pending,
                    ExecPayload::ShellStream(ShellStream {
                        stdout: None,
                        stderr: None,
                        start: None,
                        exit: Some(ShellStreamExit {
                            code: u32::from(is_error),
                            cwd: working_directory.clone(),
                            aborted: false,
                            local_execution_time_ms: Some(0),
                        }),
                    }),
                )?);
                out
            } else {
                vec![encode_exec_message(
                    pending,
                    ExecPayload::Shell(if is_error {
                        ShellResult {
                            success: None,
                            failure: Some(ShellFailure {
                                command: command.clone(),
                                working_directory: working_directory.clone(),
                                exit_code: 1,
                                signal: String::new(),
                                stdout: String::new(),
                                stderr: content,
                                execution_time: 0,
                                aborted: false,
                                local_execution_time_ms: Some(0),
                            }),
                        }
                    } else {
                        ShellResult {
                            success: Some(ShellSuccess {
                                command: command.clone(),
                                working_directory: working_directory.clone(),
                                exit_code: 0,
                                signal: String::new(),
                                stdout: content,
                                stderr: String::new(),
                                execution_time: 0,
                                local_execution_time_ms: Some(0),
                            }),
                            failure: None,
                        }
                    }),
                )?]
            }
        }
    };

    frames.push(encode_control_close(pending.id)?);
    Ok(frames)
}

pub fn encode_exec_heartbeat(id: u32) -> Result<Bytes, prost::EncodeError> {
    encode_agent_message(AgentClientMessage {
        run_request: None,
        exec_client_message: None,
        kv_client_message: None,
        interaction_response: None,
        exec_client_control_message: Some(ExecClientControlMessage {
            stream_close: None,
            throw: None,
            heartbeat: Some(ExecClientHeartbeat { id }),
        }),
        client_heartbeat: None,
    })
}

pub fn encode_control_throw(id: u32, error: String) -> Result<Vec<Bytes>, prost::EncodeError> {
    Ok(vec![
        encode_agent_message(AgentClientMessage {
            run_request: None,
            exec_client_message: None,
            kv_client_message: None,
            exec_client_control_message: Some(ExecClientControlMessage {
                stream_close: None,
                throw: Some(ExecClientThrow {
                    id,
                    error,
                    stack_trace: None,
                    error_code: None,
                }),
                heartbeat: None,
            }),
            interaction_response: None,
            client_heartbeat: None,
        })?,
        encode_control_close(id)?,
    ])
}

enum ExecPayload {
    Read(ReadResult),
    Write(WriteResult),
    Delete(DeleteResult),
    Grep(GrepResult),
    Ls(LsResult),
    Shell(ShellResult),
    ShellStream(ShellStream),
}

fn encode_exec_message(
    pending: &PendingCursorExec,
    payload: ExecPayload,
) -> Result<Bytes, prost::EncodeError> {
    let mut exec = ExecClientMessage {
        id: pending.id,
        exec_id: pending.exec_id.clone(),
        local_execution_time_ms: Some(0),
        shell_result: None,
        write_result: None,
        delete_result: None,
        grep_result: None,
        read_result: None,
        ls_result: None,
        request_context_result: None,
        shell_stream: None,
    };
    match payload {
        ExecPayload::Read(value) => exec.read_result = Some(value),
        ExecPayload::Write(value) => exec.write_result = Some(value),
        ExecPayload::Delete(value) => exec.delete_result = Some(value),
        ExecPayload::Grep(value) => exec.grep_result = Some(value),
        ExecPayload::Ls(value) => exec.ls_result = Some(value),
        ExecPayload::Shell(value) => exec.shell_result = Some(value),
        ExecPayload::ShellStream(value) => exec.shell_stream = Some(value),
    }
    encode_agent_message(AgentClientMessage {
        run_request: None,
        exec_client_message: Some(exec),
        kv_client_message: None,
        exec_client_control_message: None,
        interaction_response: None,
        client_heartbeat: None,
    })
}

pub(crate) fn encode_control_close(id: u32) -> Result<Bytes, prost::EncodeError> {
    encode_agent_message(AgentClientMessage {
        run_request: None,
        exec_client_message: None,
        kv_client_message: None,
        interaction_response: None,
        exec_client_control_message: Some(ExecClientControlMessage {
            stream_close: Some(ExecClientStreamClose { id }),
            throw: None,
            heartbeat: None,
        }),
        client_heartbeat: None,
    })
}

fn encode_agent_message(message: AgentClientMessage) -> Result<Bytes, prost::EncodeError> {
    let mut payload = Vec::new();
    message.encode(&mut payload)?;
    Ok(encode_connect_frame(payload, 0))
}

fn saturating_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::client::decode_upstream_frames;

    fn pending_read() -> PendingCursorExec {
        PendingCursorExec {
            id: 7,
            exec_id: Some("exec-7".into()),
            tool_use_id: "tool-7".into(),
            claude_name: "Read".into(),
            claude_input: serde_json::json!({"file_path":"README.md"}),
            kind: CursorExecKind::Read {
                path: "README.md".into(),
                range_applied: false,
            },
        }
    }

    #[test]
    fn read_success_uses_exec_id_and_real_result_tag() {
        let frames = encode_tool_result_frames(
            &pending_read(),
            &serde_json::json!({"type":"tool_result","content":"one\ntwo"}),
        )
        .unwrap();
        assert_eq!(frames.len(), 2);
        let decoded = decode_upstream_frames(&frames[0]).unwrap();
        let msg = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        let exec = msg.exec_client_message.unwrap();
        assert_eq!(exec.id, 7);
        assert_eq!(exec.exec_id.as_deref(), Some("exec-7"));
        let success = exec.read_result.unwrap().success.unwrap();
        assert_eq!(success.content.as_deref(), Some("one\ntwo"));
        assert_eq!(success.total_lines, 2);
    }

    #[test]
    fn read_error_is_not_encoded_as_success() {
        let frames = encode_tool_result_frames(
            &pending_read(),
            &serde_json::json!({"type":"tool_result","content":"missing","is_error":true}),
        )
        .unwrap();
        let decoded = decode_upstream_frames(&frames[0]).unwrap();
        let msg = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        let result = msg.exec_client_message.unwrap().read_result.unwrap();
        assert!(result.success.is_none());
        assert_eq!(result.error.unwrap().error, "missing");
    }
}
