use lo2cin4bt_core::{
    handle_engine_service_request, EngineServiceCommand, EngineServiceError, EngineServiceRequest,
    EngineServiceResponse, EngineServiceStatus, ENGINE_SERVICE_PROTOCOL_VERSION,
};
use std::collections::HashSet;
use std::io::{self, BufRead, Stdout, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    if let Err(exc) = run_server() {
        eprintln!("{exc}");
        std::process::exit(2);
    }
}

fn run_server() -> Result<(), String> {
    let stdin = io::stdin();
    let stdout = Arc::new(Mutex::new(io::stdout()));
    let active = Arc::new(Mutex::new(HashSet::<String>::new()));
    let canceled = Arc::new(Mutex::new(HashSet::<String>::new()));

    for line in stdin.lock().lines() {
        let input_text = line.map_err(|exc| format!("unable to read stdin line: {exc}"))?;
        if input_text.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<EngineServiceRequest>(&input_text) {
            Ok(request) => request,
            Err(exc) => {
                write_response(
                    &stdout,
                    &EngineServiceResponse {
                        protocol_version: ENGINE_SERVICE_PROTOCOL_VERSION.to_string(),
                        request_id: String::new(),
                        status: EngineServiceStatus::Error,
                        operation: None,
                        result: None,
                        error: Some(EngineServiceError {
                            code: "invalid_envelope".to_string(),
                            message: exc.to_string(),
                            retryable: false,
                        }),
                    },
                )?;
                continue;
            }
        };

        match request.command {
            EngineServiceCommand::Execute
            | EngineServiceCommand::ExecuteEngineRequest
            | EngineServiceCommand::ExecuteEngineRequestBatch => {
                write_response(
                    &stdout,
                    &EngineServiceResponse::progress(
                        &request,
                        serde_json::json!({"stage": "accepted"}),
                    ),
                )?;
                let worker_stdout = Arc::clone(&stdout);
                let worker_active = Arc::clone(&active);
                let worker_canceled = Arc::clone(&canceled);
                active
                    .lock()
                    .map_err(|_| "active request registry lock poisoned".to_string())?
                    .insert(request.request_id.clone());
                thread::spawn(move || {
                    let request_id = request.request_id.clone();
                    let response = match catch_unwind(AssertUnwindSafe(|| {
                        handle_engine_service_request(request.clone())
                    })) {
                        Ok(response) => response,
                        Err(_) => EngineServiceResponse::failure(
                            &request,
                            "worker_panic",
                            "Rust engine worker panicked".to_string(),
                        ),
                    };
                    let was_canceled = worker_canceled
                        .lock()
                        .map(|mut rows| rows.remove(&request_id))
                        .unwrap_or(false);
                    if let Ok(mut rows) = worker_active.lock() {
                        rows.remove(&request_id);
                    }
                    let final_response = if was_canceled {
                        EngineServiceResponse::failure(
                            &request,
                            "canceled",
                            "request canceled by control command".to_string(),
                        )
                    } else {
                        response
                    };
                    let _ = write_response(&worker_stdout, &final_response);
                });
            }
            EngineServiceCommand::Cancel => {
                if let Some(target) = request
                    .payload
                    .get("target_request_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                {
                    let is_active = active
                        .lock()
                        .map_err(|_| "active request registry lock poisoned".to_string())?
                        .contains(target);
                    if is_active {
                        canceled
                            .lock()
                            .map_err(|_| "cancel registry lock poisoned".to_string())?
                            .insert(target.to_string());
                    }
                }
                let response = handle_engine_service_request(request);
                write_response(&stdout, &response)?;
            }
            EngineServiceCommand::Shutdown => {
                let response = handle_engine_service_request(request);
                write_response(&stdout, &response)?;
                break;
            }
            _ => {
                let response = handle_engine_service_request(request);
                write_response(&stdout, &response)?;
            }
        }
    }
    Ok(())
}

fn write_response(
    stdout: &Arc<Mutex<Stdout>>,
    response: &EngineServiceResponse,
) -> Result<(), String> {
    let encoded = serde_json::to_string(response)
        .map_err(|exc| format!("unable to serialize engine response: {exc}"))?;
    let mut writer = stdout
        .lock()
        .map_err(|_| "engine stdout lock poisoned".to_string())?;
    writeln!(writer, "{encoded}").map_err(|exc| format!("unable to write stdout: {exc}"))?;
    writer
        .flush()
        .map_err(|exc| format!("unable to flush stdout: {exc}"))
}
