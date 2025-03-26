# Databricks notebook source
library(httr)
library(jsonlite)

workspace_url <- dbutils.secrets.get(scope='gwsc-secrets', key='workspace_url')
token <- dbutils.secrets.get(scope='gwsc-secrets', key='workspace_token')

job_id <- 398370535700101  # Example Job ID

# 2) Kick off the job run
body <- list(
  job_id = job_id,
  notebook_params = list(input='Hello from R')
)

run_now_url <- paste0(workspace_url, "/api/2.1/jobs/run-now")
resp <- POST(
  run_now_url,
  add_headers(Authorization = paste("Bearer", token)),
  body = toJSON(body, auto_unbox = TRUE),
  encode = "json"
)

if (status_code(resp) != 200) {
  stop("Failed to start job: ", content(resp, "text"))
}

run_info <- content(resp, "parsed")
parent_run_id <- run_info$run_id
cat("Databricks job triggered. parent_run_id =", parent_run_id, "\n")

get_run_url <- paste0(workspace_url, "/api/2.1/jobs/runs/get?run_id=", parent_run_id)

finished <- FALSE
while (!finished) {
  Sys.sleep(1)
  status_resp <- GET(
    get_run_url,
    add_headers(Authorization = paste("Bearer", token))
  )
  stop_for_status(status_resp, "polling run status")
  run_status <- content(status_resp, "parsed")
  
  state <- run_status$state
  life_cycle <- state$life_cycle_state
  cat("Parent run state:", life_cycle, "\n")
  
  if (life_cycle %in% c("TERMINATED","SKIPPED","INTERNAL_ERROR")) {
    finished <- TRUE
    
    if (!is.null(state$result_state) && state$result_state == "SUCCESS") {
      cat("Multi-task job succeeded!\n")
    } else {
      cat("Multi-task job ended, but not in SUCCESS state.\n")
      if (!is.null(run_status$error)) {
        cat("Error:\n", run_status$error, "\n")
      }
    }
  }
}

tasks_list <- run_status$tasks
if (length(tasks_list) == 0) {
  cat("No tasks found in this job run.\n")
} else {
  for (task_info in tasks_list) {
    task_run_id <- task_info$run_id
    task_key <- task_info$task_key 
    
    cat("\nRetrieving output for task:", task_key, "with run_id =", task_run_id, "\n")
    
    get_output_url <- paste0(workspace_url, "/api/2.1/jobs/runs/get-output?run_id=", task_run_id)
    output_resp <- GET(
      get_output_url,
      add_headers(Authorization = paste("Bearer", token))
    )
    
    if (status_code(output_resp) == 200) {
      output_parsed <- content(output_resp, "parsed")
      if (!is.null(output_parsed$notebook_output$result)) {
        cat("Task output:\n", output_parsed$notebook_output$result, "\n")
      } else {
        cat("No notebook result for task.\n")
      }
    } else {
      cat("Failed to retrieve output for task:", content(output_resp, "text"), "\n")
    }
  }
}
