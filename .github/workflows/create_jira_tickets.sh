#!/bin/bash

# Escape double quotes in ISSUE_TITLE and ISSUE_BODY
# ESCAPED_TITLE=$(echo "$ISSUE_TITLE" | sed 's/"/\\"/g')
# ESCAPED_BODY=$(echo "$ISSUE_BODY" | sed 's/"/\\"/g')

# Escape double quotes in ISSUE_TITLE
ESCAPED_TITLE="${ISSUE_TITLE//\"/\\\"}"

# Escape double quotes and newlines in ISSUE_BODY
ESCAPED_BODY="${ISSUE_BODY//\"/\\\"}"
ESCAPED_BODY="${ESCAPED_BODY//$'\n'/\\n}"

# Use readarray to populate TASKS array directly from matching lines in ISSUE_BODY
readarray -t TASKS < <(echo "$ISSUE_BODY" | grep -E '^- \[[ x]\] ')

# Create the JSON payload directly
cat > payload.json <<EOF
{
  "fields": {
    "project": { "key": "CW" },
    "summary": "$ESCAPED_TITLE",
    "description": "$ESCAPED_BODY",
    "issuetype": { "name": "Story" }
  }
}
EOF

# Display the JSON content for verification
echo "Generated JSON payload:"
cat payload.json

# Make the API request to create the main story
RESPONSE=$(curl -s -w "%{http_code}" -o response.json -X POST -u "$JIRA_EMAIL_DAFYDD:$JIRA_API_TOKEN_DAFYDD" \
  -H "Content-Type: application/json" \
  -d @payload.json \
  "https://cworthy.atlassian.net/rest/api/2/issue")

echo "Jira response (Code $RESPONSE):"
cat response.json

# Check if curl was successful
if [[ $? -ne 0 || "$RESPONSE" -ne 201 ]]; then
    echo "Error: Failed to make a request to Jira API."
  exit 1
fi


# Extract the ID of the created story
STORY_ID=$(jq -r '.key' response.json)

# Verify STORY_ID is not empty
if [[ -z "$STORY_ID" ]]; then
  echo "Error: STORY_ID is empty. Failed to retrieve a valid story ID."
  exit 1
fi

# If there are tasks, create subtasks
if [[ ${#TASKS[@]} -gt 0 ]]; then
  echo "Creating subtasks for tasks found in the issue..."

  for TASK in "${TASKS[@]}"; do
    # Remove the `- [ ]` or `- [x]` prefix from each TASK
    CLEANED_TASK=$(echo "$TASK" | sed 's/^- \[[ x]\] //')
    
    # Check for presence of colon and split based on that
    if [[ "$CLEANED_TASK" == *:* ]]; then
      # Split into summary and description based on the first colon
      TASK_SUMMARY="${CLEANED_TASK%%:*}"
      TASK_DESCRIPTION="${CLEANED_TASK#*:}"
    else
      # If no colon, treat the whole line as the summary and leave description empty
      TASK_SUMMARY="$CLEANED_TASK"
      TASK_DESCRIPTION=""
    fi


    # Trim any extra whitespace or newlines in summary and description
    TASK_SUMMARY=$(echo "$TASK_SUMMARY" | tr -d '\n' | tr -d '\r' | xargs)
    TASK_DESCRIPTION=$(echo "$TASK_DESCRIPTION" | tr -d '\n' | tr -d '\r' | xargs)
    
    # Escape double quotes in the summary and description
    ESCAPED_TASK_SUMMARY="${TASK_SUMMARY//\"/\\\"}"
    ESCAPED_TASK_DESCRIPTION="${TASK_DESCRIPTION//\"/\\\"}"
    ESCAPED_TASK_SUMMARY="${ESCAPED_TASK_SUMMARY//$'\n'/ }"
    ESCAPED_TASK_DESCRIPTION="${ESCAPED_TASK_DESCRIPTION//$'\n'/ }"
    
    # Create JSON payload for each subtask with summary and description
    cat > subtask.json <<EOF
{
  "fields": {
    "project": { "key": "CW" },
    "summary": "$ESCAPED_TASK_SUMMARY",
    "description": "$ESCAPED_TASK_DESCRIPTION",
    "issuetype": { "id": "10009" },
    "parent": { "key": "$STORY_ID" }
  }
}
EOF
    echo "Generated JSON payload for subtask:"
    cat subtask.json

    # Make the API request to create each subtask
    SUBTASK_RESPONSE=$(curl -s -w "%{http_code}" -o subtask_response.json -X POST -u "$JIRA_EMAIL_DAFYDD:$JIRA_API_TOKEN_DAFYDD" \
      -H "Content-Type: application/json" \
      -d @subtask.json \
      "https://cworthy.atlassian.net/rest/api/2/issue")

    # Check if subtask creation was successful
    if [[ $? -ne 0 || "$SUBTASK_RESPONSE" -ne 201 ]]; then
      echo "Error: Failed to create subtask for '$TASK_SUMMARY'. Response:"
      cat subtask_response.json
      exit 1
    else
      echo "Subtask created successfully for '$TASK_SUMMARY'."
      cat subtask_response.json
    fi
  done
else
  echo "No tasks found in the issue body. No subtasks created."
fi
