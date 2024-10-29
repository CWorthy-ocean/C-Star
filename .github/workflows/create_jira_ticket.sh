#!/bin/bash

# Escape double quotes in ISSUE_TITLE and ISSUE_BODY
ESCAPED_TITLE=$(echo "$ISSUE_TITLE" | sed 's/"/\\"/g')
ESCAPED_BODY=$(echo "$ISSUE_BODY" | sed 's/"/\\"/g')

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

# Make the API request using the JSON file
curl -X POST -u "$JIRA_EMAIL_DAFYDD:$JIRA_API_TOKEN_DAFYDD" \
  -H "Content-Type: application/json" \
  -d @payload.json \
  "https://cworthy.atlassian.net/rest/api/2/issue"
