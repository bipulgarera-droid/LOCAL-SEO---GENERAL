# Gemini Client Directive

**Purpose:** Provide a unified interface for interacting with Google's Gemini API.

## Location
`lib/gemini_client.py`

## Usage
```python
from lib import gemini_client

response = gemini_client.generate_content(
    prompt="Hello world",
    model="gemini-1.5-pro", # or gemini-2.0-flash-exp
    temperature=0.7
)
```

## Key Features
- **Model Fallback:** Automatically handles model deprecation or unavailability.
- **Error Handling:** Catches API errors and returns structured responses.
- **JSON Mode:** Supports `response_mime_type="application/json"` for structured output.

## Maintenance
- **Adding Models:** Update the `DEFAULT_MODEL` or supported model lists in `lib/gemini_client.py`.
- **API Updates:** If Google changes the API format, update the `generate_content` function wrapper.
