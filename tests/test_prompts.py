import json
from src.recommendation_api.prompts import build_prompt, parser, BookRec, BookRecList


def test_prompt_contains_format_instructions():
    """The rendered system prompt should include the JSON schema instructions."""
    user = "Recommend 1 book about space"
    msgs = build_prompt(user)
    # The first message is system, ensure format_instructions placeholder was filled
    system_msg = msgs[0]
    assert "book_id" in system_msg.content
    assert "librarian_blurb" in system_msg.content


def test_parser_happy_path():
    """Parser should return a BookRecList for valid JSON."""
    sample_json = json.dumps(
        {
            "recommendations": [
                {
                    "book_id": "42",
                    "title": "Hitchhiker's Guide",
                    "author": "Douglas Adams",
                    "reading_level": 8.5,
                    "librarian_blurb": "A whimsical journey across the galaxy.",
                    "justification": "Matches user's interest in science fiction and humor."
                }
            ]
        }
    )
    parsed = parser.parse(sample_json)
    assert isinstance(parsed, BookRecList)
    assert parsed.recommendations[0].book_id == "42"


def test_parser_invalid_json_fails():
    """Parser should raise on malformed payload so fallback path triggers."""
    bad_json = "{not valid json}"
    try:
        parser.parse(bad_json)
    except Exception:
        return  # expected
    assert False, "Parser should have raised on invalid JSON" 