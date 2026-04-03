"""Unit tests for discord_messenger.py — all HTTP calls are mocked."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from discord_messenger import delete_messages, load_webhooks, send_message_to_discord

_WEBHOOKS = [
    {"url": "https://discord.com/api/webhooks/111", "thread_id": None},
    {"url": "https://discord.com/api/webhooks/222", "thread_id": "tid99"},
]


def _ok_response(msg_id: str = "msg42") -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.json.return_value = {'id': msg_id}
    return r


def _err_response(status: int = 400) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = "Bad Request"
    return r


# ---------------------------------------------------------------------------
# load_webhooks
# ---------------------------------------------------------------------------
class TestLoadWebhooks(unittest.TestCase):
    @patch('discord_messenger.load_yaml_config', return_value={'webhooks': _WEBHOOKS})
    def test_returns_webhooks_list(self, _):
        self.assertEqual(load_webhooks(), _WEBHOOKS)

    @patch('discord_messenger.load_yaml_config', return_value={})
    def test_raises_value_error_when_key_missing(self, _):
        with self.assertRaises(ValueError):
            load_webhooks()

    @patch('discord_messenger.load_yaml_config', return_value={'webhooks': None})
    def test_raises_value_error_when_webhooks_is_none(self, _):
        with self.assertRaises(ValueError):
            load_webhooks()


# ---------------------------------------------------------------------------
# send_message_to_discord
# ---------------------------------------------------------------------------
class TestSendMessageToDiscord(unittest.TestCase):
    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.post')
    def test_posts_to_every_webhook(self, mock_post, _):
        mock_post.return_value = _ok_response()
        ids = send_message_to_discord("hello", noimage=True, win='restore', debug=False)
        self.assertEqual(mock_post.call_count, 2)
        self.assertEqual(ids, ['msg42', 'msg42'])

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.post')
    def test_thread_id_appended_to_url(self, mock_post, _):
        mock_post.return_value = _ok_response()
        send_message_to_discord("x", noimage=True, win='restore', debug=False)
        # Second call (webhook 222) should include thread_id in the URL
        second_url = mock_post.call_args_list[1][0][0]
        self.assertIn('thread_id=tid99', second_url)

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.post')
    def test_no_thread_id_means_plain_url(self, mock_post, _):
        mock_post.return_value = _ok_response()
        send_message_to_discord("x", noimage=True, win='restore', debug=False)
        first_url = mock_post.call_args_list[0][0][0]
        self.assertNotIn('thread_id', first_url)

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.post')
    def test_failed_status_returns_none_for_that_webhook(self, mock_post, _):
        mock_post.return_value = _err_response(400)
        ids = send_message_to_discord("x", noimage=True, win='restore', debug=False)
        self.assertEqual(ids, [None, None])

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.post')
    def test_mixed_success_and_failure(self, mock_post, _):
        mock_post.side_effect = [_ok_response('id1'), _err_response(500)]
        ids = send_message_to_discord("x", noimage=True, win='restore', debug=False)
        self.assertEqual(ids, ['id1', None])

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.post', side_effect=ConnectionError("timeout"))
    def test_network_exception_returns_none(self, _, __):
        ids = send_message_to_discord("x", noimage=True, win='restore', debug=False)
        self.assertEqual(ids, [None, None])

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.post')
    def test_timeout_passed_to_requests(self, mock_post, _):
        mock_post.return_value = _ok_response()
        send_message_to_discord("x", noimage=True, win='restore', debug=False)
        _, kwargs = mock_post.call_args
        self.assertIn('timeout', kwargs)
        self.assertGreater(kwargs['timeout'], 0)

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS[:1])
    @patch('discord_messenger.requests.post')
    def test_204_response_treated_as_success(self, mock_post, _):
        r = MagicMock()
        r.status_code = 204
        r.json.return_value = {'id': 'abc'}
        mock_post.return_value = r
        ids = send_message_to_discord("x", noimage=True, win='restore', debug=False)
        # 204 is success; id may be None if no body, but should not be rejected
        self.assertEqual(len(ids), 1)

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS[:1])
    @patch('discord_messenger.requests.post')
    def test_json_parse_failure_returns_none_id(self, mock_post, _):
        r = MagicMock()
        r.status_code = 200
        r.json.side_effect = ValueError("no JSON")
        mock_post.return_value = r
        ids = send_message_to_discord("x", noimage=True, win='restore', debug=False)
        self.assertEqual(ids, [None])


# ---------------------------------------------------------------------------
# delete_messages
# ---------------------------------------------------------------------------
class TestDeleteMessages(unittest.TestCase):
    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.delete')
    def test_deletes_each_message(self, mock_del, _):
        r = MagicMock()
        r.status_code = 204
        mock_del.return_value = r
        delete_messages(['m1', 'm2'])
        self.assertEqual(mock_del.call_count, 2)

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.delete')
    def test_skips_none_ids(self, mock_del, _):
        r = MagicMock()
        r.status_code = 204
        mock_del.return_value = r
        delete_messages([None, 'm2'])
        self.assertEqual(mock_del.call_count, 1)

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.delete')
    def test_uses_correct_webhook_per_message(self, mock_del, _):
        r = MagicMock()
        r.status_code = 204
        mock_del.return_value = r
        delete_messages(['m1', 'm2'])
        first_url = mock_del.call_args_list[0][0][0]
        second_url = mock_del.call_args_list[1][0][0]
        self.assertIn('111', first_url)
        self.assertIn('222', second_url)

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.delete', side_effect=ConnectionError("down"))
    def test_network_exception_does_not_raise(self, _, __):
        # Should swallow the error gracefully
        delete_messages(['m1', 'm2'])

    @patch('discord_messenger.load_webhooks', return_value=_WEBHOOKS)
    @patch('discord_messenger.requests.delete')
    def test_timeout_passed_to_requests(self, mock_del, _):
        r = MagicMock()
        r.status_code = 204
        mock_del.return_value = r
        delete_messages(['m1'])
        _, kwargs = mock_del.call_args
        self.assertIn('timeout', kwargs)


if __name__ == "__main__":
    unittest.main()
