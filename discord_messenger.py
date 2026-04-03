import logging
import os
from typing import List, Optional

import requests

from utils import load_yaml_config, take_screenshot_of_app

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 10  # seconds for all Discord HTTP calls


def load_webhooks() -> List[dict]:
    """Load and validate the webhooks list from config.yaml."""
    config = load_yaml_config()
    webhooks = config.get('webhooks')
    if not webhooks:
        raise ValueError("No 'webhooks' key found in config.yaml.")
    return webhooks


def send_message_to_discord(
    message: str, noimage: bool, win: str, debug: bool
) -> List[Optional[str]]:
    """
    Post *message* to all configured Discord webhooks.
    Optionally attaches a screenshot of the TAT application window.

    Returns a list of message IDs (one per webhook; None on failure).
    """
    screenshot_path: Optional[str] = None
    if not noimage:
        screenshot_path = take_screenshot_of_app("Trade Automation Toolbox", win)

    message_ids: List[Optional[str]] = []
    webhooks = load_webhooks()

    for webhook in webhooks:
        url = webhook["url"]
        thread_id = webhook.get("thread_id")
        if thread_id:
            url += f"?thread_id={thread_id}"

        payload = {"content": message}

        try:
            if screenshot_path and os.path.isfile(screenshot_path):
                with open(screenshot_path, "rb") as image_file:
                    files = {"file": (os.path.basename(screenshot_path), image_file)}
                    response = requests.post(
                        url, data=payload, files=files, timeout=REQUEST_TIMEOUT
                    )
            else:
                response = requests.post(url, data=payload, timeout=REQUEST_TIMEOUT)

            if response.status_code not in (200, 204):
                logger.warning(
                    "Failed to send to webhook %s. Status %d: %s",
                    url, response.status_code, response.text,
                )
                message_ids.append(None)
            else:
                try:
                    message_ids.append(response.json().get('id'))
                except ValueError:
                    message_ids.append(None)

        except Exception as e:
            logger.error("Error sending message to %s: %s", url, e)
            message_ids.append(None)

    if screenshot_path and os.path.exists(screenshot_path):
        os.remove(screenshot_path)

    return message_ids


def delete_messages(message_ids: List[Optional[str]]) -> None:
    """
    Delete previously sent Discord messages.
    Each message ID is paired with its originating webhook.
    """
    webhooks = load_webhooks()
    for msg_id, webhook in zip(message_ids, webhooks):
        if msg_id is None:
            continue
        url = f"{webhook['url']}/messages/{msg_id}"
        try:
            response = requests.delete(url, timeout=REQUEST_TIMEOUT)
            if response.status_code not in (200, 204):
                logger.warning(
                    "Failed to delete message %s. Status %d", msg_id, response.status_code
                )
        except Exception as e:
            logger.error("Error deleting message %s: %s", msg_id, e)
