#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-12-03
# @Filename: notifier.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import smtplib
from dataclasses import dataclass
from email.message import EmailMessage

import httpx

from lvmcryo import config, log


@dataclass
class Notifier:
    """Sends notifications over Slack or email."""

    silent: bool = False

    async def send_to_slack(
        self,
        text: str | None = None,
        blocks: list[dict] | None = None,
        channel: str | None = None,
        is_alert: bool = False,
    ):
        """Posts a message to Slack.

        Parameters
        ----------
        text
            Plain text to send to the Slack channel.
        blocks
            A list of blocks to send to the Slack channel. These follow the Slack
            API format for blocks. Incompatible with ``text``.
        channel
            The channel in the SSDS-V workspace where to send the message. Defaults
            to the configuration value.
        is_alert
            If ``True``, tags people in the message to raise visibility.

        """

        if self.silent or not config["notifications"]:
            log.warning("Notifications are disabled. Not sending Slack message.")
            return

        base_url = config["api.url"]
        route = config["notifications.slack.post_message_route"]
        channel = config["notifications.slack.channel"]

        if text is None and blocks is None:
            raise ValueError("Either text or blocks must be defined.")
        elif text is not None and blocks is not None:
            raise ValueError("Only one of text or blocks can be defined.")

        if is_alert:
            alert_users: list[str] = config.get("notifications.alerts.users.slack", [])

            user_ids: list[str] = []
            for user in alert_users:
                try:
                    user_ids.append(await self._get_slack_user_id(user))
                except RuntimeError:
                    log.warning(f"Failed getting userID for {user}.")

            user_ids = list(map(lambda s: f"<@{s}>", user_ids))

            if len(alert_users) > 0:
                if text is not None:
                    text = " ".join(user_ids) + " " + text
                elif blocks is not None:
                    blocks.insert(
                        0,
                        {
                            "type": "section",
                            "text": {
                                "type": "plain_text",
                                "text": " ".join(user_ids),
                            },
                        },
                    )

        async with httpx.AsyncClient(base_url=base_url) as client:
            await client.post(
                route,
                json={
                    "channel": channel,
                    "text": text,
                    "blocks": blocks,
                },
            )

    async def _get_slack_user_id(self, display_name: str) -> str:
        """Gets the userID of a Slack user from its display name."""

        base_url = config["api.url"]
        route = config["notifications.slack.user_id_route"]

        async with httpx.AsyncClient(base_url=base_url) as client:
            response = await client.get(f"{route}/{display_name}")

        if response.status_code != 200 or response.json() is None:
            raise RuntimeError("Failed getting userID from Slack.")

        return response.json()

    def send_email(
        self,
        recipients: list[str] | None = None,
        subject: str | None = None,
        message: str | None = None,
    ):
        """Sends an email to a list of recipients.

        Parameters
        ----------
        recipients
            A list of email addresses to send the email to. If ``None``, uses the
            default recipients in the configuration.
        subject
            The subject of the email.
        message
            The body of the email.

        """

        if self.silent or not config["notifications"]:
            log.warning("Notifications are disabled. Not sending email message.")
            return

        recipients = recipients or config["notifications.alerts.users.email"]
        assert recipients is not None

        subject = subject or "Alert during LVM LN2 fill"

        if message is None:
            log.warning("No email body defined. Sending empty email.")
            message = ""

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = config["notifications.email.smtp.sender"]
        msg["To"] = ", ".join(recipients)

        if "<html>" in message.lower():
            msg.set_content(
                "This email is in HTML format. Please use an "
                "HTML-capable email client to read it."
            )
            msg.add_alternative(message, subtype="html")
        else:
            msg.set_content(message)

        if (reply_to := config["notifications.email.smtp.reply-to"]) is not None:
            msg["Reply-To"] = reply_to

        host = config["notifications.email.smtp.host"]
        port = config["notifications.email.smtp.port"]

        with smtplib.SMTP(host=host, port=port) as smtp:
            smtp.send_message(msg)
