#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-12-03
# @Filename: notifier.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib
import smtplib
import warnings
from email.message import EmailMessage

import httpx
from pydantic import BaseModel, ValidationError

from sdsstools.configuration import Configuration

from lvmcryo.config import NotificationLevel, get_internal_config


class NotifierConfig(BaseModel):
    """Configuration for the Notifier class."""

    slack_route: str
    slack_channels: dict[NotificationLevel, str]
    slack_mentions: dict[NotificationLevel, str | list[str]] = {}
    slack_from: str = "LN₂ Helper"

    email_recipients: list[str]
    email_server: str
    email_from: str
    email_reply_to: str | None = None


class Notifier:
    """Sends notifications over Slack or email."""

    def __init__(self, config_data: Configuration | dict):
        if not config_data["notifications"]:
            raise ValidationError("Configuration does not have notifications section.")

        self.config = NotifierConfig(**config_data["notifications"])

        self.disabled: bool = False
        self.slack_disabled: bool = False
        self.email_disabled: bool = False

    def __repr__(self):
        return self.config.__repr__()

    @classmethod
    def from_config(cls, config: Configuration | dict | str | pathlib.Path) -> Notifier:
        """Creates a `.Notifier` instance from a configuration object or file."""

        if isinstance(config, (dict, Configuration)):
            return cls(config)

        config_data = get_internal_config(config)
        return cls(config_data)

    async def post_to_slack(
        self,
        text: str | None = None,
        level: NotificationLevel = NotificationLevel.info,
        mentions: str | list[str] | None = None,
        channel: str | None = None,
    ):
        """Posts a message to Slack.

        Parameters
        ----------
        text
            Plain text to send to the Slack channel.
        level
            The level of the message. Determines the channel where the message
            is sent.
        mentions
            A list of Slack users to mention in the message.
        channel
            The channel in the SSDS-V workspace where to send the message. Defaults
            to the configuration value.

        """

        if self.disabled or self.slack_disabled:
            return

        route = self.config.slack_route
        channel = channel or self.config.slack_channels[level]

        if mentions is None:
            if level in self.config.slack_mentions:
                mentions = self.config.slack_mentions[level]
            else:
                mentions = []

        if isinstance(mentions, str):
            mentions = [mentions]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                route,
                json={
                    "channel": channel,
                    "text": text,
                    "username": self.config.slack_from,
                    "mentions": mentions,
                },
            )

        if response.status_code != 200:
            warnings.warn(f"Failed sending message to Slack: {response.text}")
            return False

        return True

    def send_email(
        self,
        message: str,
        subject: str = "A message from the LVM Cryo system",
        recipients: list[str] | None = None,
        from_address: str | None = None,
        email_server: str | None = None,
    ):
        """Sends an email to a list of recipients.

        Parameters
        ----------
        message
            The body of the email. Can be a string in HTML format.
        subject
            The subject of the email.
        recipients
            A list of email addresses to send the email to. If ``None``, uses the
            default recipients in the configuration.
        from_address
            The email address to send the email from. If ``None``, uses the default
            address in the configuration.
        email_server
            The SMTP server to use. If ``None``, uses the default server in the
            configuration.

        """

        if self.disabled or self.email_disabled:
            return

        recipients = recipients or self.config.email_recipients
        from_address = from_address or self.config.email_from
        email_server = email_server or self.config.email_server
        email_reply_to = self.config.email_reply_to or from_address

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_address
        msg["To"] = ", ".join(recipients)
        msg["Reply-To"] = email_reply_to

        if "<html>" in message.lower():
            msg.set_content(
                "This email is in HTML format. Please use an "
                "HTML-capable email client to read it."
            )
            msg.add_alternative(message, subtype="html")
        else:
            msg.set_content(message)

        try:
            with smtplib.SMTP(host=email_server) as smtp:
                smtp.send_message(msg)
        except Exception as ee:
            warnings.warn(f"Failed sending email: {ee}")
            return False

        return True
