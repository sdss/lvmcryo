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
import traceback
import warnings
from email.message import EmailMessage

from typing import TYPE_CHECKING

import httpx
import pygments
from pydantic import BaseModel, ValidationError
from pygments.formatters import HtmlFormatter
from pygments.lexers.python import PythonTracebackLexer
from pygments.lexers.rust import RustLexer

from lvmopstools.devices.specs import spectrograph_pressures, spectrograph_temperatures

from lvmcryo.config import NotificationLevel, get_internal_config
from lvmcryo.tools import get_fake_logger, render_template


if TYPE_CHECKING:
    from lvmcryo.handlers import LN2Handler


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

    def __init__(self, config_data: dict | str | pathlib.Path | None = None):
        if config_data is None or isinstance(config_data, (str, pathlib.Path)):
            config_data = get_internal_config(config_data)

        if not config_data["notifications"]:
            raise ValidationError("Configuration does not have notifications section.")

        self.config = NotifierConfig(**config_data["notifications"])

        self.disabled: bool = False
        self.slack_disabled: bool = False
        self.email_disabled: bool = False

    def __repr__(self):
        return f"<Notifier (disabled={str(self.disabled).lower()})>"

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
        subject: str = "A message from lvmcryo",
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
        email_host, *email_rest = email_server.split(":")
        email_port: int = 0
        if len(email_rest) == 1:
            email_port = int(email_rest[0])

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
            with smtplib.SMTP(host=email_host, port=email_port) as smtp:
                smtp.send_message(msg)
        except Exception as ee:
            warnings.warn(f"Failed sending email: {ee}")
            return False

        return True

    async def notify_failure(
        self,
        error: str | Exception | None = None,
        handler: LN2Handler | None = None,
        channels: list[str] = ["slack", "email"],
        include_status: bool = True,
        include_log: bool = True,
    ):
        """Notifies a fill failure to the users."""

        log = handler.log if handler else get_fake_logger()

        spec_data: dict[str, dict] | None = None
        if include_status:
            try:
                temp_data = await spectrograph_temperatures()
                pressure_data = await spectrograph_pressures()

                # Massage the data into the format that the template expects.
                cryostats = list(pressure_data)
                spec_data = {}
                for cryostat in cryostats:
                    spec_data[cryostat] = {
                        "ccd": temp_data.get(f"{cryostat}_ccd", -999),
                        "ln2": temp_data.get(f"{cryostat}_ln2", -999),
                        "pressure": pressure_data.get(cryostat, -999),
                    }

            except Exception as err:
                log.error(f"Failed retrieving spectrograph data: {err!r}")

        formatter = HtmlFormatter(style="default")

        log_blob = None
        if include_log:
            fh = getattr(log, "fh", None)
            if fh:
                fh.flush()
            log_filename = getattr(log, "log_filename", None)
            if log_filename:
                log_data = open(log_filename, "r").read()
                log_blob = pygments.highlight(log_data, RustLexer(), formatter)

        for channel in channels:
            try:
                if channel == "slack":
                    slack_message = (
                        "Something went wrong with the LN₂ fill. "
                        "Please check the status of the spectrographs. "
                        "Grafana plots are available <https://lvm-grafana.lco.cl|here>."
                    )
                    if error:
                        if isinstance(error, Exception):
                            trace = "".join(traceback.format_exception(error))
                            slack_message += f"\n```{trace}```"
                        else:
                            slack_message += f"\nThe error was: {error}"

                    await self.post_to_slack(
                        text=slack_message,
                        level=NotificationLevel.error,
                    )

                elif channel == "email":
                    try:
                        if isinstance(error, Exception):
                            email_error = pygments.highlight(
                                "".join(traceback.format_exception(error)),
                                PythonTracebackLexer(),
                                formatter,
                            )
                        else:
                            email_error = error

                        message = render_template(
                            "alert_message.html",
                            render_data=dict(
                                event_times=handler.event_times if handler else {},
                                spec_data=spec_data,
                                log_blob=log_blob,
                                log_css=formatter.get_style_defs(),
                                error=email_error,
                            ),
                        )
                    except Exception as err:
                        log.error(f"Failed rendering alert template: {err!r}")
                        log.warning("Sending a plain text message.")
                        message = "LN2 fill failed. Please check the cryostats!!!"

                    self.send_email(
                        subject="ERROR: LVM LN2 fill failed",
                        message=message,
                    )

            except Exception as err:
                log.error(f"Failed sending alert over {channel}: {err!r}")
                continue
