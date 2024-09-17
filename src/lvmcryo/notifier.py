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
import uuid
import warnings
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
    slack_channels: dict[NotificationLevel, str | list[str]]
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

        self.config = NotifierConfig(
            slack_route=config_data["api_routes"]["slack"],
            **config_data["notifications"],
        )

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
        channels: str | list[str] | None = None,
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
        channels
            The channel in the SSDS-V workspace where to send the message. Defaults
            to the configuration value. Can be a string or a list of strings, in
            which case the message is sent to all the channels in the list.

        """

        if self.disabled or self.slack_disabled:
            return

        route = self.config.slack_route

        channels = channels or self.config.slack_channels[level]
        if isinstance(channels, str):
            channels = [channels]

        if mentions is None:
            if level in self.config.slack_mentions:
                mentions = self.config.slack_mentions[level]
            else:
                mentions = []

        if isinstance(mentions, str):
            mentions = [mentions]

        failed: bool = False
        for channel in channels:
            async with httpx.AsyncClient(follow_redirects=True) as client:
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
                failed = True

        return not failed

    def send_email(
        self,
        html_message: str | None = None,
        plaintext_message: str | None = None,
        subject: str = "A message from lvmcryo",
        recipients: list[str] | None = None,
        from_address: str | None = None,
        images: dict[str, pathlib.Path | None] = {},
        email_server: str | None = None,
    ):
        """Sends an email to a list of recipients.

        Parameters
        ----------
        html_message
            The body of the email as an HTML string.
        plaintext_message
            An alternative plaintext version of the email body.
        subject
            The subject of the email.
        recipients
            A list of email addresses to send the email to. If ``None``, uses the
            default recipients in the configuration.
        from_address
            The email address to send the email from. If ``None``, uses the default
            address in the configuration.
        images
            A mapping of images to embed. This requires an ``html_message``. The
            mapping must be in the form ``{xyx: path}``, where ``xyz`` is the
            content ID key associated to the image. In the HTML text there must
            be a corresponding ``<img src="cid:xyz">`` tag.
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

        msg = MIMEMultipart("alternative" if html_message else "mixed")
        msg["Subject"] = subject
        msg["From"] = from_address
        msg["To"] = ", ".join(recipients)
        msg["Reply-To"] = email_reply_to

        plain = MIMEText(
            plaintext_message
            or "A message was sent from lvmcryo but your email "
            "client cannot process HTML.",
            "plain",
        )
        msg.attach(plain)

        if html_message:
            for cid, image in images.items():
                # Need to replace the cid with a globally unique one.
                cid_unique = f"{cid}-{uuid.uuid4()!s}"
                html_message = html_message.replace(f"cid:{cid}", f"cid:{cid_unique}")

                if image is not None and pathlib.Path(image).exists():
                    with open(image, "rb") as fp:
                        image = MIMEImage(fp.read())

                    # Specify the  ID according to the img src in the HTML part
                    image.add_header("Content-ID", f"<{cid_unique}>")
                    msg.attach(image)

            html = MIMEText(html_message, "html")
            msg.attach(html)

        try:
            with smtplib.SMTP(host=email_host, port=email_port) as smtp:
                smtp.sendmail(from_address, ", ".join(recipients), msg.as_string())
        except Exception as ee:
            warnings.warn(f"Failed sending email: {ee}")
            return False

        return True

    async def notify_after_fill(
        self,
        success: bool,
        error_message: str | Exception | None = None,
        handler: LN2Handler | None = None,
        post_to_slack: bool = True,
        send_email: bool = True,
        include_status: bool = True,
        include_log: bool = True,
        images: dict[str, pathlib.Path | None] = {},
    ):
        """Notifies a fill success or failure to the users."""

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

        if post_to_slack:
            try:
                if success:
                    slack_message = "LN₂ fill completed successfully."
                else:
                    slack_message = (
                        "Something went wrong with the LN₂ fill. "
                        "Please check the status of the spectrographs. "
                        "Grafana plots are available <https://lvm-grafana.lco.cl|here>."
                    )
                    if error_message:
                        if isinstance(error_message, Exception):
                            trace = "".join(traceback.format_exception(error_message))
                            slack_message += f"\n```{trace}```"
                        else:
                            slack_message += f"\nThe error was: {error_message}"

                await self.post_to_slack(
                    text=slack_message,
                    level=NotificationLevel.error,
                )

            except Exception as err:
                log.error(f"Failed posting to Slack: {err!r}")

        if send_email:
            subject: str = (
                "SUCCESS: LVM LN2 fill completed"
                if success
                else "ERROR: LVM LN2 fill failed"
            )
            plain_message: str = (
                "LN2 fill completed successfully."
                if success
                else "LN2 fill failed. Please check the cryostats."
            )
            html_message: str | None = None
            email_error: str | None = None
            template = "success_message.html" if success else "alert_message.html"

            try:
                try:
                    if not success:
                        if isinstance(error_message, Exception):
                            email_error = pygments.highlight(
                                "".join(traceback.format_exception(error_message)),
                                PythonTracebackLexer(),
                                formatter,
                            )
                        else:
                            email_error = error_message

                        plain_message = (
                            "LN2 fill failed. Please check the cryostats. "
                            "You are not receiving a full message because your email "
                            "client does not support HTML."
                        )

                    html_message = render_template(
                        template,
                        render_data=dict(
                            event_times=handler.event_times if handler else {},
                            spec_data=spec_data,
                            log_blob=log_blob,
                            log_css=formatter.get_style_defs(),
                            error=email_error,
                            images=True if len(images) > 0 else False,
                        ),
                    )

                except Exception as err:
                    log.error(f"Failed rendering template: {err!r}")
                    log.warning("Sending a plain text message.")

                self.send_email(
                    subject=subject,
                    html_message=html_message,
                    plaintext_message=plain_message,
                    images=images,
                )

            except Exception as err:
                log.error(f"Failed sending email: {err!r}")
