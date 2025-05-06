#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-09-14
# @Filename: config.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import datetime
import os
import pathlib
import warnings
from enum import Enum
from functools import lru_cache

from typing import Annotated, Any, Self

from click.core import ParameterSource
from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    field_serializer,
    field_validator,
    model_validator,
)

from sdsstools.configuration import Configuration


class Actions(str, Enum):
    purge_fill = "purge-and-fill"
    purge = "purge"
    fill = "fill"
    abort = "abort"
    clear_lock = "clear-lock"


class InteractiveMode(str, Enum):
    auto = "auto"
    yes = "yes"
    no = "no"


class NotificationLevel(str, Enum):
    info = "info"
    error = "error"


class ThermistorConfig(BaseModel):
    """Thermistor configuration model."""

    channel: str | None = None
    monitoring_interval: float = 1.0
    close_valve: bool = True
    required_active_time: float = 10.0
    disabled: bool = False


class ValveConfig(BaseModel):
    """Valve configuration model."""

    actor: str
    outlet: str
    thermistor: ThermistorConfig | None = Field(default_factory=ThermistorConfig)

    @model_validator(mode="after")
    def validate_after(self) -> Self:
        """Validates the valve configuration after the fields have been set."""

        if self.thermistor and self.thermistor.channel is None:
            self.thermistor.channel = self.outlet

        return self


ExcludedField = Field(repr=False, exclude=True)


@lru_cache()
def get_internal_config(path: pathlib.Path | str | None = None) -> Configuration:
    """Returns the internal configuration."""

    default_path = pathlib.Path(__file__).parent / "config.yaml"
    envvar_config_file = os.environ.get("LVMCRYO_CONFIG_FILE", None)

    if path is None and envvar_config_file is not None:
        path = envvar_config_file

    return Configuration(path, base_config=default_path)


class Config(BaseModel):
    """Configuration model.

    This model defines the options with which a purge/fill process will run.
    It is usually generated from the options passed to a CLI command after
    they are parsed by `.process_cli_options`.

    """

    action: Actions = Actions.purge_fill
    cameras: list[str] = []  # Empty list means all cameras.

    interactive: InteractiveMode = InteractiveMode.auto
    no_prompt: bool = False
    dry_run: bool = False
    clear_lock: bool = False
    with_traceback: bool = False

    use_thermistors: bool = True
    require_all_thermistors: bool = False
    check_pressures: bool = True
    check_temperatures: bool = True
    max_pressure: float | None = None
    max_temperature: float | None = None
    purge_time: float | None = None
    min_purge_time: float | None = None
    max_purge_time: float | None = None
    fill_time: float | None = None
    min_fill_time: float | None = None
    max_fill_time: float | None = None

    max_temperature_increase: float = 0.0

    verbose: bool = False
    quiet: bool = False
    notify: bool = True
    slack: bool = True
    email: bool = True
    email_level: NotificationLevel = NotificationLevel.error

    write_log: bool = False
    log_path: pathlib.Path | None = None
    write_json: bool = False
    write_data: bool = False
    data_path: pathlib.Path | None = None
    data_extra_time: float = 0.0

    version: str | None = None

    valve_info: Annotated[dict[str, ValveConfig], ExcludedField] = {}

    config_file: Annotated[pathlib.Path | None, ExcludedField] = None
    _internal_config: Annotated[Configuration, PrivateAttr] = Configuration({})

    error: Annotated[bool, ExcludedField] = False

    profile: str | None = None
    param_source: Annotated[dict[str, ParameterSource | None], ExcludedField] = {}

    def model_post_init(self, __context: Any) -> None:
        self._internal_config = get_internal_config(self.config_file)
        return super().model_post_init(__context)

    @property
    def internal_config(self):
        return self._internal_config

    @field_validator("interactive", mode="after")
    @classmethod
    def validate_interactive(cls, value: InteractiveMode) -> InteractiveMode:
        """Returns the interactive mode."""

        from lvmcryo.tools import is_container

        is_container = is_container()

        if value in ["yes", True, 1]:
            if is_container:
                warnings.warn(
                    "Interactive mode may not work in containers.",
                    UserWarning,
                )
            return InteractiveMode.yes

        if value in ["no", False, 0]:
            return InteractiveMode.no

        if value in ["auto", None]:
            if is_container:
                return InteractiveMode.no
            else:
                return InteractiveMode.yes

        raise ValueError(f"Invalid value for interactive mode: {value!r}")

    @model_validator(mode="before")
    @classmethod
    def before_validator(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            # Not sure if this is likely to happen.
            return data

        # Ensure we have an internal configuration dictionary.
        config = get_internal_config(data.get("config_file", None))
        defaults = config.get("defaults", {})

        # If a profile has been passed, we update the input parameters with those
        # in the profile. But we want to do that only for cases in which the user
        # has not explicitly passed the parameter as a flag.
        if (profile := data["profile"]) is not None:
            profile_data = config.get("profiles", {}).get(profile, {})
            param_source = data.get("param_source", {})
            for key in profile_data:
                if key not in data:
                    warnings.warn(f"Unknwon parameter in profile: {key}", UserWarning)
                    continue
                psource = param_source.get(key, None)
                if psource != ParameterSource.COMMANDLINE:
                    data[key] = profile_data[key]

        # Use internal configuration files to fill in missing fields.
        for field in [
            "min_purge_time",
            "max_purge_time",
            "min_fill_time",
            "max_fill_time",
            "max_pressure",
            "max_temperature",
            "max_temperature_increase",
        ]:
            default_value = defaults.get(field, None)
            if field not in data or data[field] is None:
                data[field] = default_value

        if "cameras" not in data or data["cameras"] == []:
            if "cameras" not in defaults:
                raise ValueError("No cameras defined in the configuration file.")
            data["cameras"] = defaults["cameras"]

        if "valve_info" not in data or data["valve_info"] is None:
            if "valve_info" not in config:
                raise ValueError("No valve_info defined in the configuration file.")
            data["valve_info"] = config["valve_info"]

        # Fill out the interactive value for now with None. Will be validated later.
        if "interactive" not in data:
            data["interactive"] = None

        return data

    @field_serializer("action", "interactive", "email_level")
    def serialize_enums(self, enum) -> str:
        """Serialises the enumerations to string."""

        return enum.value

    @field_serializer("log_path", "data_path")
    def serialize_path(self, path: pathlib.Path | None) -> str | None:
        """Serialises the path."""

        return str(path) if path is not None else None

    @model_validator(mode="after")
    def validate_after(self) -> Self:
        """Performs validations after the fields have been set."""

        # At this point we should have narrowed down the interactive mode.
        if self.interactive not in [InteractiveMode.yes, InteractiveMode.no]:
            raise ValueError("Invalid interactive mode after validation.")

        # If we are not in interactive mode, we set no_prompt to True.
        if self.interactive == InteractiveMode.no:
            self.no_prompt = True

        defaults = self._internal_config.get("defaults", {})

        # Ensure the log path is set correctly.
        if self.log_path is not None:
            self.write_log = True
        elif self.write_log is False:
            self.log_path = None
        else:
            if self.log_path is not None:
                self._check_log_path(self.log_path)
            else:
                default_path = defaults.get("log_path", "./{timestamp}.log")
                self.log_path = pathlib.Path(
                    default_path.format(timestamp=self._log_basename())
                )

        # Ensure the data path is set correctly.
        if self.data_path is not None:
            self.write_data = True
        elif self.write_data is False:
            self.data_path = None
        else:
            if self.data_path is not None:
                self._check_log_path(self.data_path)
            else:
                if self.log_path is not None:
                    self.data_path = self.log_path.with_suffix(".parquet")
                else:
                    default_path = defaults.get("data_path", "./{timestamp}.parquet")
                    self.data_path = pathlib.Path(
                        default_path.format(timestamp=self._log_basename("parquet"))
                    )

        # We won't write a JSON file if we are not writing a normal log.
        if not self.write_log:
            self.write_json = False

        # We cannot purge/fill without thermistors and without fill and purge times
        # unless we are in interactive mode.
        if not self.use_thermistors and not self.interactive:
            no_purge_time = self.purge_time is None
            no_fill_time = self.fill_time is None

            if self.action == Actions.purge_fill and (no_fill_time or no_purge_time):
                raise ValueError(
                    "use_thermistors=False requires interactive mode "
                    "or defining purge and fill times."
                )

            if self.action == Actions.purge and no_purge_time:
                raise ValueError(
                    "use_thermistors=False requires interactive mode "
                    "or defining a purge time."
                )

            if self.action == Actions.fill and no_fill_time:
                raise ValueError(
                    "use_thermistors=False requires interactive mode "
                    "or defining a fill time."
                )

        # use_thermistors cannot be used with fill/purge times.
        if self.use_thermistors and (self.purge_time or self.fill_time):
            raise ValueError("use_thermistors cannot be used with purge/fill times.")

        # If we are using thermistors, require max_fill_time and max_purge_time.
        if self.use_thermistors:
            if self.max_fill_time is None or self.max_purge_time is None:
                raise ValueError(
                    "use_thermistors=True defining max_fill_time and max_purge_time. "
                    "This probably indicates a problem with your configuration file."
                )

        return self

    def _check_log_path(self, path: pathlib.Path, extension="log") -> pathlib.Path:
        """Checks that the log path is valid."""

        if path.is_dir():
            return path / self._log_basename(extension)
        else:
            if path.exists():
                raise ValueError(f"Path {path} already exists.")

        return path

    def _log_basename(self, extension: str | None = None) -> str:
        """Generates a log basename with a timestamp."""

        extension = extension or ""

        now = datetime.datetime.now(datetime.UTC)
        iso = now.isoformat(timespec="seconds").split("+")[0]

        if extension and not extension.startswith("."):
            extension = f".{extension}"

        return f"lvmcryo_{iso}{extension}"
