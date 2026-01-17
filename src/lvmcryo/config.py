#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-09-14
# @Filename: config.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import datetime
import enum
import os
import pathlib
import warnings
from enum import Enum
from functools import lru_cache

from typing import Annotated, Any, Self

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


class InteractiveMode(str, Enum):
    auto = "auto"
    yes = "yes"
    no = "no"


class NotificationLevel(str, Enum):
    info = "info"
    error = "error"


class ParameterOrigin(Enum):
    """Enumeration to track how a parameter was set."""

    DEFAULT = enum.auto()
    ENVVAR = enum.auto()
    COMMAND_LINE = enum.auto()
    FUNCTION_CALL = enum.auto()


class ThermistorConfig(BaseModel):
    """Thermistor configuration model."""

    channel: str | None = Field(default=None)
    monitoring_interval: float = Field(default=1.0)
    close_valve: bool = Field(default=True)
    required_active_time: float = Field(default=10.0)
    disabled: bool = Field(default=False)


class ValveConfig(BaseModel):
    """Valve configuration model."""

    actor: str
    outlet: str
    thermistor: ThermistorConfig | None = Field(default_factory=ThermistorConfig)

    @field_validator("thermistor", mode="before")
    @classmethod
    def validate_thermistor(cls, value: Any) -> ThermistorConfig:
        """Validates the thermistor configuration."""

        if value is None:
            return ThermistorConfig()

        if isinstance(value, ThermistorConfig):
            return value

        return ThermistorConfig(**value)

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

    # Configuration options
    profile: str | None = Field(
        default=None,
        description="The configuration profile to use.",
    )
    config_file: pathlib.Path | None = Field(
        default=None,
        description="The configuration file to use.",
    )

    # Actions
    action: Actions = Field(
        default=Actions.purge_fill,
        description="The action to perform.",
    )
    cameras: list[str] = Field(
        default_factory=list,
        description="List of camera names to use.",
    )

    # Interactivity options
    interactive: InteractiveMode | None = Field(
        default=None,
        description="The interactive mode to use.",
    )
    no_prompt: bool = Field(
        default=False,
        description="Whether to disable prompts.",
    )
    dry_run: bool = Field(
        default=False,
        description="Whether to perform a dry run without opening/closing valves.",
    )
    clear_lock: bool = Field(
        default=False,
        description="Whether to clear the lock file before running.",
    )
    with_traceback: bool = Field(
        default=False,
        description="Whether to show full traceback on errors.",
    )

    # Thermistor and sensor options
    use_thermistors: bool = Field(
        default=True,
        description="Whether to use thermistors.",
    )
    require_all_thermistors: bool = Field(
        default=False,
        description="Whether to require all thermistors to be active.",
    )

    # Initial checks
    check_pressures: bool = Field(
        default=True,
        description="Whether to check pressures.",
    )
    check_temperatures: bool = Field(
        default=True,
        description="Whether to check temperatures.",
    )
    check_o2_sensors: bool = Field(
        default=True,
        description="Whether to check oxygen sensors.",
    )
    max_pressure: float | None = Field(
        default=None,
        description="Maximum cryostat pressure if checking pressures.",
    )
    max_temperature: float | None = Field(
        default=None,
        description="Maximum cryostat temperature if checking temperatures.",
    )

    # Timeout options
    purge_time: float | None = Field(
        default=None,
        description="Purge time in seconds.",
    )
    min_purge_time: float | None = Field(
        default=None,
        description="Minimum purge time in seconds.",
    )
    max_purge_time: float | None = Field(
        default=None,
        description="Maximum purge time in seconds.",
    )
    fill_time: float | None = Field(
        default=None,
        description="Fill time in seconds.",
    )
    min_fill_time: float | None = Field(
        default=None,
        description="Minimum fill time in seconds.",
    )
    max_fill_time: float | None = Field(
        default=None,
        description="Maximum fill time in seconds.",
    )
    max_temperature_increase: float = Field(
        default=0.0,
        description="Maximum temperature increase allowed.",
    )

    # Output and notification options
    verbose: bool = Field(
        default=False,
        description="Whether to enable verbose output.",
    )
    quiet: bool = Field(
        default=False,
        description="Whether to suppress output.",
    )
    notify: bool = Field(
        default=True,
        description="Whether to send notifications.",
    )
    slack: bool = Field(
        default=True,
        description="Whether to send Slack notifications.",
    )
    email: bool = Field(
        default=True,
        description="Whether to send email notifications.",
    )
    email_level: NotificationLevel = Field(
        default=NotificationLevel.error,
        description="The notification level for emails.",
    )

    # Logging options
    write_log: bool = Field(
        default=False,
        description="Whether to write logs to a file.",
    )
    log_path: pathlib.Path | None = Field(
        default=None,
        description="Path to the log file.",
    )
    write_json: bool = Field(
        default=False,
        description="Whether to write JSON output.",
    )
    write_data: bool = Field(
        default=False,
        description="Whether to write data output.",
    )
    data_path: pathlib.Path | None = Field(
        default=None,
        description="Path to the data file.",
    )
    data_extra_time: float = Field(
        default=0.0,
        description="Extra time to add to data collection.",
    )

    version: str | None = Field(
        default=None,
        description="The version of the lvmcryo package.",
    )

    valve_info: dict[str, ValveConfig] = {}

    _internal_config: Annotated[Configuration, PrivateAttr] = Configuration({})

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

        if value in ["yes", True, 1]:
            if is_container():
                warnings.warn(
                    "Interactive mode may not work in containers.",
                    UserWarning,
                )
            return InteractiveMode.yes

        if value in ["no", False, 0]:
            return InteractiveMode.no

        if value in ["auto", None]:
            if is_container():
                return InteractiveMode.no
            else:
                return InteractiveMode.yes

        raise ValueError(f"Invalid value for interactive mode: {value!r}")

    @field_serializer("action", "interactive", "email_level")
    def serialize_enums(self, enum) -> str:
        """Serialises the enumerations to string."""

        return enum.value

    @field_serializer("log_path", "data_path")
    def serialize_path(self, path: pathlib.Path | None) -> str | None:
        """Serialises the path."""

        return str(path) if path is not None else None

    @model_validator(mode="before")
    @classmethod
    def before_validator(cls, data: Any) -> Any:
        # Fill out the interactive value for now with None. Will be validated later.
        if "interactive" not in data:
            data["interactive"] = None

        return data

    @model_validator(mode="after")
    def validate_after(self) -> Self:
        """Performs validations after the fields have been set."""

        defaults = self.get_defaults()

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
            if getattr(self, field) is None:
                setattr(self, field, default_value)

        if self.cameras == []:
            self.cameras = defaults.get("cameras", [])

        if self.valve_info is None or self.valve_info == {}:
            if "valve_info" not in self._internal_config:
                raise ValueError("No valve_info defined in the configuration file.")
            for valve, valve_info in self._internal_config["valve_info"].items():
                self.valve_info[valve] = ValveConfig(**valve_info)

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
                    "use_thermistors=True requires defining max_fill_time and "
                    "max_purge_time. This probably indicates a problem with your "
                    "configuration file."
                )

        return self

    def get_internal_config(self) -> Configuration:
        """Returns the internal configuration object."""

        return self._internal_config

    def get_defaults(self) -> dict[str, Any]:
        """Returns a dictionary with the default values for all parameters."""

        defaults = self._internal_config.get("defaults", {})

        return defaults

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
