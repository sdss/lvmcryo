# Changelog

## Next release

### 🏷️ Changed

* Set `clear_lock` to `false` in the `production` profile to prevent automated fills from cancelling manual ones.

### ✨ Improved

* Use `defaults` configuration values when a profile doesn't specify a parameter.


## 0.5.2 - 2026-02-02

### 🔧 Fixed

Remove hotfix in `o2_alert`.


## 0.5.1 - 2026-01-17

### ✨ Improved

* Use coroutine function in `ensure_future.on_release_callback`.
* Allow running the LN2 fill in dry-run mode via the API.

### 🔧 Fixed

* Serialise `config_file` path in the `Config` model, which was causing DB load to fail.
* Ensure that `interactive` is properly handled.


## 0.5.0 - 2026-01-17

### 🚀 New

* [#16](https://github.com/sdss/lvmcryo/pull/16) Add a REST API to trigger LN2 fills remotely.

### ✨ Improved

* [#15](https://github.com/sdss/lvmcryo/pull/15) `lvmcryo.runner.ln2_runner` can now be called programmatically with the same functionality as the CLI. The CLI has been modified to use `ln2_runner` without changes in behaviour.
* [#15](https://github.com/sdss/lvmcryo/pull/15) Added ``abort`` CLI command.
* No need to run `pyenv shell --unset` in the manual fill script since it runs in its own shell.
* Add wait time before starting manual fill to allow other fills to be aborted.
* Improve management of profiles and environment variables.

### 🔧 Fixed

* [#15](https://github.com/sdss/lvmcryo/pull/15) Various fixes to ensure that removing the lock file cancels all ongoing fills.
* Prevent multiple calls to `ValveHandler.finish()` overriding the valve close time.


## 0.4.2 - 2026-01-09

### ✨ Improved

* Replace auto-fill restart command.


## 0.4.1 - 2026-01-09

### ✨ Improved

* Unpin `numpy` version.

### 🔧 Fixed

* Fix import of `Sequence` in `tools.py`.


## 0.4.0 - 2026-01-09

### 🚀 New

* Add a `manual-fill.sh` script to perform a manual LN2 fill.
* Add `lvmcryo auto-fill` command to enable/disable automatic fills via a cronjob.

### ✨ Improved

* Monitor lock file and abort if it is deleted during operation.
* Add `clear-lock` command to remove existing lock file.
* Ensure all valves are closed at the end of the LN2 fill process.
* Do not run post-fill validation if the fill was aborted or failed.
* Add retrier for `LN2Handler.stop()`.


## 0.3.18 - 2025-12-24

### ✨ Improved

* [#14](https://github.com/sdss/lvmcryo/pull/14) Add `--no-check-o2-sensors` flag to `lvmcryo ln2` command to disable O2 sensor checks during fill/purge.


## 0.3.17 - 2025-10-26

### ✨ Improved

* Add `--version` flag to the CLI.

### 🔧 Fixed

* Fix validation of `ThermistorConfig` caused by a change in behaviour in `pydantic` 2.12. As a result `lvmcryo` 0.3.16 has been yanked.

### ⚙️ Engineering

* Add very basic testing for `LN2Handler`.


## 0.3.16 - 2025-10-25

### ✨ Improved

* Include date and time of fill in email subject.

### 🔧 Fixed

* Fix turning on/off of all ion pumps at once.
* Prevent O2 alert not being detected when the API is returning no valid alerts data.

### ⚙️ Engineering

* Replace `[tool.uv.dev-dependencies]` with a `[project.optional-dependencies.dev]` in `pyproject.toml`.


## 0.3.15 - 2025-07-30

### ✨ Improved

* Check if aborted/failed before starting purge/fill.
* Prevent race conditions while aborting a fill.purge due to SIGINT/SIGTERM.
* More graceful exit on SIGINT/SIGTERM.

### ⚙️ Engineering

* Pin `numpy` to `<2.3.0` to allow issues with CentOS 7.


## 0.3.14 - 2025-07-05

### 🔧 Fixed

* When validation failed, the error message was not being returned, which caused the email notification to indicate a successful fill.

### ⚙️ Engineering

* Use `uv_build` as build backend.


## 0.3.13 - 2025-05-07

### ✨ Improved

* Add log message indicating successful fill validation.
* Add `--config-file` to `list-profiles`.
* Make `get_internal_config()` aware of the `LVMCRYO_CONFIG_FILE` environment variable.


## 0.3.12 - 2025-05-06

### ✨ Improved

* Add `validation.max_temperature_increase` option to define the maximum temperature increase allowed for the cameras after the LN2 fill.


## 0.3.11 - 2025-04-16

### ✨ Improved

* Monitor LN2 e-stops during fill and before operating a valve.
* Updated the names of the NPSs to which the solenoid valves are connected.


## 0.3.10 - 2025-03-10

### ✨ Improved

* Use `lvmopstools 0.5.10` with support for controlling NPS-connected ion pumps.


## 0.3.9 - 2025-03-05

### ✨ Improved

* Use repr for some placeholders.

### 🔧 Fixed

* Fix several typos in CLI help messages
* Only check the temperature difference of cameras that have been filled.


## 0.3.8 - 2025-01-12

### ✨ Improved

* Try to close as many LN2 valves as possible before failing.
* Add timeouts to valve operations and to the global LN2 runner and actions.


## 0.3.7 - 2025-01-09

### ✨ Improved

* `lmvcryo ion --on` now checks the pressure of the camera (using the Sens4 transducer) and won't turn the ion pump on if the pressure is >1e-4. This can be overridden by passing the `--skip-pressure-check` flag.


## 0.3.6 - 2024-11-29

### ✨ Improved

* Add additional retries for valve and check safe commands.


## 0.3.5 - 2024-11-28

### ✨ Improved

* Log the payload if the DB registration fails.

### 🔧 Fixed

* Emit warnings as `UserWarning` to make sure they are recorded in the logs.
* Fix incorrect use of `sendmail` for multiple recipients in email notifications.


## 0.3.4 - 2024-11-12

### 🏷️ Changed

* Use `smtp-02.lco.cl` as the SMTP server for sending emails.


## 0.3.3 - 2024-11-12

### ✨ Improved

* Include the LVM Web link in a Slack message when the fill starts.

### 🔧 Fixed

* Fix rendering of Grafana URL in Slack message.


## 0.3.2 - 2024-11-11

### ✨ Improved

* [#10](https://github.com/sdss/lvmcryo/pull/10) Update database record during fill. Now the DB record for a fill is created before the purge begins and updated several times during the purge/fill process.


## 0.3.1 - 2024-11-10

### 🔧 Fixed

* Rename `debug` to `with_traceback` in the `production` profile.


## 0.3.0 - 2024-11-10

### 🚀 New

* [#8](https://github.com/sdss/lvmcryo/pull/8) `lvmcryo ln2` now accepts a `--profile` argument that allows to define groups of parameters from an entry in the configuration file.

### ✨ Improved

* Move imports inside CLI callback function to improve startup time.
* Use API to create notifications which are also stored in the database.

### ⚙️ Engineering

* Use `uv` for packaging and dependency management.


## 0.2.1 - 2024-10-10

### ✨ Improved

* Add custom help section `"Post-fill data logging"`.
* Allow to set data logging options via environment variables.
* Add link to Grafana plots in alert and success messages.


## 0.2.0 - 2024-10-08

### 🚀 New

* Add option `--require-all-thermistors`. When passed, the thermistors don't close the valve when they become active. Once all thermistors are active, the valves are all closed at the same time. This can potentially prevent overpressures in the last one or two cryostat being filled as the other valves close.
* Add validation of post-data.

### ✨ Improved

* Improve handling of keyboard interrupt during post-processing.
* Removed several unused default parameters from the configuration file.


## 0.1.6 - 2024-09-26

### ✨ Improved

* Explicitely fail the action on error.
* Preserve empty string in error field in the database.

### 🔧 Fixed

* Always post the fill link in Slack and email.

## 0.1.5 - 2024-09-25

### 🔧 Fixed

* Add additional error handling for non-fatal errors during notifications.


## 0.1.4 - 2024-09-24

### ✨ Improved

* Add `version` to `Config`.
* Various logging improvements.
* Updated internal configuration file with new `lvmapi` port. Update URL routes.
* Add the time at which the thermistor first activated to the valve times written to the database and email.
* Issue notification in Slack during post-fill when waiting extra time to collect data.

### 🔧 Fixed

* Fix JSON log not being saved to the database in some cases.


## 0.1.3 - 2024-09-20

### ✨ Improved

* Do not report a thermistor warning every interval.
* Always include JSON log payload when loading the DB.

### 🔧 Fixed

* Fix link to LVM Web in email templates.


## 0.1.2 - 2024-09-18

### 🚀 New

* Report the link to the LMV Web page for the fill in Slack and email.

### ✨ Improved

* Report if a valve closed due to timeout.

### 🔧 Fixed

* Include extra event times in the success template.


## 0.1.1 - 2024-09-18

### 🚀 New

* Added initial checks for O2 alarms and NPS status.
* Added `--clear-lock` option in `lvmcryo ln2` to remove the lock if present.

### ✨ Improved

* Improved error handling logic.


## 0.1.0 - 2024-09-18

### 🚀 New

* Initial release. Tested in interactive mode and as a Kubernetes cronjob.
