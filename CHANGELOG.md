# Changelog

## Next version

### ✨ Improved

* Do not report a thermistor warning every interval.

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
