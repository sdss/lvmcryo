# Changelog

## Next version

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
