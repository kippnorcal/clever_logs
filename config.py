"""
Commented out "Participation" and "Resource_Usage" as we currently no longer need to pull this data.
Not completely removing these reports and their needed infrastructure in the code (such as the
Connector._process_files_with_datestamp method) just in case we ever need to use it again.
"""

data_reports = {
    # "Participation": "daily-participation",
    # "Resource_Usage": "resource-usage",
    "StudentGoogleAccounts": "idm-reports"
}
