#
# This script modifies dbt docs website files generated by executing "dbt docs generate"
# for a dbt project. These are all custom modifications.
#

# Import useful libraries.
import json
import os
import re

# Set path to the dbt project that we executed "dbt docs generate" for.
PATH_DBT_PROJECT = '../dbt_data_baddies_datafest'  # relative path for running locally from scripts directory

###########################################################
# Read file contents in.
###########################################################

# Read content of the index.html file that was generated by executing "dbt docs generate".
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'index.html'), 'r', encoding="utf8") as f:
    html_index = f.read()

# Read content of the manifest.json file that was generated by executing "dbt docs generate".
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'manifest.json'), 'r') as f:
    json_manifest = json.loads(f.read())

# Read content of the catalog.json file that was generated by executing "dbt docs generate".
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'catalog.json'), 'r') as f:
    json_catalog = json.loads(f.read())

# Read content of the docs_ourstyle.css file that we created.
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'assets/docs_ourstyle.css'), 'r', encoding="utf8") as f:
    docs_ourstyle = f.read()

###########################################################
# Modify the content read from the index.html file.
# Note: In future should explore using an html parsing 
#       library to do this.
###########################################################

# Append custom css to the <head>.
html_index = html_index.replace(
    '</head>', 
    '<style>' + docs_ourstyle + '</style>\n</head>'
)

# Change the document title.
html_index = html_index.replace(
    '<title>dbt Docs</title>',
    '<title>Data Baddies</title>'
)

# Change the document favicon.
html_index = re.sub(
    r'<link rel="shortcut icon"(.*)/>',
    '<link rel="icon" type="image/x-icon" href="assets/data_baddies_selfie.jpg">',
    html_index
)

# Change the document logo (make it non-clickable).
html_index = html_index.replace(
    '<img style="width: 100px; height: 40px" class="logo" ng-src="{{ logo }}" />',
    '<img class="logo" src="assets/data_baddies_selfie.jpg" style="width: 100px; height: 40px" />'
)

# Modify the existing header to replace 'dbt' with 'Data Baddies' if present
html_index = re.sub(
    r'<h1[^>]*>dbt</h1>',  # Matches the <h1> element with dbt
    '<h1>Data Baddies</h1>',  # Replace with your title
    html_index
)

###########################################################
# Modify the content read from the json.manifest file.
###########################################################

# While models can be hidden using dbt_project.yml, macros cannot, so this code is needed.
# Code credit: https://getdbt.slack.com/archives/C01NH3F2E05/p1694608854050879?thread_ts=1694461283.130219&cid=C01NH3F2E05
IGNORE_PROJECTS = ['dbt', 'dbt_date', 'dbt_utils']
for macro in json_manifest['macros']:
    for ignore_project in IGNORE_PROJECTS:
        if ignore_project in macro:
            json_manifest['macros'][macro]['docs']['show'] = False

###########################################################
# Write out.
###########################################################

# Write to index.html
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'index.html'), 'w', encoding="utf8") as f:
    f.write(html_index)

# Write to manifest.json
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'manifest.json'), 'w', encoding="utf8") as f:
    f.write(json.dumps(json_manifest))

# Write to catalog.json
with open(os.path.join(PATH_DBT_PROJECT, 'target', 'catalog.json'), 'w', encoding="utf8") as f:
    f.write(json.dumps(json_catalog))

print("Executed Successfully")
