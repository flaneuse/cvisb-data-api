# @name:        access_dropbox.py
# @summary:     Play test file to figure out how Dropbox API works.
# @description: Building up to a larger app that reads/writes from Dropbox; initial tests to figure out how the API works.
# @sources:
# @depends:
# @author:      Laura Hughes
# @email:       lhughes@scripps.edu
# @license:     Apache 2
# @date:        16 March 2018

import dropbox
import pandas as pd
import io

token = ""

# --- ACCESS DROPBOX ---
main_folder = "/CViSB_test"

dbx = dropbox.Dropbox(token)

dbx.users_get_current_account()

# --- LIST FILES ---
# Get files from the current folder:
# Note: entry also lists all metadata for the file, which is great.
# times seem to be in GMT
# useful params:
#   - name:         file name
#   - path_display:   path info (based on home dir)
#   - client_modified/server_modified: when changed
#   - class differs b/w folder and file: dropbox.files.FolderMetadata vs. dropbox.files.FileMetadata
for entry in dbx.files_list_folder(main_folder).entries:
    print(entry)
    print(type(entry))

# Get metadata for particular file
dbx.files_get_metadata("/CViSB_test/test2.csv")

# --- READ ---
# Accessing particular file:
md, res = dbx.files_download("/CViSB_test/test2.csv")
md

res.raw
res.content

pd.read_csv(io.StringIO(res.content.decode('utf-8')))

# Also works: (but unclear how generate key, plus no authentication)
url = 'https://www.dropbox.com/s/flfcc25nnqgr0t6/test2.csv?dl=1'
df2=pd.read_csv(url)

# --- UPLOAD ---
# Modifying / uploading new file.
# NOTE: file must be in binary form to be uploaded.

df = pd.DataFrame({'a': [1, 2], 'b': [2, 3]})


# Save Excel file
output = io.BytesIO()
writer = pd.ExcelWriter(output, engine='xlsxwriter')
df.to_excel(writer, sheet_name='raw_data', index=False)
writer.save()
dbx.files_upload(output.getvalue(), '/test.xlsx')

# CSV
test_file = df.to_csv()

dbx.files_upload(test_file.encode('utf-8'), '/test.csv')
# Testing overriding
df3 = pd.DataFrame({'newa': [10, 20], 'newb': [20, 30]})
# dbx.files_upload(df3.to_csv().encode('utf-8'), '/test.csv') --> API error (b/c already written)
dbx.files_upload(df3.to_csv().encode('utf-8'), '/test.csv', mode=dropbox.files.WriteMode.overwrite)

# NOTE: Do not use this to upload a file larger than 150 MB. Instead, create an upload session with files_upload_session_start().

# --- CREATE FOLDERS ---
# Create a buncha folders
folders = ['Metadata', 'Kenzen', 'Piccolo', 'Vitals', 'ELISA', 'HLA Sequencing']
for folder in folders:
    print(main_folder + "/Data/" + folder)
    dbx.files_create_folder(main_folder + "/Data/" + folder)
