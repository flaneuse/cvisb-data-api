# @name:        create_fake_data.py
# @summary:     Creates a series of fake patients and "data" simulating CViSB data
# @description: For prototyping and testing out CViSB data management website, creating a series of fake patients and data files.
# @sources:
# @depends:
# @author:      Laura Hughes
# @email:       lhughes@scripps.edu
# @license:     Apache-2.0
# @date:        16 March 2018


import pandas as pd
import numpy as np
import io
import dropbox

# [ Set up params ] ---------------------------------------------------------------------------------------
token = ""
dropbox_folder = "/CViSB_test"
expt_file = 'expt_list.csv'

# [ Set up fake patient generator ] -----------------------------------------------------------------------
def fakePatients(number = 25):
    ids = np.arange(number)
    patients = pd.DataFrame()


    create_id = np.vectorize(lambda x: 'fakeid' + str(x).zfill(4))
    patients['patient_id'] = create_id(ids)
    patients['sex'] = patients.apply(create_sex, axis = 1)
    patients['age'] = patients.apply(create_age, axis = 1)
    patients['cohort'] = patients.apply(create_cohort, axis = 1)
    patients['cohort_exposure'] = patients.apply(create_exposure, axis = 1)
    patients['timepoints'] = patients.apply(create_timepts, axis = 1)

    return patients


def create_sex(x):
    if (np.random.rand() > 0.5):
        return('male')
    else:
        return('female')

def create_age(x):
    return round(np.random.rand()*100)

def create_cohort(x):
    if (np.random.rand() > 0.67):
        return('Ebola')
    else:
        return('Lassa')

def create_exposure(x):
    rand_num = np.random.rand()
    if (rand_num > 0.8):
        return("exposed")
    elif (rand_num > 0.1):
        if (np.random.rand() > 0.2):
            return("died")
        else:
            return("survived")
    else:
        return("community")


def create_timepts(x):
    rand_num = np.random.rand()
    timepts = [
    [0, 1],
    [0, 1, 2],
    [0, 1, 2, 3],
    [0, 1, 2, 3, 4],
    [0, 1, 2, 3, 4, 7],
    [0, 1, 2, 3, 4, 7, 10]
    ]
    if (rand_num < 0.4):
        return(timepts[0])
    elif (rand_num < 0.6):
        return(timepts[1])
    elif (rand_num < 0.6):
        return(timepts[2])
    elif (rand_num < 0.6):
        return(timepts[3])
    elif (rand_num < 0.6):
        return(timepts[4])
    else:
        return(timepts[5])

# [ Create patients ] -------------------------------------------------------------------------------------
patients = fakePatients()

# --- Upload to dropbox ---
dbx = dropbox.Dropbox(token)

dbx.files_upload(patients.to_csv(index = False).encode('utf-8'), dropbox_folder + '/fakepatient_roster.csv')
# dbx.files_upload(patients.to_csv(sep = '\t', index = False).encode('utf-8'), '/fakepatient_roster.tsv')


# [ Create samples ] --------------------------------------------------------------------------------------
# Convert array of timepoints to wide dataframe of timepoints
# TODO: use the function I wrote
tp = pd.DataFrame(patients['timepoints'].values.tolist(), index = patients.patient_id).reset_index()
# Convert to long dataframe
tp = pd.melt(tp, id_vars = ['patient_id'], value_name = 'timepoint').drop('variable', axis = 1)
# Remove NAs
tp = tp.dropna(axis = 0, how='any')



# Create a sample for every timepoint
sample_list = pd.DataFrame(data = {'sample_id': ['plasma', 'PMBC', 'hDNA', 'vDNA', 'hRNA', 'vRNA'], 'description': ['raw blood plasma', 'raw peripheral blood mononuclear cells', 'extracted host DNA', 'extracted viral DNA', 'extracted host RNA', 'extracted viral RNA']})

sample_list['tmp'] = 1
tp['tmp'] = 1

# Merge on drop timepoint 0; no biological data taken
samples = pd.merge(tp[tp.timepoint > 0], sample_list, on='tmp').drop('tmp', axis = 1)

# Fill some fields to be inputted later.
samples['creation_date'] = np.NaN
samples['storage_loc'] = np.NaN
samples['invalid'] = False


dbx.files_upload(samples.to_csv(index = False).encode('utf-8'), dropbox_folder + '/fakesample_list.csv', mode=dropbox.files.WriteMode.overwrite)

# [ Generate file list ] ----------------------------------------------------------------------------------
tp.drop('tmp', axis = 1)

def gen_filelist(patient_timepts):
    # read in the experiment file structure
    md, res = dbx.files_download(dropbox_folder +"/" + expt_file)
    if(res.status_code == 200):
        expts = pd.read_csv(io.StringIO(res.content.decode('utf-8')))


expts


def array2long(df, var, id_var):
    # remove any NAs in column
    df = df[df[var].notnull()]

    if(any(df[var].apply(lambda x: type(x)) == str)):
        # Convert string to array
        df[var] = df[var].apply(lambda x: x.replace(', ', ',').split(','))

    # splay data frame wide
    temp = pd.DataFrame(df[var].values.tolist(), index = df[id_var]).reset_index()

    # Convert to long dataframe
    temp = pd.melt(temp, id_vars = [id_var], value_name = var).drop('variable', axis = 1)
    # Remove NAs
    temp = temp.dropna(axis = 0, how='any')

    return temp

# BUG: fix the .loc for NAN setting
ex_times = array2long(expts, 'timepts', 'expt_id')
ex_files = array2long(expts, 'file_types', 'expt_id')
ex_pars = array2long(expts, 'params', 'expt_id')


# Merge on drop timepoint 0; no biological data taken
expt_files = pd.merge(ex_times, ex_files, on='expt_id')
expt_files = pd.merge(expt_files, ex_pars, on='expt_id', how='outer')
expt_files = pd.merge(expt_files, expts.drop(['timepts', 'file_types', 'params'], axis = 1), on='expt_id')

expt_files

pts = array2long(patients, var='timepoints', id_var = 'patient_id')
pts['timepts'] = pts.timepoints.apply(lambda x: str(round(x)))
pts.timepts[0]

files = pd.merge(pts, expt_files, left_on='timepts', right_on='timepts', how='left')

files.head()

def create_filename(row):
    return row.patient_id + "_T" + row.timepts + "_" + row.expt_id + row.file_types

files['filename'] = files.apply(create_filename, axis = 1)

files.shape

# [ Create random dummy files ] ---------------------------------------------------------------------------
dummy_content = "This is not a real file."

np.random.seed(20180316)

ids = files.patient_id.unique()
ids



for patient_id in ids:
    print('\n'+ patient_id)
    # Filter by a specific patient_id
    subset = files[files.patient_id == patient_id]

    num_files = len(subset)

    # For each patient, choose a random number of files to create
    num2gen = np.random.randint(1, num_files)

    file_idx = np.random.choice(num_files, size = num2gen, replace=False)
    sel_files = subset.iloc[file_idx]

    # loop over the selected file names and create fake files
    for idx, row in sel_files.iterrows():
        filename = row.dropbox + row.filename
        print(filename)
        dbx.files_upload(dummy_content.encode('utf-8'), filename, mode=dropbox.files.WriteMode.overwrite)

# [ Find if the files have already been uploaded ] --------------------------------------------------------

folders = []
for entry in dbx.files_list_folder(dropbox_folder + "/Data").entries:
    print(entry)
    if(type(entry) == dropbox.files.FolderMetadata):
        folders.append(entry.path_display)

dbx.files_list_folder('').entries

fnames = []
fpaths = []
fdates = []

for folder in folders:
    print(folder)
    for entry in dbx.files_list_folder(folder).entries:
        fnames.append(entry.name)
        fpaths.append(entry.path_display)
        fdates.append(entry.server_modified)

dbx_files = pd.DataFrame({'filename': fnames, 'date_modified': fdates})

files = pd.merge(files, dbx_files, on='filename', how='left')

files['status'] = pd.notnull(files.date_modified)

files.head()
